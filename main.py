import asyncio
import json
import logging
import os
import time
from datetime import datetime
import httpx
import numpy as np
import websockets
from dotenv import load_dotenv

load_dotenv()

# ================= 설정 영역 =================
MARKETS = ["KRW-ETH", "KRW-SOL"]
UNIT = 1                   # 1 = 1분봉, 60 = 1시간봉 (1, 3, 5, 15, 60, 240 가능)
SIGMA_THRESHOLD = 2.0      # 이탈 기준 시그마 (±2.0σ)
COOLDOWN_SEC = 300         # 동일 방향 재알림 최소 대기 시간(초)
LOG_FILE = "bb_sigma_tracker.log"

TELEGRAM_TOKEN = os.getenv("TELEBOT_TKN")
CHAT_ID = os.getenv("TELEBOT_CID")
# ============================================

# 로거 설정 (콘솔 + 파일 동시 기록)
logger = logging.getLogger("SigmaTracker")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


async def send_telegram(client: httpx.AsyncClient, text: str):
    """비동기 텔레그램 메시지 발송 (웹소켓 루프 블로킹 방지)"""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.error("[설정 오류] .env에서 TELEBOT_TKN 또는 TELEBOT_CID를 불러오지 못했습니다.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }
    try:
        resp = await client.post(url, json=payload, timeout=5.0)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"[텔레그램 발송 실패] {e}")


async def fetch_past_19_closes(client: httpx.AsyncClient, market: str) -> list[float]:
    """특정 마켓의 직전 완료된 19개 캔들 종가 조회 (과거 -> 최신 순)"""
    url = f"https://api.upbit.com/v1/candles/minutes/{UNIT}"
    resp = await client.get(url, params={"market": market, "count": 20}, timeout=5.0)
    resp.raise_for_status()
    candles = resp.json()
    return [float(c["trade_price"]) for c in reversed(candles[1:20])]


async def run_sigma_watcher():
    # 종목별 독립 상태 관리 딕셔너리 초기화
    state = {
        m: {
            "past_19": [],
            "alert_state": "NORMAL",  # "NORMAL", "LOWER_BREAK", "UPPER_BREAK"
            "last_alert_time": {"LOWER_BREAK": 0.0, "UPPER_BREAK": 0.0},
            "current_candle_slot": None,
            "alarm_triggered_this_minute": False,
            "latest_snapshot": None,
        }
        for m in MARKETS
    }

    last_logged_minute = None

    async with httpx.AsyncClient() as http_client:
        # 초기 과거 19개 캔들 데이터 병렬 조회
        initial_candles = await asyncio.gather(
            *(fetch_past_19_closes(http_client, m) for m in MARKETS)
        )
        for m, closes in zip(MARKETS, initial_candles):
            state[m]["past_19"] = closes

        logger.info(f"[{', '.join(MARKETS)}] {UNIT}분봉 트래킹 시작 (기준: ±{SIGMA_THRESHOLD}σ)")

        uri = "wss://api.upbit.com/websocket/v1"
        while True:
            try:
                async with websockets.connect(uri, ping_interval=30, ping_timeout=10) as ws:
                    subscribe_msg = [
                        {"ticket": "bb-sigma-alert-bot"},
                        {"type": "ticker", "codes": MARKETS, "isOnlyRealtime": True},
                    ]
                    await ws.send(json.dumps(subscribe_msg))

                    while True:
                        raw = await ws.recv()
                        data = json.loads(raw.decode("utf-8"))
                        market = data.get("code")
                        if market not in state:
                            continue

                        price = float(data["trade_price"])
                        ts_ms = int(data["timestamp"])
                        now_ts = time.time()
                        now_dt = datetime.fromtimestamp(now_ts)

                        # 1. 분이 바뀌었는지 체크 -> 직전 분 전체 종목 요약 로그 기록
                        if last_logged_minute is None:
                            last_logged_minute = now_dt.minute
                        elif now_dt.minute != last_logged_minute:
                            for m in MARKETS:
                                snap = state[m]["latest_snapshot"]
                                if snap is not None:
                                    logger.info(
                                        f"[{m}] 현재가={snap['price']:,.0f} | "
                                        f"20SMA={snap['mean']:,.0f} | "
                                        f"시그마={snap['sigma']:+.2f}σ | "
                                        f"알람={state[m]['alarm_triggered_this_minute']}"
                                    )
                                state[m]["alarm_triggered_this_minute"] = False
                            last_logged_minute = now_dt.minute

                        m_state = state[market]

                        # 2. 해당 종목의 캔들 슬롯 변경 감지 시 과거 19개 종가 갱신
                        candle_slot = ts_ms // (UNIT * 60 * 1000)
                        if (
                            m_state["current_candle_slot"] is not None
                            and candle_slot != m_state["current_candle_slot"]
                        ):
                            m_state["past_19"] = await fetch_past_19_closes(http_client, market)
                            m_state["alert_state"] = "NORMAL"
                        m_state["current_candle_slot"] = candle_slot

                        # 3. 실시간 20개 윈도우로 모표준편차(ddof=0) 계산
                        window = np.array(m_state["past_19"] + [price], dtype=float)
                        mean = window.mean()
                        std = window.std(ddof=0)
                        if std == 0:
                            continue

                        sigma = (price - mean) / std
                        upper_2s = mean + (SIGMA_THRESHOLD * std)
                        lower_2s = mean - (SIGMA_THRESHOLD * std)

                        # 최신 상태 스냅샷 업데이트
                        m_state["latest_snapshot"] = {
                            "price": price,
                            "mean": mean,
                            "sigma": sigma,
                        }

                        # 4. 하단 밴드(-2σ) 이탈 감지
                        if sigma <= -SIGMA_THRESHOLD:
                            if (
                                m_state["alert_state"] != "LOWER_BREAK"
                                and (now_ts - m_state["last_alert_time"]["LOWER_BREAK"] > COOLDOWN_SEC)
                            ):
                                m_state["alert_state"] = "LOWER_BREAK"
                                m_state["last_alert_time"]["LOWER_BREAK"] = now_ts
                                m_state["alarm_triggered_this_minute"] = True

                                msg = (
                                    f"🚨 <b>[{market}] -{SIGMA_THRESHOLD}σ 하단 이탈!</b>\n"
                                    f"• 기준봉: {UNIT}분봉\n"
                                    f"• 현재가: <b>{price:,.0f}원</b>\n"
                                    f"• 현재 시그마: <b>{sigma:+.2f}σ</b>\n"
                                    f"• 하단밴드(-2σ): {lower_2s:,.0f}원\n"
                                    f"• 중심선(20SMA): {mean:,.0f}원"
                                )
                                asyncio.create_task(send_telegram(http_client, msg))

                        # 5. 상단 밴드(+2σ) 돌파 감지
                        elif sigma >= SIGMA_THRESHOLD:
                            if (
                                m_state["alert_state"] != "UPPER_BREAK"
                                and (now_ts - m_state["last_alert_time"]["UPPER_BREAK"] > COOLDOWN_SEC)
                            ):
                                m_state["alert_state"] = "UPPER_BREAK"
                                m_state["last_alert_time"]["UPPER_BREAK"] = now_ts
                                m_state["alarm_triggered_this_minute"] = True

                                msg = (
                                    f"🔥 <b>[{market}] +{SIGMA_THRESHOLD}σ 상단 돌파!</b>\n"
                                    f"• 기준봉: {UNIT}분봉\n"
                                    f"• 현재가: <b>{price:,.0f}원</b>\n"
                                    f"• 현재 시그마: <b>{sigma:+.2f}σ</b>\n"
                                    f"• 상단밴드(+2σ): {upper_2s:,.0f}원\n"
                                    f"• 중심선(20SMA): {mean:,.0f}원"
                                )
                                asyncio.create_task(send_telegram(http_client, msg))

                        # 6. 밴드 안쪽(-1.95σ ~ +1.95σ) 복귀 시 히스테리시스 리셋
                        elif -(SIGMA_THRESHOLD - 0.05) < sigma < (SIGMA_THRESHOLD - 0.05):
                            m_state["alert_state"] = "NORMAL"

            except Exception as e:
                logger.warning(f"[연결 끊김 - 3초 후 재접속] {e}")
                await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(run_sigma_watcher())