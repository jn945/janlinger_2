import asyncio
import json
import numpy as np
import requests
import websockets
import time

MARKET = "KRW-ETH"
UNIT = 1  # 1시간봉 (분 단위: 1, 3, 5, 15, 60, 240)

def get_past_19_closes():
    url = f"https://api.upbit.com/v1/candles/minutes/{UNIT}"
    res = requests.get(url, params={"market": MARKET, "count": 20}).json()
    # 인덱스 0이 현재 진행 중인 캔들이므로 1~19번(직전 완료된 19개 캔들)을 과거→최신 순 정렬
    return [c["trade_price"] for c in reversed(res[1:20])]

async def track_realtime_sigma():
    past_19 = get_past_19_closes()
    current_candle_ts = None

    uri = "wss://api.upbit.com/websocket/v1"
    async with websockets.connect(uri) as ws:
        subscribe_fmt = [
            {"ticket": "bb-sigma-tracker"},
            {"type": "ticker", "codes": [MARKET], "isOnlyRealtime": True}
        ]
        await ws.send(json.dumps(subscribe_fmt))

        while True:
            raw = await ws.recv()
            data = json.loads(raw.decode("utf-8"))
            price = data["trade_price"]
            
            # 정각(1시간 단위)이 바뀌어 새 캔들이 시작되면 과거 19개 캔들 갱신
            candle_hour = data["timestamp"] // (UNIT * 60 * 1000)
            if current_candle_ts is not None and candle_hour != current_candle_ts:
                past_19 = get_past_19_closes()
            current_candle_ts = candle_hour

            # 20개 종가(과거 19개 + 실시간 현재가 1개)로 모표준편차(ddof=0) 계산
            window = np.array(past_19 + [price], dtype=float)
            mean = window.mean()
            std = window.std(ddof=0)
            sigma = (price - mean) / std if std > 0 else 0.0

            print(f"{time.time()} \r[{MARKET}] 현재가: {price:,.0f}원 | 20SMA: {mean:,.0f}원 | 현재 위치: {sigma:+.2f}σ", end="")

if __name__ == "__main__":
    asyncio.run(track_realtime_sigma())