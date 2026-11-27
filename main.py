import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx
import jwt
import numpy as np
import websockets
from dotenv import load_dotenv

load_dotenv()

# ================= 설정 영역 =================
MARKETS = ["KRW-ETH", "KRW-SOL"]
UNIT = 1                     # 1 = 1분봉, 60 = 1시간봉 (1, 3, 5, 15, 60, 240 가능)
SIGMA_THRESHOLD = 3.0        # 현재 고정 기준 (±3.0σ) - 추후 Agent 판단값으로 대체 예정
ORDER_AMOUNT_KRW = 10000.0   # 매수 기본 단위 (10,000원)
COOLDOWN_SEC = 300           # 동일 방향 재알림/재주문 최소 대기 시간(초)

LOG_FILE = "bb_sigma_tracker.log"
DB_FILE = "bb_tasks.db"
HISTORY_JSON_FILE = "bb_tasks_history.json"

TELEGRAM_TOKEN = os.getenv("TELEBOT_TKN")
CHAT_ID = os.getenv("TELEBOT_CID")
UPBIT_ACCESS_KEY = os.getenv("UAK")
UPBIT_SECRET_KEY = os.getenv("USK")

UPBIT_REST_URL = "https://api.upbit.com"
UPBIT_WS_URL = "wss://api.upbit.com/websocket/v1"
# ============================================

logger = logging.getLogger("SigmaTrader")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


# ---------------- 추후 Agent 판단으로 대체할 훅 ----------------
async def resolve_target_sigma(market: str, side: str, current_sigma: float, context: dict) -> float:
    """
    현재 버전: 3.0 시그마 고정.
    추후 Agent(LLM/정책 엔진)가 시장 상황을 판단해 반환하는 값으로 교체 가능.
    """
    _ = (market, side, current_sigma, context)
    return SIGMA_THRESHOLD


# ---------------- 업비트 호가 단위 및 인증 유틸 ----------------
def round_to_upbit_tick(price: float, side: str) -> float:
    """
    업비트 KRW 마켓 일반 호가 단위 보정.
    매수(bid)는 보수적으로 내림, 매도(ask)는 보수적으로 올림.
    """
    if price >= 2_000_000:
        tick = 1000.0
    elif price >= 1_000_000:
        tick = 500.0
    elif price >= 500_000:
        tick = 100.0
    elif price >= 100_000:
        tick = 50.0
    elif price >= 10_000:
        tick = 10.0
    elif price >= 1_000:
        tick = 5.0
    elif price >= 100:
        tick = 1.0
    elif price >= 10:
        tick = 0.1
    else:
        tick = 0.01

    steps = price / tick
    if side == "bid":
        return round(np.floor(steps) * tick, 4)
    return round(np.ceil(steps) * tick, 4)


def make_upbit_headers(query_params: dict | None = None) -> dict:
    if not UPBIT_ACCESS_KEY or not UPBIT_SECRET_KEY:
        raise RuntimeError(".env에 UPBIT_ACCESS_KEY 또는 UPBIT_SECRET_KEY가 설정되지 않았습니다.")

    payload: dict = {
        "access_key": UPBIT_ACCESS_KEY,
        "nonce": str(uuid.uuid4()),
    }
    if query_params:
        query_string = urlencode(query_params).encode("utf-8")
        m = hashlib.sha512()
        m.update(query_string)
        payload["query_hash"] = m.hexdigest()
        payload["query_hash_alg"] = "SHA512"

    token = jwt.encode(payload, UPBIT_SECRET_KEY, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return {"Authorization": f"Bearer {token}"}


# ---------------- DB & JSON 영속화 ----------------
class TaskStore:
    def __init__(self, db_path: str, json_path: str):
        self.db_path = db_path
        self.json_path = json_path
        self.lock = asyncio.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    market TEXT NOT NULL,
                    status TEXT NOT NULL,            -- ACTIVE, INSUFFICIENT_BALANCE, CLOSED, CANCELLED
                    is_active INTEGER NOT NULL,      -- 1 or 0
                    order_sigma REAL NOT NULL,       -- 주문 생성 시점의 볼린저 시그마
                    buy_sigma REAL,                  -- 매수 주문 시점 시그마
                    sell_sigma REAL,                 -- 매도 주문 시점 시그마
                    buy_order_uuid TEXT,
                    target_buy_price REAL,
                    buy_price REAL,
                    buy_qty REAL,
                    buy_amount REAL,
                    bought_at TEXT,
                    sell_order_uuid TEXT,
                    target_sell_price REAL,
                    sell_price REAL,
                    sold_at TEXT,
                    pnl_krw REAL,
                    pnl_pct REAL,
                    outcome TEXT,                    -- PROFIT, LOSS, INSUFFICIENT_BALANCE
                    note TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    market TEXT NOT NULL,
                    side TEXT NOT NULL,              -- bid / ask
                    order_uuid TEXT,
                    order_sigma REAL NOT NULL,
                    price REAL,
                    volume REAL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _export_json_locked(self, conn: sqlite3.Connection):
        task_rows = [dict(r) for r in conn.execute("SELECT * FROM tasks ORDER BY created_at DESC")]
        order_rows = [dict(r) for r in conn.execute("SELECT * FROM order_events ORDER BY id DESC")]
        payload = {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "tasks": task_rows,
            "order_events": order_rows,
        }
        tmp_path = f"{self.json_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.json_path)

    async def create_task(self, record: dict):
        async with self.lock:
            now_iso = datetime.now(timezone.utc).isoformat()
            record.setdefault("created_at", now_iso)
            record["updated_at"] = now_iso
            cols = ", ".join(record.keys())
            placeholders = ", ".join(["?"] * len(record))
            with self._connect() as conn:
                conn.execute(f"INSERT INTO tasks ({cols}) VALUES ({placeholders})", list(record.values()))
                self._export_json_locked(conn)
                conn.commit()

    async def update_task(self, task_id: str, updates: dict):
        async with self.lock:
            updates["updated_at"] = datetime.now(timezone.utc).isoformat()
            assignments = ", ".join([f"{k} = ?" for k in updates.keys()])
            values = list(updates.values()) + [task_id]
            with self._connect() as conn:
                conn.execute(f"UPDATE tasks SET {assignments} WHERE task_id = ?", values)
                self._export_json_locked(conn)
                conn.commit()

    async def log_order_event(self, task_id: str, market: str, side: str, order_uuid: str | None, order_sigma: float, price: float | None, volume: float | None, state: str):
        async with self.lock:
            now_iso = datetime.now(timezone.utc).isoformat()
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO order_events (task_id, market, side, order_uuid, order_sigma, price, volume, state, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (task_id, market, side, order_uuid, order_sigma, price, volume, state, now_iso),
                )
                self._export_json_locked(conn)
                conn.commit()

    async def get_lowest_active_buy_task(self, market: str) -> dict | None:
        async with self.lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT * FROM tasks
                    WHERE market = ? AND status = 'ACTIVE' AND is_active = 1 AND buy_price IS NOT NULL
                    ORDER BY buy_price ASC, created_at ASC
                    LIMIT 1
                    """,
                    (market,),
                ).fetchone()
                return dict(row) if row else None


# ---------------- 외부 API 호출 ----------------
async def send_telegram(client: httpx.AsyncClient, text: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.warning("[텔레그램 미설정] TELEBOT_TKN 또는 TELEBOT_CID 없음")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = await client.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=5.0)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"[텔레그램 발송 실패] {e}")


async def fetch_past_19_closes(client: httpx.AsyncClient, market: str) -> list[float]:
    url = f"{UPBIT_REST_URL}/v1/candles/minutes/{UNIT}"
    resp = await client.get(url, params={"market": market, "count": 20}, timeout=5.0)
    resp.raise_for_status()
    candles = resp.json()
    return [float(c["trade_price"]) for c in reversed(candles[1:20])]


async def get_krw_balance(client: httpx.AsyncClient) -> float:
    headers = make_upbit_headers()
    resp = await client.get(f"{UPBIT_REST_URL}/v1/accounts", headers=headers, timeout=5.0)
    resp.raise_for_status()
    for acct in resp.json():
        if acct.get("currency") == "KRW":
            return float(acct.get("balance", 0.0))
    return 0.0


async def place_limit_order(client: httpx.AsyncClient, market: str, side: str, price: float, volume: float) -> dict:
    """
    업비트 지정가 주문:
    - side="bid": 매수 (price=주문단가, volume=주문수량)
    - side="ask": 매도 (price=주문단가, volume=매도수량)
    """
    params = {
        "market": market,
        "side": side,
        "ord_type": "limit",
        "price": str(price),
        "volume": f"{volume:.8f}".rstrip("0").rstrip("."),
    }
    headers = make_upbit_headers(params)
    resp = await client.post(f"{UPBIT_REST_URL}/v1/orders", params=params, headers=headers, timeout=5.0)
    resp.raise_for_status()
    return resp.json()


async def get_order_detail(client: httpx.AsyncClient, order_uuid: str) -> dict:
    params = {"uuid": order_uuid}
    headers = make_upbit_headers(params)
    resp = await client.get(f"{UPBIT_REST_URL}/v1/order", params=params, headers=headers, timeout=5.0)
    resp.raise_for_status()
    return resp.json()


def calc_avg_executed_price(order_data: dict, fallback_price: float) -> float:
    trades = order_data.get("trades") or []
    total_funds = 0.0
    total_vol = 0.0
    for tr in trades:
        p = float(tr.get("price", 0.0))
        v = float(tr.get("volume", 0.0))
        total_funds += p * v
        total_vol += v
    if total_vol > 0:
        return total_funds / total_vol
    if order_data.get("price"):
        return float(order_data["price"])
    return fallback_price


# ---------------- 비동기 체결 감시 태스크 ----------------
async def monitor_buy_fill(
    client: httpx.AsyncClient,
    store: TaskStore,
    task_id: str,
    market: str,
    order_uuid: str,
    order_sigma: float,
    target_price: float,
    target_qty: float,
    max_polls: int = 120,
    poll_interval: float = 2.0,
):
    """매수 주문 체결 여부를 비동기로 추적하고 체결 시 ACTIVE Task로 확정"""
    try:
        for _ in range(max_polls):
            await asyncio.sleep(poll_interval)
            info = await get_order_detail(client, order_uuid)
            state = info.get("state")
            executed_vol = float(info.get("executed_volume", 0.0))

            if state == "done" and executed_vol > 0:
                avg_price = calc_avg_executed_price(info, target_price)
                actual_amount = avg_price * executed_vol
                await store.update_task(
                    task_id,
                    {
                        "status": "ACTIVE",
                        "is_active": 1,
                        "buy_price": avg_price,
                        "buy_qty": executed_vol,
                        "buy_amount": actual_amount,
                        "bought_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                await store.log_order_event(task_id, market, "bid", order_uuid, order_sigma, avg_price, executed_vol, "DONE")
                logger.info(
                    f"[매수 체결완료] task_id={task_id} | market={market} | sigma={order_sigma:+.2f}σ | "
                    f"체결가={avg_price:,.0f}원 | 수량={executed_vol:.6f}"
                )
                await send_telegram(
                    client,
                    f"✅ <b>[{market}] 매수 체결 완료</b>\n"
                    f"• Task ID: <code>{task_id}</code>\n"
                    f"• 주문 시그마: <b>{order_sigma:+.2f}σ</b>\n"
                    f"• 체결가: <b>{avg_price:,.0f}원</b>\n"
                    f"• 체결수량: {executed_vol:.6f} (약 {actual_amount:,.0f}원)",
                )
                return

            if state == "cancel":
                await store.update_task(task_id, {"status": "CANCELLED", "is_active": 0, "note": "주문 취소됨"})
                await store.log_order_event(task_id, market, "bid", order_uuid, order_sigma, target_price, executed_vol, "CANCEL")
                return

        logger.warning(f"[매수 체결 대기 타임아웃] task_id={task_id} | uuid={order_uuid}")
    except Exception as e:
        logger.error(f"[매수 체결 감시 오류] task_id={task_id} | {e}")


async def monitor_sell_fill(
    client: httpx.AsyncClient,
    store: TaskStore,
    task: dict,
    order_uuid: str,
    sell_sigma: float,
    target_sell_price: float,
    max_polls: int = 120,
    poll_interval: float = 2.0,
):
    """매도 주문 체결 여부를 비동기로 추적하고 손익 기록 후 비활성(CLOSED) 처리"""
    task_id = task["task_id"]
    market = task["market"]
    buy_price = float(task["buy_price"])
    buy_qty = float(task["buy_qty"])
    buy_amount = float(task.get("buy_amount") or (buy_price * buy_qty))

    try:
        for _ in range(max_polls):
            await asyncio.sleep(poll_interval)
            info = await get_order_detail(client, order_uuid)
            state = info.get("state")
            executed_vol = float(info.get("executed_volume", 0.0))

            if state == "done" and executed_vol > 0:
                sell_price = calc_avg_executed_price(info, target_sell_price)
                sell_proceeds = sell_price * executed_vol
                pnl_krw = sell_proceeds - (buy_price * executed_vol)
                pnl_pct = ((sell_price - buy_price) / buy_price * 100.0) if buy_price > 0 else 0.0
                outcome = "PROFIT" if pnl_krw >= 0 else "LOSS"

                await store.update_task(
                    task_id,
                    {
                        "status": "CLOSED",
                        "is_active": 0,
                        "sell_order_uuid": order_uuid,
                        "sell_sigma": sell_sigma,
                        "target_sell_price": target_sell_price,
                        "sell_price": sell_price,
                        "sold_at": datetime.now(timezone.utc).isoformat(),
                        "pnl_krw": pnl_krw,
                        "pnl_pct": pnl_pct,
                        "outcome": outcome,
                    },
                )
                await store.log_order_event(task_id, market, "ask", order_uuid, sell_sigma, sell_price, executed_vol, "DONE")
                logger.info(
                    f"[매도 체결완료] task_id={task_id} | market={market} | 매도시그마={sell_sigma:+.2f}σ | "
                    f"매수가={buy_price:,.0f} -> 매도가={sell_price:,.0f} | 손익={pnl_krw:+,.0f}원 ({pnl_pct:+.2f}%) | {outcome}"
                )
                await send_telegram(
                    client,
                    f"💰 <b>[{market}] 매도 체결 완료 ({outcome})</b>\n"
                    f"• Task ID: <code>{task_id}</code>\n"
                    f"• 매수/매도 시그마: {float(task.get('buy_sigma') or 0):+.2f}σ → <b>{sell_sigma:+.2f}σ</b>\n"
                    f"• 매수가 / 매도가: {buy_price:,.0f}원 → <b>{sell_price:,.0f}원</b>\n"
                    f"• 실현손익: <b>{pnl_krw:+,.0f}원 ({pnl_pct:+.2f}%)</b>",
                )
                return

            if state == "cancel":
                await store.update_task(task_id, {"status": "ACTIVE", "is_active": 1, "note": "매도 주문 취소 - 재대기"})
                await store.log_order_event(task_id, market, "ask", order_uuid, sell_sigma, target_sell_price, executed_vol, "CANCEL")
                return

        logger.warning(f"[매도 체결 대기 타임아웃] task_id={task_id} | uuid={order_uuid}")
    except Exception as e:
        logger.error(f"[매도 체결 감시 오류] task_id={task_id} | {e}")


# ---------------- 매수/매도 주문 집행 ----------------
async def handle_lower_break(client: httpx.AsyncClient, store: TaskStore, market: str, price: float, mean: float, std: float, sigma: float):
    target_sigma = await resolve_target_sigma(market, "buy", sigma, {"price": price, "mean": mean, "std": std})
    target_price = round_to_upbit_tick(mean - (target_sigma * std), side="bid")
    if target_price <= 0:
        target_price = round_to_upbit_tick(price, side="bid")

    task_id = f"tsk_{market}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    krw_bal = await get_krw_balance(client)

    if krw_bal < ORDER_AMOUNT_KRW:
        await store.create_task(
            {
                "task_id": task_id,
                "market": market,
                "status": "INSUFFICIENT_BALANCE",
                "is_active": 0,
                "order_sigma": sigma,
                "buy_sigma": sigma,
                "target_buy_price": target_price,
                "buy_amount": ORDER_AMOUNT_KRW,
                "outcome": "INSUFFICIENT_BALANCE",
                "note": f"잔액부족 (보유 KRW: {krw_bal:,.0f}원, 필요: {ORDER_AMOUNT_KRW:,.0f}원)",
            }
        )
        logger.warning(f"[잔액부족] task_id={task_id} | market={market} | sigma={sigma:+.2f}σ | 잔액={krw_bal:,.0f}원")
        await send_telegram(
            client,
            f"⚠️ <b>[{market}] 하단 이탈(-{target_sigma:.1f}σ) - 잔액부족</b>\n"
            f"• Task ID: <code>{task_id}</code>\n"
            f"• 주문 시그마: <b>{sigma:+.2f}σ</b>\n"
            f"• 목표 매수가: {target_price:,.0f}원\n"
            f"• 보유 KRW: {krw_bal:,.0f}원 (비활성화 처리)",
        )
        return

    qty = ORDER_AMOUNT_KRW / target_price
    order_res = await place_limit_order(client, market, "bid", target_price, qty)
    order_uuid = order_res.get("uuid")

    await store.create_task(
        {
            "task_id": task_id,
            "market": market,
            "status": "PENDING_BUY",
            "is_active": 0,
            "order_sigma": sigma,
            "buy_sigma": sigma,
            "buy_order_uuid": order_uuid,
            "target_buy_price": target_price,
            "buy_qty": qty,
            "buy_amount": ORDER_AMOUNT_KRW,
            "note": "매수 주문 접수 - 체결 감시 중",
        }
    )
    await store.log_order_event(task_id, market, "bid", order_uuid, sigma, target_price, qty, "PLACED")

    logger.info(
        f"[매수 주문] task_id={task_id} | market={market} | order_sigma={sigma:+.2f}σ | "
        f"호가={target_price:,.0f}원 | 수량={qty:.6f} | uuid={order_uuid}"
    )
    await send_telegram(
        client,
        f"🚨 <b>[{market}] 하단 이탈 매수 주문</b>\n"
        f"• Task ID: <code>{task_id}</code>\n"
        f"• 주문 시그마: <b>{sigma:+.2f}σ</b> (기준 {target_sigma:.1f}σ)\n"
        f"• 현재가 / 매수호가: {price:,.0f}원 / <b>{target_price:,.0f}원</b>\n"
        f"• 주문금액: {ORDER_AMOUNT_KRW:,.0f}원 | UUID: <code>{order_uuid}</code>",
    )

    # 비동기 체결 감시 태스크로 위임하고 시세 트래킹은 즉시 계속
    asyncio.create_task(
        monitor_buy_fill(client, store, task_id, market, order_uuid, sigma, target_price, qty)
    )


async def handle_upper_break(client: httpx.AsyncClient, store: TaskStore, market: str, price: float, mean: float, std: float, sigma: float):
    target_sigma = await resolve_target_sigma(market, "sell", sigma, {"price": price, "mean": mean, "std": std})
    target_sell_price = round_to_upbit_tick(mean + (target_sigma * std), side="ask")

    target_task = await store.get_lowest_active_buy_task(market)
    if not target_task:
        logger.info(f"[상단 돌파] market={market} | sigma={sigma:+.2f}σ | 매도할 ACTIVE Task 없음")
        await send_telegram(
            client,
            f"🔥 <b>[{market}] 상단 돌파(+{target_sigma:.1f}σ)</b>\n"
            f"• 현재가: <b>{price:,.0f}원</b>\n"
            f"• 현재 시그마: <b>{sigma:+.2f}σ</b>\n"
            f"• 활성 매수 Task 없음 (매도 스킵)",
        )
        return

    task_id = target_task["task_id"]
    sell_qty = float(target_task["buy_qty"])
    sell_price = max(target_sell_price, round_to_upbit_tick(price, side="ask"))

    order_res = await place_limit_order(client, market, "ask", sell_price, sell_qty)
    order_uuid = order_res.get("uuid")

    await store.update_task(
        task_id,
        {
            "sell_order_uuid": order_uuid,
            "sell_sigma": sigma,
            "target_sell_price": sell_price,
            "note": f"매도 주문 접수 (order_sigma={sigma:+.2f}σ)",
        },
    )
    await store.log_order_event(task_id, market, "ask", order_uuid, sigma, sell_price, sell_qty, "PLACED")

    logger.info(
        f"[매도 주문] task_id={task_id} | market={market} | order_sigma={sigma:+.2f}σ | "
        f"매수가={float(target_task['buy_price']):,.0f}원 -> 매도호가={sell_price:,.0f}원 | uuid={order_uuid}"
    )
    await send_telegram(
        client,
        f"🔥 <b>[{market}] 상단 돌파 매도 주문 (최저 매수가 Task)</b>\n"
        f"• Task ID: <code>{task_id}</code>\n"
        f"• 주문 시그마: <b>{sigma:+.2f}σ</b> (기준 {target_sigma:.1f}σ)\n"
        f"• 매수가 / 매도호가: {float(target_task['buy_price']):,.0f}원 → <b>{sell_price:,.0f}원</b>\n"
        f"• 수량: {sell_qty:.6f} | UUID: <code>{order_uuid}</code>",
    )

    asyncio.create_task(monitor_sell_fill(client, store, target_task, order_uuid, sigma, sell_price))


# ---------------- 메인 실시간 트래킹 루프 ----------------
async def run_sigma_watcher():
    store = TaskStore(DB_FILE, HISTORY_JSON_FILE)

    state = {
        m: {
            "past_19": [],
            "alert_state": "NORMAL",
            "last_alert_time": {"LOWER_BREAK": 0.0, "UPPER_BREAK": 0.0},
            "current_candle_slot": None,
            "alarm_triggered_this_minute": False,
            "latest_snapshot": None,
        }
        for m in MARKETS
    }

    last_logged_minute = None

    async with httpx.AsyncClient() as http_client:
        initial_candles = await asyncio.gather(*(fetch_past_19_closes(http_client, m) for m in MARKETS))
        for m, closes in zip(MARKETS, initial_candles):
            state[m]["past_19"] = closes

        logger.info(f"[{', '.join(MARKETS)}] {UNIT}분봉 실주문 엔진 시작 (기준: ±{SIGMA_THRESHOLD}σ, 단위: {ORDER_AMOUNT_KRW:,.0f}원)")

        while True:
            try:
                async with websockets.connect(UPBIT_WS_URL, ping_interval=30, ping_timeout=10) as ws:
                    subscribe_msg = [
                        {"ticket": "bb-sigma-trader"},
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

                        # 1. 매분 요약 로그 (알람=True/False)
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

                        # 2. 캔들 롤오버 시 과거 19개 종가 갱신
                        candle_slot = ts_ms // (UNIT * 60 * 1000)
                        if m_state["current_candle_slot"] is not None and candle_slot != m_state["current_candle_slot"]:
                            m_state["past_19"] = await fetch_past_19_closes(http_client, market)
                            m_state["alert_state"] = "NORMAL"
                        m_state["current_candle_slot"] = candle_slot

                        # 3. 20기간 모표준편차(ddof=0) 시그마 계산
                        window = np.array(m_state["past_19"] + [price], dtype=float)
                        mean = window.mean()
                        std = window.std(ddof=0)
                        if std == 0:
                            continue

                        sigma = (price - mean) / std
                        m_state["latest_snapshot"] = {"price": price, "mean": mean, "sigma": sigma}

                        # 4. 하단 이탈 (-3σ) -> 매수
                        if sigma <= -SIGMA_THRESHOLD:
                            if (
                                m_state["alert_state"] != "LOWER_BREAK"
                                and (now_ts - m_state["last_alert_time"]["LOWER_BREAK"] > COOLDOWN_SEC)
                            ):
                                m_state["alert_state"] = "LOWER_BREAK"
                                m_state["last_alert_time"]["LOWER_BREAK"] = now_ts
                                m_state["alarm_triggered_this_minute"] = True
                                asyncio.create_task(
                                    handle_lower_break(http_client, store, market, price, mean, std, sigma)
                                )

                        # 5. 상단 돌파 (+3σ) -> 최저 매수가 ACTIVE Task 매도
                        elif sigma >= SIGMA_THRESHOLD:
                            if (
                                m_state["alert_state"] != "UPPER_BREAK"
                                and (now_ts - m_state["last_alert_time"]["UPPER_BREAK"] > COOLDOWN_SEC)
                            ):
                                m_state["alert_state"] = "UPPER_BREAK"
                                m_state["last_alert_time"]["UPPER_BREAK"] = now_ts
                                m_state["alarm_triggered_this_minute"] = True
                                asyncio.create_task(
                                    handle_upper_break(http_client, store, market, price, mean, std, sigma)
                                )

                        # 6. 밴드 내 복귀 시 히스테리시스 리셋
                        elif -(SIGMA_THRESHOLD - 0.05) < sigma < (SIGMA_THRESHOLD - 0.05):
                            m_state["alert_state"] = "NORMAL"

            except Exception as e:
                logger.warning(f"[연결 끊김 - 3초 후 재접속] {e}")
                await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(run_sigma_watcher())