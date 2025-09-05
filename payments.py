# payments.py
# Унифицированный слой платежей для Click/Payme (Hosted Checkout + рекуррент)
# Работает как каркас: ссылки/подписи по протоколам провайдеров,
# вебхуки, продление подписки. Все секреты берутся из ENV.

from __future__ import annotations
import os, hmac, hashlib, base64, json, time
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any
from datetime import datetime, timedelta, timezone

from psycopg import connect
from psycopg.rows import dict_row

DB_URL = os.getenv("DATABASE_URL")

# ===== провайдеры =====
CLICK_MERCHANT_ID   = os.getenv("CLICK_MERCHANT_ID", "")
CLICK_SERVICE_ID    = os.getenv("CLICK_SERVICE_ID", "")
CLICK_SECRET_KEY    = os.getenv("CLICK_SECRET_KEY", "")          # для подписи
CLICK_BASE_CHECKOUT = os.getenv("CLICK_BASE_CHECKOUT", "https://my.click.uz/services/pay")  # или prod урл
CLICK_RETURN_URL    = os.getenv("CLICK_RETURN_URL", "")          # https://<ваш-домен>/pay/success
CLICK_CANCEL_URL    = os.getenv("CLICK_CANCEL_URL", "")          # https://<ваш-домен>/pay/cancel

PAYME_MERCHANT_ID   = os.getenv("PAYME_MERCHANT_ID", "")         # paycom merchant id (GUID)
PAYME_SECRET_KEY    = os.getenv("PAYME_SECRET_KEY", "")          # для HMAC подписи методов (если нужно)
PAYME_BASE_CHECKOUT = os.getenv("PAYME_BASE_CHECKOUT", "https://checkout.paycom.uz")  # hosted checkout
PAYME_RETURN_URL    = os.getenv("PAYME_RETURN_URL", "")          # https://<ваш-домен>/pay/success
PAYME_CANCEL_URL    = os.getenv("PAYME_CANCEL_URL", "")          # https://<ваш-домен>/pay/cancel

# ===== вспомогательные =====
def db():
    return connect(DB_URL, autocommit=True, row_factory=dict_row)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

@dataclass
class Plan:
    code: str
    amount: int     # в суммах (UZS)
    currency: str   # 'UZS'
    period_days: int

# Базовые тарифы (можно расширить через БД)
PLANS: Dict[str, Plan] = {
    "basic_m": Plan("basic_m", amount=49000, currency="UZS", period_days=30),
    "pro_m":   Plan("pro_m",   amount=99000, currency="UZS", period_days=30),
}

# ===== БД-helpers =====
def create_order(user_id: int, provider: str, plan_code: str, kind: str) -> int:
    """Создаёт заказ (pending) и возвращает order_id."""
    plan = PLANS[plan_code]
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO orders(user_id, provider, plan_code, amount, currency, kind, status)
            VALUES (%s,%s,%s,%s,%s,%s,'pending') RETURNING id;
        """, (user_id, provider, plan.code, plan.amount, plan.currency, kind))
        return cur.fetchone()["id"]

def get_order(order_id: int) -> dict | None:
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM orders WHERE id=%s;", (order_id,))
        return cur.fetchone()

def mark_order_paid(order_id: int, provider_trx_id: str, raw: dict):
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE orders SET status='paid', paid_at=NOW(), provider_trx_id=%s, raw=COALESCE(raw,'{}'::jsonb)||%s::jsonb
            WHERE id=%s;
        """, (provider_trx_id, json.dumps(raw), order_id))

def upsert_subscription(user_id: int, plan_code: str, provider: str, payment_token: Optional[str], days: int):
    """Создаёт или продлевает подписку."""
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO subscriptions(user_id, plan_code, provider, status, current_period_end, payment_token)
            VALUES (%s,%s,%s,'active', NOW() + (%s || ' days')::interval, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET plan_code=EXCLUDED.plan_code,
                provider=EXCLUDED.provider,
                status='active',
                payment_token = COALESCE(EXCLUDED.payment_token, subscriptions.payment_token),
                current_period_end = GREATEST(subscriptions.current_period_end, NOW()) + (%s || ' days')::interval,
                updated_at=NOW();
        """, (user_id, plan_code, provider, days, payment_token, days))

def log_tx(order_id: int, provider: str, data: dict, status: str):
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO payment_transactions(order_id, provider, status, payload)
            VALUES (%s,%s,%s,%s);
        """, (order_id, provider, status, json.dumps(data)))

# ====== CLICK ======
def _click_sign(merchant_trans_id: str, amount: int, action: str, sign_time: str) -> str:
    raw = f"{CLICK_MERCHANT_ID}{merchant_trans_id}{amount}{CLICK_SECRET_KEY}{action}{sign_time}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def click_checkout_link(order_id: int, plan: Plan, description: str = "Подписка TripleA") -> str:
    # merchant_trans_id — наш ID заказа
    merchant_trans_id = str(order_id)
    amount = plan.amount
    action = "0"                                 # 0 — оплата
    sign_time = str(int(time.time()))
    sign = _click_sign(merchant_trans_id, amount, action, sign_time)
    params = {
        "merchant_id": CLICK_MERCHANT_ID,
        "service_id": CLICK_SERVICE_ID,
        "merchant_trans_id": merchant_trans_id,
        "amount": amount,
        "action": action,
        "sign_time": sign_time,
        "sign_string": sign,
        "return_url": CLICK_RETURN_URL,
        "cancel_url": CLICK_CANCEL_URL,
        "description": description,
    }
    q = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{CLICK_BASE_CHECKOUT}?{q}"

def click_handle_callback(payload: Dict[str, Any]) -> Tuple[bool, str, Optional[int], Optional[str]]:
    """
    Обработка серверного callback'а Click. Возвращает (ok, message, order_id, provider_tx_id)
    Документация Click: параметры могут приходить как form-data.
    """
    try:
        merchant_trans_id = payload.get("merchant_trans_id") or payload.get("merchant_trans_id")
        amount = int(float(payload.get("amount", 0)))
        action = str(payload.get("action", "0"))
        sign_time = payload.get("sign_time", "")
        sign = payload.get("sign_string", "")

        if _click_sign(str(merchant_trans_id), amount, action, sign_time) != str(sign):
            return False, "invalid signature", None, None

        order_id = int(merchant_trans_id)
        provider_tx = str(payload.get("click_trans_id", ""))

        # логируем и помечаем заказ
        log_tx(order_id, "click", payload, "paid")
        mark_order_paid(order_id, provider_tx, payload)
        return True, "OK", order_id, provider_tx
    except Exception as e:
        return False, f"error: {e}", None, None

# ====== PAYME (Paycom Hosted Checkout) ======
def payme_checkout_link(order_id: int, plan: Plan, description: str = "Подписка TripleA") -> str:
    """
    Hosted Checkout: редирект с merchant, amount и merchant_trans_id в base64.
    В проде используйте созданную в кабинете ссылку с расширенными параметрами/brand.
    """
    # Payme принимает "amount" в тийинах (копейках) → UZS * 100
    amount_tiyin = plan.amount * 100
    state = {
        "m": PAYME_MERCHANT_ID,
        "ac.order_id": str(order_id),
        "a": amount_tiyin,
        "c": plan.currency,
        "l": description,
        "cr": "uzs",
        "callback": PAYME_RETURN_URL,
    }
    b64 = base64.urlsafe_b64encode(json.dumps(state).encode()).decode()
    return f"{PAYME_BASE_CHECKOUT}/{b64}"

def payme_handle_callback(payload: Dict[str, Any], headers: Dict[str, str]) -> Tuple[bool, str, Optional[int], Optional[str]]:
    """
    Простейшая обработка — многие интеграции используют JSON-RPC. Здесь принимаем hosted-callback.
    Сигнатуру можно проверять по заголовку Authorization: Basic base64(merchant:password).
    """
    try:
        # В hosted варианте обычно приходит state с нашим order_id
        raw_state = payload.get("state") or payload.get("merchant_trans_id") or ""
        order_id = None
        try:
            j = json.loads(base64.b64decode(raw_state + "==").decode())
            order_id = int(j.get("ac.order_id"))
        except Exception:
            if raw_state:
                order_id = int(raw_state)
        if not order_id:
            return False, "order not found in state", None, None

        provider_tx = str(payload.get("transaction", "") or payload.get("paycom_transaction_id", ""))

        log_tx(order_id, "payme", payload, "paid")
        mark_order_paid(order_id, provider_tx, payload)
        return True, "OK", order_id, provider_tx
    except Exception as e:
        return False, f"error: {e}", None, None

# ===== публичное API слоя =====
def build_checkout_link(provider: str, order_id: int, plan_code: str) -> str:
    plan = PLANS[plan_code]
    if provider == "click":
        return click_checkout_link(order_id, plan)
    elif provider == "payme":
        return payme_checkout_link(order_id, plan)
    else:
        raise ValueError("unknown provider")

def activate_after_payment(order_id: int):
    """Активирует/продлевает подписку по заказу."""
    o = get_order(order_id)
    if not o or o["status"] != "paid":
        return
    plan = PLANS[o["plan_code"]]
    # payment_token тут будет None — для реального рекуррента получите токен в callback'е
    upsert_subscription(user_id=o["user_id"], plan_code=plan.code,
                        provider=o["provider"], payment_token=None,
                        days=plan.period_days)
