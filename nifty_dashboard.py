from __future__ import annotations

import html
import time
from collections import deque
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import requests
import streamlit as st

from login import get_access_token
import upstox_ws


# =========================================================
# CONFIG
# =========================================================
APP_TITLE = "NIFTY Live Option Chain"
EXPIRY = "2026-04-21"
INSTRUMENT = "NSE_INDEX|Nifty 50"

REFRESH_SECONDS = 1
REQUEST_TIMEOUT = 12
TABLE_HEIGHT_PX = 560

# periodic full REST refresh for correction / greeks / bid ask
REST_REFRESH_SECONDS = 20

# keep only last 300 snapshots in memory
SNAPSHOT_BUFFER_SIZE = 300


# =========================================================
# PAGE SETUP
# =========================================================
st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 0.12rem;
        padding-bottom: 0.12rem;
        padding-left: 0.35rem;
        padding-right: 0.35rem;
        max-width: 100% !important;
    }

    div[data-testid="stMetric"] {
        border: 1px solid rgba(128,128,128,0.10);
        border-radius: 8px;
        padding: 2px 6px;
        background: transparent;
        box-shadow: none;
    }

    div[data-testid="stMetricLabel"] p {
        font-size: 11px !important;
        margin-bottom: 0px !important;
    }

    div[data-testid="stMetricValue"] {
        font-size: 14px !important;
        line-height: 1.0 !important;
    }

    .main-title {
        font-size: 14px;
        font-weight: 700;
        margin-bottom: 0px;
    }

    .sub-title {
        color: #6b7280;
        font-size: 12px;
        margin-bottom: 4px;
    }

    hr {
        margin-top: 0.18rem !important;
        margin-bottom: 0.18rem !important;
    }

    .element-container {
        margin-bottom: 0.15rem !important;
    }

    .oc-shell {
        width: 100%;
        overflow: hidden;
        background: transparent;
    }

    .oc-wrap {
        width: 100%;
        height: 82vh;
        max-height: 82vh;
        overflow: auto;
        background: transparent;
        border: 1px solid #e5e7eb;
        border-radius: 8px;
    }

    .oc-table {
        border-collapse: separate;
        border-spacing: 0;
        min-width: 100%;
        width: max-content;
        font-size: 12px;
        background: transparent;
    }

    .oc-table th,
    .oc-table td {
        border-right: 1px solid #e5e7eb;
        border-bottom: 1px solid #e5e7eb;
        padding: 6px 10px;
        text-align: center;
        white-space: nowrap;
        background: transparent;
        min-width: 105px;
    }

    .oc-table th {
        position: sticky;
        top: 0;
        z-index: 10;
        background: #ffffff;
        color: #5a5a5a;
        font-weight: 700;
        font-size: 12px;
    }

    .oc-table .strike-head {
        min-width: 120px;
    }

    .oc-main {
        font-size: 12px;
        line-height: 1.1;
        color: #2a2a2a;
    }

    .oc-sub {
        font-size: 12px;
        line-height: 1.1;
        margin-top: 4px;
        color: #666;
    }

    .oc-green {
        color: #0f7b5f;
    }

    .oc-orange {
        color: #f05a28;
    }

    .oc-strike-col {
        min-width: 120px;
        max-width: 120px;
        background: transparent;
    }

    .oc-strike-main {
        font-size: 12px;
        font-weight: 500;
        color: #111827;
        line-height: 1.1;
    }

    .oc-strike-sub {
        font-size: 12px;
        color: #4b5563;
        margin-top: 4px;
        line-height: 1.1;
    }

    .oc-nearest td {
        font-weight: 700;
    }

    .oc-wrap::-webkit-scrollbar {
        height: 10px;
        width: 10px;
    }

    .oc-wrap::-webkit-scrollbar-thumb {
        background: #c9c0ae;
        border-radius: 10px;
    }

    .oc-wrap::-webkit-scrollbar-track {
        background: transparent;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# SESSION STATE
# =========================================================
DEFAULT_STATE = {
    "nifty_ws_started": False,
    "nifty_last_subscribed_keys": [],
    "nifty_fetch_error": None,
}

for key, value in DEFAULT_STATE.items():
    if key not in st.session_state:
        st.session_state[key] = value

if "nifty_np_store" not in st.session_state:
    st.session_state["nifty_np_store"] = {
        "ready": False,
        "n": 0,

        # mapping
        "ce_key_to_idx": {},
        "pe_key_to_idx": {},

        # static / slow-changing arrays
        "strike": np.array([], dtype=np.float64),
        "ce_key": [],
        "pe_key": [],
        "ce_prev_oi": np.array([], dtype=np.float64),
        "pe_prev_oi": np.array([], dtype=np.float64),
        "ce_iv": np.array([], dtype=np.float64),
        "ce_gamma": np.array([], dtype=np.float64),
        "ce_theta": np.array([], dtype=np.float64),
        "ce_delta": np.array([], dtype=np.float64),
        "ce_bid": np.array([], dtype=np.float64),
        "ce_ask": np.array([], dtype=np.float64),
        "ce_bid_qty": np.array([], dtype=np.float64),
        "ce_ask_qty": np.array([], dtype=np.float64),
        "pe_bid_qty": np.array([], dtype=np.float64),
        "pe_ask_qty": np.array([], dtype=np.float64),
        "pe_bid": np.array([], dtype=np.float64),
        "pe_ask": np.array([], dtype=np.float64),
        "pe_iv": np.array([], dtype=np.float64),
        "pe_gamma": np.array([], dtype=np.float64),
        "pe_theta": np.array([], dtype=np.float64),
        "pe_delta": np.array([], dtype=np.float64),

        # live arrays
        "ce_oi": np.array([], dtype=np.float64),
        "pe_oi": np.array([], dtype=np.float64),
        "ce_chg_oi": np.array([], dtype=np.float64),
        "pe_chg_oi": np.array([], dtype=np.float64),
        "ce_volume": np.array([], dtype=np.float64),
        "pe_volume": np.array([], dtype=np.float64),
        "ce_ltp": np.array([], dtype=np.float64),
        "pe_ltp": np.array([], dtype=np.float64),
        "pcr_row": np.array([], dtype=np.float64),

        "meta": {
            "spot": None,
            "pcr": None,
            "open": None,
            "high": None,
            "low": None,
            "prev_close": None,
            "last_rest_refresh_ts": 0.0,
            "last_ws_apply_ts": None,
            "last_ui_update_ts": None,
            "last_snapshot_ts": None,
        },

        # compact rolling analysis buffer
        "snapshots": deque(maxlen=SNAPSHOT_BUFFER_SIZE),
    }


# =========================================================
# CACHED RESOURCES
# =========================================================
@st.cache_resource
def get_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    return session


# =========================================================
# HELPERS
# =========================================================
def get_store() -> dict[str, Any]:
    return st.session_state["nifty_np_store"]


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def nan_float(value: Any) -> float:
    x = safe_float(value)
    return np.nan if x is None else float(x)


def fmt_num(value: Any, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


def fmt_int(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return f"{int(float(value)):,}"
    except (TypeError, ValueError):
        return str(value)


def fmt_volume(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        v = float(value)
        if v >= 100000:
            return f"{v / 100000:.2f}L"
        if v >= 1000:
            return f"{v / 1000:.2f}K"
        return f"{int(v)}"
    except (TypeError, ValueError):
        return str(value)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def esc(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    return html.escape(str(value))


# =========================================================
# API
# =========================================================
def get_response_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    session = get_http_session()
    response = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def extract_ohlc(data: dict[str, Any]) -> dict[str, Any] | None:
    payload = data.get("data", {})
    if not isinstance(payload, dict) or not payload:
        return None

    for key in ("NSE_INDEX:Nifty 50", "NSE_INDEX|Nifty 50", INSTRUMENT):
        candidate = payload.get(key)
        if isinstance(candidate, dict) and "ohlc" in candidate:
            return candidate["ohlc"]

    first_value = next(iter(payload.values()), None)
    if isinstance(first_value, dict) and "ohlc" in first_value:
        return first_value["ohlc"]

    return None


def get_nifty_ohlc(token: str) -> tuple[float | None, float | None, float | None, float | None]:
    try:
        data = get_response_json(
            "https://api.upstox.com/v2/market-quote/ohlc",
            headers={"Authorization": f"Bearer {token}"},
            params={"instrument_key": INSTRUMENT, "interval": "1d"},
        )
        ohlc = extract_ohlc(data)
        if not ohlc:
            return None, None, None, None

        return (
            safe_float(ohlc.get("open")),
            safe_float(ohlc.get("high")),
            safe_float(ohlc.get("low")),
            safe_float(ohlc.get("close")),
        )
    except Exception:
        return None, None, None, None


def fetch_chain(token: str) -> tuple[pd.DataFrame, list[str], float | None]:
    data = get_response_json(
        "https://api.upstox.com/v2/option/chain",
        headers={"Authorization": f"Bearer {token}"},
        params={"instrument_key": INSTRUMENT, "expiry_date": EXPIRY},
    )

    raw_rows = data.get("data", [])
    rows: list[dict[str, Any]] = []
    keys: list[str] = []
    spot: float | None = None

    for item in raw_rows:
        strike = safe_float(item.get("strike_price"))
        row_spot = safe_float(item.get("underlying_spot_price"))
        if row_spot is not None:
            spot = row_spot

        pcr = safe_float(item.get("pcr"))

        call = item.get("call_options") or {}
        put = item.get("put_options") or {}

        ce_md = call.get("market_data") or {}
        pe_md = put.get("market_data") or {}

        ce_gk = call.get("option_greeks") or {}
        pe_gk = put.get("option_greeks") or {}

        ce_key = call.get("instrument_key")
        pe_key = put.get("instrument_key")

        if ce_key:
            keys.append(ce_key)
        if pe_key:
            keys.append(pe_key)

        ce_oi = safe_float(ce_md.get("oi"))
        pe_oi = safe_float(pe_md.get("oi"))
        ce_prev_oi = safe_float(ce_md.get("prev_oi"))
        pe_prev_oi = safe_float(pe_md.get("prev_oi"))

        rows.append(
            {
                "STRIKE": strike,
                "SPOT": row_spot,
                "PCR": pcr,
                "CE_KEY": ce_key,
                "CE_OI": ce_oi,
                "CE_PREV_OI": ce_prev_oi,
                "CE_CHG_OI": None if ce_oi is None or ce_prev_oi is None else ce_oi - ce_prev_oi,
                "CE_VOLUME": safe_float(ce_md.get("volume")),
                "CE_IV": safe_float(ce_gk.get("iv")),
                "CE_DELTA": safe_float(ce_gk.get("delta")),
                "CE_GAMMA": safe_float(ce_gk.get("gamma")),
                "CE_THETA": safe_float(ce_gk.get("theta")),
                "CE_VEGA": safe_float(ce_gk.get("vega")),
                "CE_LTP": safe_float(ce_md.get("ltp")),
                "CE_BID": safe_float(ce_md.get("bid_price")),
                "CE_ASK": safe_float(ce_md.get("ask_price")),
                "CE_BID_QTY": safe_float(ce_md.get("bid_qty")),
                "CE_ASK_QTY": safe_float(ce_md.get("ask_qty")),

                "PE_KEY": pe_key,
                "PE_OI": pe_oi,
                "PE_PREV_OI": pe_prev_oi,
                "PE_CHG_OI": None if pe_oi is None or pe_prev_oi is None else pe_oi - pe_prev_oi,
                "PE_VOLUME": safe_float(pe_md.get("volume")),
                "PE_IV": safe_float(pe_gk.get("iv")),
                "PE_DELTA": safe_float(pe_gk.get("delta")),
                "PE_GAMMA": safe_float(pe_gk.get("gamma")),
                "PE_THETA": safe_float(pe_gk.get("theta")),
                "PE_VEGA": safe_float(pe_gk.get("vega")),
                "PE_LTP": safe_float(pe_md.get("ltp")),
                "PE_BID": safe_float(pe_md.get("bid_price")),
                "PE_ASK": safe_float(pe_md.get("ask_price")),
                "PE_BID_QTY": safe_float(pe_md.get("bid_qty")),
                "PE_ASK_QTY": safe_float(pe_md.get("ask_qty")),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("STRIKE").reset_index(drop=True)

    return df, sorted(set(keys)), spot


# =========================================================
# WEBSOCKET
# =========================================================
def init_ws(token: str, keys: list[str]) -> None:
    if not keys:
        return

    try:
        if not st.session_state.nifty_ws_started:
            upstox_ws.start_ws(token)
            time.sleep(1.0)
            upstox_ws.subscribe(keys)
            st.session_state.nifty_ws_started = True
            st.session_state.nifty_last_subscribed_keys = keys
            return

        prev = set(st.session_state.nifty_last_subscribed_keys)
        curr = set(keys)
        new_keys = sorted(curr - prev)
        if new_keys:
            upstox_ws.subscribe(new_keys)
            st.session_state.nifty_last_subscribed_keys = sorted(curr)
    except Exception as exc:
        st.session_state.nifty_fetch_error = f"WS init error: {exc}"


# =========================================================
# NUMPY MEMORY ENGINE
# =========================================================
def bootstrap_store_from_rest(token: str, force: bool = False) -> None:
    store = get_store()

    if not force and store["ready"]:
        elapsed = time.time() - float(store["meta"]["last_rest_refresh_ts"] or 0)
        if elapsed < REST_REFRESH_SECONDS:
            return

    df, keys, spot = fetch_chain(token)

    if df.empty:
        st.session_state.nifty_fetch_error = "No option chain data received."
        return

    open_, high, low, prev_close = get_nifty_ohlc(token)

    n = len(df)

    ce_key_to_idx: dict[str, int] = {}
    pe_key_to_idx: dict[str, int] = {}

    ce_keys = df["CE_KEY"].fillna("").tolist()
    pe_keys = df["PE_KEY"].fillna("").tolist()

    for idx, key in enumerate(ce_keys):
        if key:
            ce_key_to_idx[key] = idx

    for idx, key in enumerate(pe_keys):
        if key:
            pe_key_to_idx[key] = idx

    store["ready"] = True
    store["n"] = n
    store["ce_key_to_idx"] = ce_key_to_idx
    store["pe_key_to_idx"] = pe_key_to_idx

    # static / slower arrays
    store["strike"] = df["STRIKE"].astype(float).to_numpy(dtype=np.float64)
    store["ce_key"] = ce_keys
    store["pe_key"] = pe_keys
    store["ce_prev_oi"] = df["CE_PREV_OI"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_prev_oi"] = df["PE_PREV_OI"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_iv"] = df["CE_IV"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_gamma"] = df["CE_GAMMA"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_theta"] = df["CE_THETA"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_delta"] = df["CE_DELTA"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_bid"] = df["CE_BID"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_ask"] = df["CE_ASK"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_bid_qty"] = df["CE_BID_QTY"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_ask_qty"] = df["CE_ASK_QTY"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_bid_qty"] = df["PE_BID_QTY"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_ask_qty"] = df["PE_ASK_QTY"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_bid"] = df["PE_BID"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_ask"] = df["PE_ASK"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_iv"] = df["PE_IV"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_gamma"] = df["PE_GAMMA"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_theta"] = df["PE_THETA"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_delta"] = df["PE_DELTA"].apply(nan_float).to_numpy(dtype=np.float64)

    # live arrays
    store["ce_oi"] = df["CE_OI"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_oi"] = df["PE_OI"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_chg_oi"] = df["CE_CHG_OI"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_chg_oi"] = df["PE_CHG_OI"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_volume"] = df["CE_VOLUME"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_volume"] = df["PE_VOLUME"].apply(nan_float).to_numpy(dtype=np.float64)
    store["ce_ltp"] = df["CE_LTP"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pe_ltp"] = df["PE_LTP"].apply(nan_float).to_numpy(dtype=np.float64)
    store["pcr_row"] = df["PCR"].apply(nan_float).to_numpy(dtype=np.float64)

    total_call = np.nansum(store["ce_oi"])
    total_put = np.nansum(store["pe_oi"])
    pcr = (total_put / total_call) if total_call else None

    store["meta"]["spot"] = spot
    store["meta"]["pcr"] = pcr
    store["meta"]["open"] = open_
    store["meta"]["high"] = high
    store["meta"]["low"] = low
    store["meta"]["prev_close"] = prev_close
    store["meta"]["last_rest_refresh_ts"] = time.time()

    init_ws(token, keys)
    st.session_state.nifty_fetch_error = None


def apply_ws_ticks_to_store() -> None:
    store = get_store()
    if not store["ready"]:
        return

    ticks = getattr(upstox_ws, "ticks", {})
    if not isinstance(ticks, dict) or not ticks:
        return

    dirty = False

    for instrument_key, tick in ticks.items():
        if not isinstance(tick, dict):
            continue

        tick_ltp = safe_float(tick.get("last_price"))
        tick_volume = safe_float(tick.get("volume"))
        tick_oi = safe_float(tick.get("oi"))

        if instrument_key in store["ce_key_to_idx"]:
            idx = store["ce_key_to_idx"][instrument_key]

            if tick_ltp is not None:
                store["ce_ltp"][idx] = tick_ltp
                dirty = True

            if tick_volume is not None:
                store["ce_volume"][idx] = tick_volume
                dirty = True

            if tick_oi is not None:
                store["ce_oi"][idx] = tick_oi
                prev_oi = store["ce_prev_oi"][idx]
                store["ce_chg_oi"][idx] = np.nan if np.isnan(prev_oi) else tick_oi - prev_oi
                dirty = True

        elif instrument_key in store["pe_key_to_idx"]:
            idx = store["pe_key_to_idx"][instrument_key]

            if tick_ltp is not None:
                store["pe_ltp"][idx] = tick_ltp
                dirty = True

            if tick_volume is not None:
                store["pe_volume"][idx] = tick_volume
                dirty = True

            if tick_oi is not None:
                store["pe_oi"][idx] = tick_oi
                prev_oi = store["pe_prev_oi"][idx]
                store["pe_chg_oi"][idx] = np.nan if np.isnan(prev_oi) else tick_oi - prev_oi
                dirty = True

    if dirty:
        total_call = np.nansum(store["ce_oi"])
        total_put = np.nansum(store["pe_oi"])
        store["meta"]["pcr"] = (total_put / total_call) if total_call else None
        store["meta"]["last_ws_apply_ts"] = now_str()


def append_compact_snapshot() -> None:
    store = get_store()
    if not store["ready"]:
        return

    ts = now_str()
    spot = store["meta"]["spot"]
    pcr = store["meta"]["pcr"]

    ce_oi = store["ce_oi"]
    pe_oi = store["pe_oi"]
    strikes = store["strike"]
    ce_chg = store["ce_chg_oi"]
    pe_chg = store["pe_chg_oi"]

    total_ce_oi = float(np.nansum(ce_oi)) if ce_oi.size else 0.0
    total_pe_oi = float(np.nansum(pe_oi)) if pe_oi.size else 0.0
    total_ce_chg = float(np.nansum(ce_chg)) if ce_chg.size else 0.0
    total_pe_chg = float(np.nansum(pe_chg)) if pe_chg.size else 0.0

    support = None
    resistance = None
    atm_strike = None

    if strikes.size:
        if np.isfinite(pe_oi).any():
            support = float(strikes[int(np.nanargmax(pe_oi))])

        if np.isfinite(ce_oi).any():
            resistance = float(strikes[int(np.nanargmax(ce_oi))])

        if spot is not None:
            atm_idx = int(np.nanargmin(np.abs(strikes - float(spot))))
            atm_strike = float(strikes[atm_idx])

    trend = "Neutral"
    if pcr is not None:
        if pcr >= 1.3 and total_pe_chg > total_ce_chg:
            trend = "Strong Uptrend"
        elif pcr >= 1.05 and total_pe_chg > total_ce_chg:
            trend = "Uptrend"
        elif pcr <= 0.7 and total_ce_chg > total_pe_chg:
            trend = "Strong Downtrend"
        elif pcr < 0.95 and total_ce_chg > total_pe_chg:
            trend = "Downtrend"

    snapshot = {
        "timestamp": ts,
        "spot": spot,
        "pcr": pcr,
        "support": support,
        "resistance": resistance,
        "atm_strike": atm_strike,
        "trend": trend,
        "total_ce_oi": total_ce_oi,
        "total_pe_oi": total_pe_oi,
        "total_ce_chg_oi": total_ce_chg,
        "total_pe_chg_oi": total_pe_chg,
    }

    dq = store["snapshots"]
    if not dq or dq[-1].get("timestamp") != ts:
        dq.append(snapshot)
        store["meta"]["last_snapshot_ts"] = ts


# =========================================================
# DATAFRAME FOR RENDER
# =========================================================
def store_to_df() -> pd.DataFrame:
    store = get_store()
    if not store["ready"] or store["n"] == 0:
        return pd.DataFrame()

    n = store["n"]
    spot = store["meta"]["spot"]
    global_pcr = store["meta"]["pcr"]

    df = pd.DataFrame(
        {
            "CE_IV": store["ce_iv"],
            "CE_GAMMA": store["ce_gamma"],
            "CE_THETA": store["ce_theta"],
            "CE_DELTA": store["ce_delta"],
            "CE_CHG_OI": store["ce_chg_oi"],
            "CE_OI": store["ce_oi"],
            "CE_VOLUME": store["ce_volume"],
            "CE_LTP": store["ce_ltp"],
            "STRIKE": store["strike"],
            "PCR": np.where(np.isnan(store["pcr_row"]), global_pcr, store["pcr_row"]),
            "PE_LTP": store["pe_ltp"],
            "PE_VOLUME": store["pe_volume"],
            "PE_OI": store["pe_oi"],
            "PE_CHG_OI": store["pe_chg_oi"],
            "PE_DELTA": store["pe_delta"],
            "PE_THETA": store["pe_theta"],
            "PE_GAMMA": store["pe_gamma"],
            "PE_IV": store["pe_iv"],
            "CE_BID": store["ce_bid"],
            "CE_ASK": store["ce_ask"],
            "PE_BID": store["pe_bid"],
            "PE_ASK": store["pe_ask"],
            "SPOT": np.full(n, np.nan if spot is None else float(spot), dtype=np.float64),
        }
    )

    return df


# =========================================================
# UI PREP
# =========================================================
def build_display_df(df: pd.DataFrame, spot: float | None) -> tuple[pd.DataFrame, int | None]:
    if df.empty:
        return pd.DataFrame(), None

    nearest_idx: int | None = None
    view_df = df.copy()

    if spot is not None:
        nearest_idx = int((view_df["STRIKE"] - spot).abs().idxmin())

    view_df["CE_OI_LAKHS"] = view_df["CE_OI"] / 100000.0
    view_df["PE_OI_LAKHS"] = view_df["PE_OI"] / 100000.0
    view_df["CE_VOL_FMT"] = view_df["CE_VOLUME"].apply(fmt_volume)
    view_df["PE_VOL_FMT"] = view_df["PE_VOLUME"].apply(fmt_volume)

    view_df["CE_OI_PCT"] = (
        (view_df["CE_CHG_OI"] / view_df["CE_OI"]) * 100
    ).replace([float("inf"), -float("inf")], pd.NA)

    view_df["PE_OI_PCT"] = (
        (view_df["PE_CHG_OI"] / view_df["PE_OI"]) * 100
    ).replace([float("inf"), -float("inf")], pd.NA)

    ce_mid = (view_df["CE_BID"].fillna(0) + view_df["CE_ASK"].fillna(0)) / 2
    pe_mid = (view_df["PE_BID"].fillna(0) + view_df["PE_ASK"].fillna(0)) / 2

    view_df["CE_LTP_PCT"] = (
        ((view_df["CE_LTP"] - ce_mid) / ce_mid.replace(0, pd.NA)) * 100
    ).replace([float("inf"), -float("inf")], pd.NA)

    view_df["PE_LTP_PCT"] = (
        ((view_df["PE_LTP"] - pe_mid) / pe_mid.replace(0, pd.NA)) * 100
    ).replace([float("inf"), -float("inf")], pd.NA)

    ordered_cols = [
        "CE_IV",
        "CE_GAMMA",
        "CE_THETA",
        "CE_DELTA",
        "CE_CHG_OI",
        "CE_OI_LAKHS",
        "CE_VOL_FMT",
        "CE_LTP",
        "STRIKE",
        "PCR",
        "PE_LTP",
        "PE_VOL_FMT",
        "PE_OI_LAKHS",
        "PE_CHG_OI",
        "PE_DELTA",
        "PE_THETA",
        "PE_GAMMA",
        "PE_IV",
        "CE_OI_PCT",
        "PE_OI_PCT",
        "CE_LTP_PCT",
        "PE_LTP_PCT",
    ]

    view_df = view_df[ordered_cols].copy()
    return view_df, nearest_idx


# =========================================================
# HTML TABLE
# =========================================================
def render_option_chain_html(display_df: pd.DataFrame, nearest_idx: int | None) -> str:
    if display_df.empty:
        return "<div style='padding:8px;'>No option chain data available.</div>"

    def fmt_main(col_name: str, val: Any) -> str:
        if val is None or pd.isna(val):
            return "-"

        if col_name in ["CE_CHG_OI", "PE_CHG_OI", "STRIKE"]:
            return fmt_int(val)

        if col_name in ["CE_OI_LAKHS", "PE_OI_LAKHS"]:
            return fmt_num(val, 1)

        if col_name in ["CE_VOL_FMT", "PE_VOL_FMT"]:
            return str(val)

        if "GAMMA" in col_name:
            return fmt_num(val, 4)

        if "DELTA" in col_name or "THETA" in col_name:
            return fmt_num(val, 4)

        if "IV" in col_name:
            return fmt_num(val, 2)

        return fmt_num(val, 2)

    def fmt_sub_pct(val: Any) -> str:
        if val is None or pd.isna(val):
            return ""
        sign = "+" if float(val) > 0 else ""
        return f"{sign}{float(val):,.2f} %"

    headers = [
        ("CE_IV", "IV"),
        ("CE_GAMMA", "Gamma"),
        ("CE_THETA", "Theta"),
        ("CE_DELTA", "Delta"),
        ("CE_CHG_OI", "OI (chg)"),
        ("CE_OI_LAKHS", "OI (lakhs)"),
        ("CE_VOL_FMT", "Volume"),
        ("CE_LTP", "LTP"),
        ("STRIKE", "Strike"),
        ("PE_LTP", "LTP"),
        ("PE_VOL_FMT", "Volume"),
        ("PE_OI_LAKHS", "OI (lakhs)"),
        ("PE_CHG_OI", "OI (chg)"),
        ("PE_DELTA", "Delta"),
        ("PE_THETA", "Theta"),
        ("PE_GAMMA", "Gamma"),
        ("PE_IV", "IV"),
    ]

    header_html = "<tr>"
    for _, label in headers:
        extra_cls = " strike-head" if label == "Strike" else ""
        header_html += f"<th class='{extra_cls}'>{esc(label)}</th>"
    header_html += "</tr>"

    rows_html = ""
    for row_idx, (_, row) in enumerate(display_df.iterrows()):
        is_nearest = nearest_idx is not None and row_idx == nearest_idx
        tr_class = "oc-nearest" if is_nearest else ""

        ce_oi_pct = fmt_sub_pct(row["CE_OI_PCT"])
        pe_oi_pct = fmt_sub_pct(row["PE_OI_PCT"])
        ce_ltp_pct = fmt_sub_pct(row["CE_LTP_PCT"])
        pe_ltp_pct = fmt_sub_pct(row["PE_LTP_PCT"])

        pcr_text = ""
        if row["PCR"] is not None and not pd.isna(row["PCR"]):
            pcr_text = f"PCR: {float(row['PCR']):.2f}"

        def dual_cell(main: str, sub: str = "", cls: str = "") -> str:
            sub_html = f"<div class='oc-sub {cls}'>{esc(sub)}</div>" if sub else "<div class='oc-sub'>&nbsp;</div>"
            return f"<td><div class='oc-main {cls}'>{esc(main)}</div>{sub_html}</td>"

        rows_html += f"<tr class='{tr_class}'>"
        rows_html += dual_cell(fmt_main("CE_IV", row["CE_IV"]))
        rows_html += dual_cell(fmt_main("CE_GAMMA", row["CE_GAMMA"]))
        rows_html += dual_cell(fmt_main("CE_THETA", row["CE_THETA"]))
        rows_html += dual_cell(fmt_main("CE_DELTA", row["CE_DELTA"]))
        rows_html += dual_cell(fmt_main("CE_CHG_OI", row["CE_CHG_OI"]))
        rows_html += dual_cell(fmt_main("CE_OI_LAKHS", row["CE_OI_LAKHS"]), ce_oi_pct, "oc-green")
        rows_html += dual_cell(fmt_main("CE_VOL_FMT", row["CE_VOL_FMT"]))
        rows_html += dual_cell(fmt_main("CE_LTP", row["CE_LTP"]), ce_ltp_pct, "oc-orange")

        rows_html += (
            f"<td class='oc-strike-col'>"
            f"<div class='oc-strike-main'>{esc(fmt_main('STRIKE', row['STRIKE']))}</div>"
            f"<div class='oc-strike-sub'>{esc(pcr_text) if pcr_text else '&nbsp;'}</div>"
            f"</td>"
        )

        rows_html += dual_cell(fmt_main("PE_LTP", row["PE_LTP"]), pe_ltp_pct, "oc-orange")
        rows_html += dual_cell(fmt_main("PE_VOL_FMT", row["PE_VOL_FMT"]))
        rows_html += dual_cell(fmt_main("PE_OI_LAKHS", row["PE_OI_LAKHS"]), pe_oi_pct, "oc-green")
        rows_html += dual_cell(fmt_main("PE_CHG_OI", row["PE_CHG_OI"]))
        rows_html += dual_cell(fmt_main("PE_DELTA", row["PE_DELTA"]))
        rows_html += dual_cell(fmt_main("PE_THETA", row["PE_THETA"]))
        rows_html += dual_cell(fmt_main("PE_GAMMA", row["PE_GAMMA"]))
        rows_html += dual_cell(fmt_main("PE_IV", row["PE_IV"]))
        rows_html += "</tr>"

    return f"""
    <div class="oc-shell">
        <div class="oc-wrap" style="height:{TABLE_HEIGHT_PX}px; max-height:{TABLE_HEIGHT_PX}px;">
            <table class="oc-table">
                <thead>{header_html}</thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
    </div>
    """


# =========================================================
# HEADER
# =========================================================
def show_header() -> None:
    st.markdown(f'<div class="main-title">{APP_TITLE}</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="sub-title">Expiry: {EXPIRY} | Instrument: {INSTRUMENT}</div>',
        unsafe_allow_html=True,
    )


# =========================================================
# UI PAINT
# =========================================================
def paint_ui(
    spot_ph,
    pcr_ph,
    open_ph,
    high_ph,
    low_ph,
    prev_close_ph,
    snapshots_ph,
    trend_ph,
    table_ph,
    status_ph,
    fetch_error_ph,
) -> None:
    store = get_store()
    meta = store["meta"]
    df = store_to_df()

    spot_ph.metric("Spot", fmt_num(meta["spot"], 2))
    pcr_ph.metric("PCR", fmt_num(meta["pcr"], 2))
    open_ph.metric("Open", fmt_num(meta["open"], 2))
    high_ph.metric("High", fmt_num(meta["high"], 2))
    low_ph.metric("Low", fmt_num(meta["low"], 2))
    prev_close_ph.metric("Prev Close", fmt_num(meta["prev_close"], 2))

    latest_trend = "-"
    if store["snapshots"]:
        latest_trend = store["snapshots"][-1].get("trend") or "-"

    snapshots_ph.metric("Snapshots", str(len(store["snapshots"])))
    trend_ph.metric("Trend", latest_trend)

    if not df.empty:
        display_df, nearest_idx = build_display_df(df, meta["spot"])
        table_html = render_option_chain_html(display_df, nearest_idx)
        table_ph.markdown(table_html, unsafe_allow_html=True)
    else:
        table_ph.warning("No option chain data available.")

    status_text = (
        f"REST refresh: "
        f"{datetime.fromtimestamp(meta['last_rest_refresh_ts']).strftime('%H:%M:%S') if meta['last_rest_refresh_ts'] else '-'}"
        f" | WS apply: {meta['last_ws_apply_ts'] or '-'}"
        f" | Last snapshot: {meta['last_snapshot_ts'] or '-'}"
    )
    status_ph.caption(status_text)

    if st.session_state.nifty_fetch_error:
        fetch_error_ph.error(st.session_state.nifty_fetch_error)
    else:
        fetch_error_ph.empty()

    meta["last_ui_update_ts"] = now_str()


# =========================================================
# MAIN APP
# =========================================================
def main() -> None:
    show_header()

    token = get_access_token()
    if not token:
        st.error("Access token not found. Please run login.py first.")
        return

    try:
        if not get_store()["ready"]:
            bootstrap_store_from_rest(token, force=True)
            append_compact_snapshot()
    except Exception as exc:
        st.session_state.nifty_fetch_error = f"Initial load error: {exc}"

    top_cols = st.columns(8)
    spot_ph = top_cols[0].empty()
    pcr_ph = top_cols[1].empty()
    open_ph = top_cols[2].empty()
    high_ph = top_cols[3].empty()
    low_ph = top_cols[4].empty()
    prev_close_ph = top_cols[5].empty()
    snapshots_ph = top_cols[6].empty()
    trend_ph = top_cols[7].empty()

    st.divider()
    table_ph = st.empty()
    st.divider()
    status_ph = st.empty()
    fetch_error_ph = st.empty()

    paint_ui(
        spot_ph,
        pcr_ph,
        open_ph,
        high_ph,
        low_ph,
        prev_close_ph,
        snapshots_ph,
        trend_ph,
        table_ph,
        status_ph,
        fetch_error_ph,
    )

    @st.fragment(run_every=f"{REFRESH_SECONDS}s")
    def live_updater() -> None:
        try:
            bootstrap_store_from_rest(token, force=False)
            apply_ws_ticks_to_store()
            append_compact_snapshot()
        except Exception as exc:
            st.session_state.nifty_fetch_error = f"Live update error: {exc}"

        paint_ui(
            spot_ph,
            pcr_ph,
            open_ph,
            high_ph,
            low_ph,
            prev_close_ph,
            snapshots_ph,
            trend_ph,
            table_ph,
            status_ph,
            fetch_error_ph,
        )

    live_updater()


if __name__ == "__main__":
    main()