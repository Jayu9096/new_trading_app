from __future__ import annotations

import json
import threading
import time
from typing import Any

import websocket

# =========================================================
# SHARED LIVE STATE
# =========================================================
ws_app: websocket.WebSocketApp | None = None
ws_thread: threading.Thread | None = None

connected = False
connecting = False

# live tick store: instrument_key -> latest tick values
ticks: dict[str, dict[str, Any]] = {}

# subscription tracking
subscribed_keys: set[str] = set()

# lock to avoid race conditions
_state_lock = threading.Lock()


# =========================================================
# HELPERS
# =========================================================
def _log(msg: str) -> None:
    print(f"[UPSTOX_WS] {msg}")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def is_connected() -> bool:
    global connected
    return connected


def get_ticks_copy() -> dict[str, dict[str, Any]]:
    with _state_lock:
        return dict(ticks)


# =========================================================
# MESSAGE PARSER
# =========================================================
def _extract_tick_payload(feed_value: dict[str, Any]) -> dict[str, Any]:
    """
    Upstox feed payloads can vary a bit.
    We try multiple common paths and normalize the fields.
    """
    ltpc = feed_value.get("ltpc", {}) or {}
    oi_block = feed_value.get("oi", {}) or {}

    last_price = _safe_float(ltpc.get("ltp"))
    volume = _safe_float(ltpc.get("volume"))
    oi = _safe_float(oi_block.get("oi"))

    return {
        "last_price": last_price,
        "volume": volume,
        "oi": oi,
        "raw": feed_value,
        "updated_at": time.time(),
    }


def on_message(ws: websocket.WebSocketApp, message: str) -> None:
    global ticks

    try:
        data = json.loads(message)
    except Exception as exc:
        _log(f"JSON decode error: {exc}")
        return

    feeds = data.get("feeds")
    if not isinstance(feeds, dict):
        return

    with _state_lock:
        for instrument_key, feed_value in feeds.items():
            if not isinstance(feed_value, dict):
                continue

            new_tick = _extract_tick_payload(feed_value)

            prev = ticks.get(instrument_key, {})
            merged = {
                "last_price": new_tick["last_price"] if new_tick["last_price"] is not None else prev.get("last_price"),
                "volume": new_tick["volume"] if new_tick["volume"] is not None else prev.get("volume"),
                "oi": new_tick["oi"] if new_tick["oi"] is not None else prev.get("oi"),
                "raw": new_tick["raw"],
                "updated_at": new_tick["updated_at"],
            }
            ticks[instrument_key] = merged


def on_error(ws: websocket.WebSocketApp, error: Any) -> None:
    global connected, connecting
    connected = False
    connecting = False
    _log(f"ERROR: {error}")


def on_close(
    ws: websocket.WebSocketApp,
    close_status_code: int | None,
    close_msg: str | None,
) -> None:
    global connected, connecting
    connected = False
    connecting = False
    _log(f"CLOSED: code={close_status_code}, msg={close_msg}")


def on_open(ws: websocket.WebSocketApp) -> None:
    global connected, connecting
    connected = True
    connecting = False
    _log("CONNECTED")

    # re-subscribe existing keys on reconnect
    if subscribed_keys:
        try:
            _send_subscribe(sorted(subscribed_keys))
            _log(f"Re-subscribed {len(subscribed_keys)} instruments")
        except Exception as exc:
            _log(f"Re-subscribe failed: {exc}")


# =========================================================
# SUBSCRIPTION PAYLOAD
# =========================================================
def _build_subscribe_message(keys: list[str]) -> dict[str, Any]:
    """
    Common Upstox market data subscription payload structure.
    """
    return {
        "guid": "option-chain-live",
        "method": "sub",
        "data": {
            "mode": "full",
            "instrumentKeys": keys,
        },
    }


def _send_subscribe(keys: list[str]) -> None:
    global ws_app

    if not ws_app:
        raise RuntimeError("WebSocket not initialized")

    if not keys:
        return

    payload = _build_subscribe_message(keys)
    ws_app.send(json.dumps(payload))


# =========================================================
# PUBLIC API
# =========================================================
def start_ws(token: str) -> None:
    global ws_app, ws_thread, connected, connecting

    if connected or connecting:
        return

    connecting = True

    url = "wss://api.upstox.com/v2/feed/market-data-feed"

    headers = [
        f"Authorization: Bearer {token}",
        "Accept: */*",
    ]

    ws_app = websocket.WebSocketApp(
        url,
        header=headers,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    def _runner() -> None:
        global connected, connecting
        try:
            ws_app.run_forever(
                ping_interval=20,
                ping_timeout=10,
                skip_utf8_validation=True,
            )
        except Exception as exc:
            connected = False
            connecting = False
            _log(f"run_forever exception: {exc}")

    ws_thread = threading.Thread(target=_runner, daemon=True)
    ws_thread.start()

    # wait a little for connect
    for _ in range(20):
        if connected:
            return
        time.sleep(0.25)

    if not connected:
        connecting = False
        _log("Connection not established yet")


def subscribe(keys: list[str]) -> None:
    global subscribed_keys

    if not keys:
        return

    clean_keys = sorted({k for k in keys if k})
    if not clean_keys:
        return

    with _state_lock:
        new_keys = [k for k in clean_keys if k not in subscribed_keys]
        subscribed_keys.update(clean_keys)

    if not new_keys:
        return

    if not connected or not ws_app:
        _log("Subscribe skipped for now; socket not connected yet")
        return

    try:
        _send_subscribe(new_keys)
        _log(f"Subscribed {len(new_keys)} instruments")
    except Exception as exc:
        _log(f"Subscribe failed: {exc}")


def unsubscribe(keys: list[str]) -> None:
    global ws_app, subscribed_keys

    if not keys or not ws_app or not connected:
        return

    clean_keys = sorted({k for k in keys if k})
    if not clean_keys:
        return

    payload = {
        "guid": "option-chain-live-unsub",
        "method": "unsub",
        "data": {
            "instrumentKeys": clean_keys,
        },
    }

    try:
        ws_app.send(json.dumps(payload))
        with _state_lock:
            for key in clean_keys:
                subscribed_keys.discard(key)
        _log(f"Unsubscribed {len(clean_keys)} instruments")
    except Exception as exc:
        _log(f"Unsubscribe failed: {exc}")


def stop_ws() -> None:
    global ws_app, ws_thread, connected, connecting

    connected = False
    connecting = False

    try:
        if ws_app:
            ws_app.close()
    except Exception:
        pass

    ws_app = None
    ws_thread = None
    _log("STOPPED")


def clear_ticks() -> None:
    with _state_lock:
        ticks.clear()


def get_tick(instrument_key: str) -> dict[str, Any] | None:
    with _state_lock:
        tick = ticks.get(instrument_key)
        return dict(tick) if tick else None