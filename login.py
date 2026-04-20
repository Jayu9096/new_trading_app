from __future__ import annotations

import json
import os
import threading
import webbrowser
from typing import Any

import requests
from dotenv import load_dotenv
from flask import Flask, request

# =========================================================
# LOAD ENV
# =========================================================
load_dotenv()

API_KEY = os.getenv("UPSTOX_API_KEY")
API_SECRET = os.getenv("UPSTOX_API_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")

TOKEN_FILE = os.path.join(os.getcwd(), "upstox_token.json")

app = Flask(__name__)


# =========================================================
# HELPERS
# =========================================================
def validate_env() -> None:
    missing: list[str] = []

    if not API_KEY:
        missing.append("UPSTOX_API_KEY")
    if not API_SECRET:
        missing.append("UPSTOX_API_SECRET")
    if not REDIRECT_URI:
        missing.append("REDIRECT_URI")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


def get_login_url() -> str:
    validate_env()
    return (
        "https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code"
        f"&client_id={API_KEY}"
        f"&redirect_uri={REDIRECT_URI}"
    )


def save_token(data: dict[str, Any]) -> None:
    print("Saving token to:", TOKEN_FILE)

    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    print("✅ Token saved successfully")


def load_token_file() -> dict[str, Any] | None:
    if not os.path.exists(TOKEN_FILE):
        return None

    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"Failed to read token file: {exc}")
        return None


def get_access_token() -> str | None:
    data = load_token_file()
    if not data:
        print("Token file not found or invalid. Run login.py first.")
        return None

    token = data.get("access_token")
    if not token:
        print("Access token not found in token file.")
        return None

    return token


def is_token_available() -> bool:
    return get_access_token() is not None


def exchange_code_for_token(code: str) -> dict[str, Any]:
    validate_env()

    url = "https://api.upstox.com/v2/login/authorization/token"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "code": code,
        "client_id": API_KEY,
        "client_secret": API_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }

    response = requests.post(url, headers=headers, data=data, timeout=20)

    try:
        response_data = response.json()
    except Exception:
        response_data = {"raw_text": response.text}

    if response.status_code != 200:
        raise RuntimeError(
            f"Token exchange failed. Status={response.status_code}, Response={response_data}"
        )

    return response_data


# =========================================================
# FLASK CALLBACK
# =========================================================
@app.route("/callback")
def callback():
    code = request.args.get("code")
    error = request.args.get("error")

    print("Callback triggered")
    print("Auth Code:", code)

    if error:
        return f"Login failed: {error}"

    if not code:
        return "No auth code received."

    try:
        token_data = exchange_code_for_token(code)

        save_token(
            {
                "auth_code": code,
                "access_token": token_data.get("access_token"),
                "full": token_data,
            }
        )

        shutdown = request.environ.get("werkzeug.server.shutdown")
        if shutdown:
            shutdown()

        return "✅ Token saved successfully. You can close this window."

    except Exception as exc:
        print("Token exchange error:", exc)
        return f"Token exchange failed: {exc}"


# =========================================================
# MAIN LOGIN RUNNER
# =========================================================
def open_login() -> None:
    login_url = get_login_url()

    print("Login URL:")
    print(login_url)

    webbrowser.open(login_url)


def run(host: str = "127.0.0.1", port: int = 5000) -> None:
    validate_env()

    print("Starting Upstox login server...")
    print(f"Callback URL should match: {REDIRECT_URI}")

    threading.Timer(1.0, open_login).start()
    app.run(host=host, port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    run()