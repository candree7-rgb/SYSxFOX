"""
FoxSignals WebSocket Test Script

Verbindet sich mit dem FoxSignals WebSocket und loggt alle Nachrichten.
Läuft lokal - zeigt dir das exakte Format der Signale.

Usage:
    python test_foxsignals_ws.py
"""

import json
import time
import threading
from websocket import WebSocketApp

# =============================================================================
# CONFIGURATION - Paste your JWT token here
# =============================================================================
JWT_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6Ijk4OGQ1YTM3OWI3OGJkZjFlNTBhNDA5MTEzZjJiMGM3NWU0NTJlNDciLCJ0eXAiOiJKV1QifQ.eyJuYW1lIjoiQW5kcsOpIENhcnZhIiwicGljdHVyZSI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0pPVldlOHhZU1JzSjBWTjByY25OVnN4MXM0c0NRX1F0YTVaZnkxWEd4cW1kaWNsRzNvPXM5Ni1jIiwiaXNzIjoiaHR0cHM6Ly9zZWN1cmV0b2tlbi5nb29nbGUuY29tL2ZveC1zaWduYWxzIiwiYXVkIjoiZm94LXNpZ25hbHMiLCJhdXRoX3RpbWUiOjE3NTY0NTM1NDcsInVzZXJfaWQiOiJWanlNakJxRzVkVU1mUDZOQWFyVkNqbFVkZVAyIiwic3ViIjoiVmp5TWpCcUc1ZFVNZlA2TkFhclZDamxVZGVQMiIsImlhdCI6MTc2NjQ3MTIyMiwiZXhwIjoxNzY2NDc0ODIyLCJlbWFpbCI6ImMuYW5kcmVlN0BnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZmlyZWJhc2UiOnsiaWRlbnRpdGllcyI6eyJnb29nbGUuY29tIjpbIjEwNzg2NDQ0NTIxNzI0NTM0MzcyMCJdLCJlbWFpbCI6WyJjLmFuZHJlZTdAZ21haWwuY29tIl19LCJzaWduX2luX3Byb3ZpZGllciI6Imdvb2dsZS5jb20ifX0.IlyZxL8-sQ8Psl9J3Gteb9G9TjdWXoHZqFZtpu6GlTzlFU7Ep4zE55jpcgrOk-Qy9VgyK5Zwiz52_b6Jj14W4a-o-3ES1XhH17aXdHk8QqCouCK0b6ikPgu-slZUNbW2LRxycJHR5U8PGA7Mf4O4J4guCby5FFYtbj21Ea7EOCulYrBGYGeKYgYJbCBjkeuIgi_xBo77zOlSPsjV8YBALnc7HViajufLJibU1El0G0MJIgda6i2Owph4K-pd0Wsal6toTAR3E3smEi_7nfK40YEyHVVIgxdzGTsfbJyqjWndwPTeJuw82OlLlG9DSirQBdD640s1zhv_IUxvG08ACA"

USER_ID = "VjyMjBqG5dUMfP6NAarVCjlUdeP2"

WS_URL = "wss://serverapi.getfoxsignals.com/socketio/socket.io/?EIO=4&transport=websocket"

# =============================================================================

def parse_socketio_message(data: str):
    """Parse Socket.IO message format.

    Socket.IO v4 message types:
    0 - open
    1 - close
    2 - ping
    3 - pong
    4 - message
    40 - connect
    41 - disconnect
    42 - event
    43 - ack
    """
    if not data:
        return None, None

    # Get message type (first 1-2 chars)
    if data.startswith("42"):
        # Event message - parse JSON payload
        try:
            payload = json.loads(data[2:])
            event_name = payload[0] if isinstance(payload, list) else None
            event_data = payload[1] if isinstance(payload, list) and len(payload) > 1 else payload
            return "event", {"name": event_name, "data": event_data}
        except:
            return "event", data[2:]
    elif data.startswith("43"):
        # Ack
        return "ack", data[2:]
    elif data.startswith("40"):
        # Connect
        return "connect", data[2:]
    elif data.startswith("41"):
        # Disconnect
        return "disconnect", data[2:]
    elif data == "2":
        return "ping", None
    elif data == "3":
        return "pong", None
    elif data.startswith("0"):
        # Open - contains session info
        try:
            return "open", json.loads(data[1:])
        except:
            return "open", data[1:]
    else:
        return "unknown", data


class FoxSignalsTest:
    def __init__(self):
        self.ws = None
        self.connected = False
        self.message_count = 0

    def on_open(self, ws):
        print("\n" + "="*60)
        print("✓ WebSocket CONNECTED")
        print("="*60)
        print("\nWaiting for messages... (Ctrl+C to stop)\n")
        self.connected = True

    def on_message(self, ws, message):
        self.message_count += 1
        msg_type, payload = parse_socketio_message(message)

        timestamp = time.strftime("%H:%M:%S")

        if msg_type == "ping":
            # Respond to ping with pong
            ws.send("3")
            print(f"[{timestamp}] ← PING → PONG")
            return

        if msg_type == "pong":
            return

        if msg_type == "open":
            print(f"[{timestamp}] ← OPEN: {json.dumps(payload, indent=2)}")
            # Nach dem Open senden wir den Connect mit Auth
            auth_msg = f'40{{"jsonwebtoken":"{JWT_TOKEN}","userid":"{USER_ID}"}}'
            ws.send(auth_msg)
            print(f"[{timestamp}] → AUTH sent")
            return

        if msg_type == "connect":
            print(f"[{timestamp}] ← CONNECTED to Socket.IO namespace")
            return

        if msg_type == "event":
            print(f"\n[{timestamp}] ← EVENT #{self.message_count}")
            print("-"*50)
            if isinstance(payload, dict):
                print(f"  Event Name: {payload.get('name', 'unknown')}")
                print(f"  Data:")
                data = payload.get('data', {})
                if isinstance(data, dict):
                    print(json.dumps(data, indent=4, ensure_ascii=False))
                else:
                    print(f"    {data}")
            else:
                print(f"  Raw: {payload}")
            print("-"*50 + "\n")
            return

        # Unknown message type
        print(f"[{timestamp}] ← [{msg_type}] {message[:200]}")

    def on_error(self, ws, error):
        print(f"\n✗ ERROR: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"\n✗ DISCONNECTED (code={close_status_code}, msg={close_msg})")
        self.connected = False

    def run(self):
        print("\n" + "="*60)
        print("FoxSignals WebSocket Test")
        print("="*60)
        print(f"\nConnecting to: {WS_URL[:50]}...")
        print(f"User ID: {USER_ID}")
        print(f"JWT expires: Check if still valid!\n")

        headers = {
            "User-Agent": "Dart/3.9 (dart:io)",
            "appversion": "1.0.208",
            "appbuildnumber": "208",
            "jsonwebtoken": JWT_TOKEN,
            "userid": USER_ID,
        }

        self.ws = WebSocketApp(
            WS_URL,
            header=headers,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        # Run with ping/pong
        self.ws.run_forever(
            ping_interval=25,
            ping_timeout=10,
        )


if __name__ == "__main__":
    try:
        test = FoxSignalsTest()
        test.run()
    except KeyboardInterrupt:
        print("\n\nStopped by user")
