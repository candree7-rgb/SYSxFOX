"""
FoxSignals WebSocket Test Script

Verbindet sich mit dem FoxSignals WebSocket und loggt alle Nachrichten.
Zeigt das exakte Format der Signale.

Usage (lokal):
    export FOXSIGNALS_JWT="your_token_here"
    export FOXSIGNALS_USER_ID="your_user_id"
    python test_foxsignals_ws.py

Usage (Railway):
    Set ENV vars: FOXSIGNALS_JWT, FOXSIGNALS_USER_ID
    Change start command to: python test_foxsignals_ws.py
"""

import os
import json
import time
from websocket import WebSocketApp

# =============================================================================
# CONFIGURATION - From ENV variables
# =============================================================================
JWT_TOKEN = os.getenv("FOXSIGNALS_JWT", "")
USER_ID = os.getenv("FOXSIGNALS_USER_ID", "")

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
            # Respond to ping with pong (silently)
            ws.send("3")
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
            if isinstance(payload, dict):
                event_name = payload.get('name', 'unknown')

                # IGNORE these noisy events (only hide data, still log name)
                ignored_events = ['prices_all', 'price', 'prices', 'ticker', 'tickers']
                if event_name in ignored_events:
                    return  # Skip price updates completely

                # Log ALL other events with full data
                print(f"\n[{timestamp}] ← EVENT: {event_name}")
                print("="*60)
                data = payload.get('data', {})
                print(json.dumps(data, indent=2, ensure_ascii=False))
                print("="*60 + "\n")

                # Also save to see all unique event names
                print(f">>> NEW EVENT TYPE FOUND: {event_name} <<<")
            else:
                print(f"\n[{timestamp}] ← EVENT: {payload}")
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
    # Validate ENV vars
    if not JWT_TOKEN:
        print("ERROR: FOXSIGNALS_JWT env variable not set!")
        print("Get it from HTTP Toolkit -> jsonwebtoken header")
        exit(1)
    if not USER_ID:
        print("ERROR: FOXSIGNALS_USER_ID env variable not set!")
        print("Get it from HTTP Toolkit -> userid header")
        exit(1)

    try:
        test = FoxSignalsTest()
        test.run()
    except KeyboardInterrupt:
        print("\n\nStopped by user")
