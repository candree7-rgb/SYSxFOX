"""
Telegram Reader Module - Event-Based (Real-Time)

Listens to messages from one or more Telegram channels using Telethon events.
Messages are received instantly, no polling needed.

Setup:
1. Get API credentials from https://my.telegram.org/apps
2. Set env vars:
   - TELEGRAM_API_ID: Your API ID
   - TELEGRAM_API_HASH: Your API Hash
   - TELEGRAM_CHANNEL: Channel ID(s), comma-separated (e.g., "-1002299206473,-1001234567890")
   - TELEGRAM_SESSION_STRING: Session string for persistent auth
"""

from telethon import events
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from typing import Callable, Optional, List, Set
import logging
import time

log = logging.getLogger("telegram_reader")


class TelegramReader:
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        channels: str,
        session_string: str = "",
    ):
        self.api_id = api_id
        self.api_hash = api_hash
        self.channels_raw = channels
        self.session = StringSession(session_string)
        self.client: Optional[TelegramClient] = None
        self._message_handler: Optional[Callable] = None
        self._chat_ids: Set[int] = set()

        # Parse channel IDs (comma-separated)
        for ch in channels.split(","):
            ch = ch.strip()
            if not ch:
                continue
            try:
                self._chat_ids.add(int(ch))
            except ValueError:
                # Username - we'll accept all if any username is given
                log.warning(f"Username '{ch}' given - will accept messages from all chats. Use numeric IDs for filtering.")

    def _match_chat(self, event) -> bool:
        """Check if message is from one of our target channels."""
        if not self._chat_ids:
            # No numeric IDs configured, accept all
            return True
        return event.chat_id in self._chat_ids

    def set_message_handler(self, handler: Callable) -> None:
        """Set the callback function for new messages.

        Handler receives: (message_id: int, text: str, timestamp: float)
        """
        self._message_handler = handler

    def start(self) -> None:
        """Start the Telegram client and listen for messages."""
        self.client = TelegramClient(self.session, self.api_id, self.api_hash)

        @self.client.on(events.NewMessage())
        async def handler(event):
            # Check if from one of our channels
            if not self._match_chat(event):
                return

            text = event.raw_text or ""
            if not text:
                return

            msg_id = event.id
            timestamp = event.date.timestamp() if event.date else time.time()

            log.debug(f"New message from {event.chat_id}: {text[:100]}...")

            if self._message_handler:
                try:
                    self._message_handler(msg_id, text, timestamp)
                except Exception as e:
                    log.error(f"Message handler error: {e}")

        self.client.start()
        log.info(f"Telegram client started, listening to {len(self._chat_ids)} channel(s): {self._chat_ids}")

    def run_forever(self) -> None:
        """Block and run until disconnected."""
        if self.client:
            self.client.run_until_disconnected()

    def disconnect(self) -> None:
        """Disconnect the client."""
        if self.client:
            self.client.disconnect()

    def get_session_string(self) -> str:
        """Get the current session string for persistence."""
        return self.session.save()
