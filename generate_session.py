#!/usr/bin/env python3
"""
Generate Telegram Session String

Run this script locally ONCE to generate a session string.
Then add the session string to your Railway ENV as TELEGRAM_SESSION_STRING.

Usage:
    python generate_session.py

You will need:
1. TELEGRAM_API_ID (from https://my.telegram.org/apps)
2. TELEGRAM_API_HASH (from https://my.telegram.org/apps)
3. Your phone number (with country code, e.g., +491234567890)
"""

import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv()


async def main():
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")

    if not api_id or not api_hash:
        print("ERROR: Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env file")
        print("\nGet these from: https://my.telegram.org/apps")
        return

    api_id = int(api_id)

    print("=" * 60)
    print("Telegram Session Generator")
    print("=" * 60)
    print()
    print("This will generate a session string for your Telegram account.")
    print("You'll receive a code via Telegram - enter it when prompted.")
    print()

    # Create client with string session
    client = TelegramClient(StringSession(), api_id, api_hash)

    await client.start()

    # Get the session string
    session_string = client.session.save()

    print()
    print("=" * 60)
    print("SUCCESS! Your session string is:")
    print("=" * 60)
    print()
    print(session_string)
    print()
    print("=" * 60)
    print("Add this to your Railway ENV as:")
    print("TELEGRAM_SESSION_STRING=<the string above>")
    print("=" * 60)

    # Test: get username
    me = await client.get_me()
    print(f"\nLogged in as: {me.first_name} (@{me.username})")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
