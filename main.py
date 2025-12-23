"""
Telegram Signal Bot for Bybit Trading - Event-Based (Real-Time)

Listens to Telegram channel in real-time and executes trades on Bybit.

Features:
- Real-time Telegram signals (event-based, no polling)
- No DCA - fixed 5% SL
- TP splits: 15/25/25/25 (90% total, 10% runner)
- SL -> BE after TP1
- Trailing stop after TP4
"""

import sys
import time
import threading
import logging

from config import (
    TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNEL,
    TELEGRAM_SESSION_STRING,
    BYBIT_API_KEY, BYBIT_API_SECRET, BYBIT_TESTNET, BYBIT_DEMO, RECV_WINDOW,
    CATEGORY, QUOTE, LEVERAGE, RISK_PCT, MARGIN_MODE,
    MAX_CONCURRENT_TRADES, MAX_TRADES_PER_DAY, TC_MAX_LAG_SEC,
    SIGNALS_PER_WINDOW, SIGNAL_WINDOW_MIN, EXCLUDED_SYMBOLS,
    STATE_FILE, DRY_RUN, LOG_LEVEL,
    SL_PCT, TP_SPLITS, TRAIL_AFTER_TP_INDEX, TRAIL_DISTANCE_PCT,
)
from bybit_v5 import BybitV5
from telegram_reader import TelegramReader
from signal_parser import parse_signal, signal_hash
from state import load_state, save_state, utc_day_key
from trade_engine import TradeEngine


def setup_logger() -> logging.Logger:
    log = logging.getLogger("bot")
    log.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%H:%M:%S")
    h.setFormatter(fmt)
    log.handlers[:] = [h]
    return log


# Global state (shared between threads)
state = {}
state_lock = threading.Lock()
log = None
engine = None
bybit = None


def trades_today() -> int:
    with state_lock:
        return int(state.get("daily_counts", {}).get(utc_day_key(), 0))


def inc_trades_today():
    with state_lock:
        k = utc_day_key()
        state.setdefault("daily_counts", {})[k] = int(state.get("daily_counts", {}).get(k, 0)) + 1


def active_trades_count() -> int:
    with state_lock:
        return len([
            tr for tr in state.get("open_trades", {}).values()
            if tr.get("status") in ("pending", "open")
        ])


def signals_in_window() -> int:
    """Count how many trades were placed in the last SIGNAL_WINDOW_MIN minutes."""
    with state_lock:
        cutoff = time.time() - (SIGNAL_WINDOW_MIN * 60)
        count = 0
        for tr in state.get("open_trades", {}).values():
            placed_ts = tr.get("placed_ts", 0)
            if placed_ts >= cutoff:
                count += 1
        return count


def on_telegram_message(msg_id: int, text: str, timestamp: float) -> None:
    """Handle incoming Telegram message - called by TelegramReader."""
    global state, engine, log

    # Skip old messages
    age = time.time() - timestamp
    if age > TC_MAX_LAG_SEC:
        log.debug(f"Skipping old message (age={age:.0f}s)")
        return

    # Parse signal
    sig = parse_signal(text, quote=QUOTE)
    if not sig:
        # Check if it looks like a signal but failed to parse
        if "Long" in text or "Short" in text:
            if "Entry" in text or "Name:" in text:
                log.debug(f"Possible signal NOT parsed: {text[:200]}...")
        return

    log.info(f"Signal received: {sig['symbol']} {sig['side'].upper()} @ {sig['trigger']}")

    # Check excluded symbols
    base_symbol = sig['symbol'].replace("USDT", "").replace("PERP", "")
    if base_symbol in EXCLUDED_SYMBOLS or sig['symbol'] in EXCLUDED_SYMBOLS:
        log.info(f"Symbol {sig['symbol']} is excluded - skipping")
        return

    # Check limits
    if active_trades_count() >= MAX_CONCURRENT_TRADES:
        log.info(f"Max concurrent trades reached ({MAX_CONCURRENT_TRADES}) - skipping")
        return

    if trades_today() >= MAX_TRADES_PER_DAY:
        log.info(f"Max daily trades reached ({MAX_TRADES_PER_DAY}) - skipping")
        return

    # Check batch limit (max X signals per time window)
    window_count = signals_in_window()
    if window_count >= SIGNALS_PER_WINDOW:
        log.info(f"Batch limit reached ({window_count}/{SIGNALS_PER_WINDOW} in {SIGNAL_WINDOW_MIN}min) - skipping")
        return

    # Check if already seen
    sh = signal_hash(sig)
    with state_lock:
        seen = set(state.get("seen_signal_hashes", []))
        if sh in seen:
            log.debug(f"Signal {sig['symbol']} already seen, skipping")
            return
        seen.add(sh)
        state["seen_signal_hashes"] = list(seen)[-500:]

    # Place entry order
    trade_id = f"{sig['symbol']}|{sig['side']}|{int(time.time())}"
    log.info(f"Placing entry order for {sig['symbol']}...")

    oid = engine.place_entry_order(sig, trade_id)
    if not oid:
        log.warning(f"Entry order failed for {sig['symbol']}")
        return

    # Store trade
    with state_lock:
        state.setdefault("open_trades", {})[trade_id] = {
            "id": trade_id,
            "symbol": sig["symbol"],
            "order_side": "Sell" if sig["side"] == "sell" else "Buy",
            "pos_side": "Short" if sig["side"] == "sell" else "Long",
            "trigger": float(sig["trigger"]),
            "tp_prices": sig.get("tp_prices") or [],
            "sl_price": None,
            "entry_order_id": oid,
            "status": "pending",
            "placed_ts": time.time(),
            "base_qty": engine.calc_base_qty(sig["symbol"], float(sig["trigger"])),
            "raw": sig.get("raw", ""),
        }

    inc_trades_today()
    log.info(f"ENTRY PLACED {sig['symbol']} {sig['side'].upper()} @ {sig['trigger']} (id={trade_id})")

    # Save state
    with state_lock:
        save_state(STATE_FILE, state)


def maintenance_loop():
    """Background thread for maintenance tasks."""
    global state, engine, log

    last_heartbeat = time.time()
    HEARTBEAT_INTERVAL = 300

    while True:
        try:
            # Heartbeat
            if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                active = active_trades_count()
                log.info(f"Heartbeat: {active} active trade(s), {trades_today()} today")
                last_heartbeat = time.time()

            # Maintenance tasks
            with state_lock:
                engine.cancel_expired_entries()
                engine.cleanup_closed_trades()
                engine.check_tp_fills_fallback()
                engine.check_position_alerts()
                engine.log_daily_stats()

                # Entry fill fallback and post-orders
                for tid, tr in list(state.get("open_trades", {}).items()):
                    if tr.get("status") == "pending":
                        sz, avg = engine.position_size_avg(tr["symbol"])
                        if sz > 0 and avg > 0:
                            tr["status"] = "open"
                            tr["entry_price"] = avg
                            tr["filled_ts"] = time.time()
                            log.info(f"ENTRY (poll) {tr['symbol']} @ {avg}")

                    if tr.get("status") == "open" and not tr.get("post_orders_placed"):
                        engine.place_post_entry_orders(tr)

                save_state(STATE_FILE, state)

        except Exception as e:
            log.exception(f"Maintenance error: {e}")

        time.sleep(10)


def ws_loop():
    """Background thread for Bybit WebSocket."""
    global engine, log

    def on_execution(ev):
        try:
            with state_lock:
                engine.on_execution(ev)
                save_state(STATE_FILE, state)
        except Exception as e:
            log.warning(f"WS execution handler error: {e}")

    def on_ws_error(err):
        log.debug(f"WS reconnecting: {err}")

    while True:
        try:
            bybit.run_private_ws(
                on_execution=on_execution,
                on_order=lambda x: None,
                on_error=on_ws_error,
            )
        except Exception as e:
            on_ws_error(e)
        time.sleep(3)


def main():
    global state, log, engine, bybit

    log = setup_logger()

    # Check required env vars
    missing = [k for k, v in {
        "TELEGRAM_API_ID": TELEGRAM_API_ID,
        "TELEGRAM_API_HASH": TELEGRAM_API_HASH,
        "TELEGRAM_CHANNEL": TELEGRAM_CHANNEL,
        "TELEGRAM_SESSION_STRING": TELEGRAM_SESSION_STRING,
        "BYBIT_API_KEY": BYBIT_API_KEY,
        "BYBIT_API_SECRET": BYBIT_API_SECRET,
    }.items() if not v]

    if missing:
        raise SystemExit(f"Missing ENV(s): {', '.join(missing)}")

    # Load state
    state = load_state(STATE_FILE)

    # Initialize Bybit
    bybit = BybitV5(
        BYBIT_API_KEY,
        BYBIT_API_SECRET,
        testnet=BYBIT_TESTNET,
        demo=BYBIT_DEMO,
        recv_window=RECV_WINDOW,
    )

    # Initialize trade engine
    engine = TradeEngine(bybit, state, log)

    # Initialize Telegram reader
    telegram = TelegramReader(
        api_id=TELEGRAM_API_ID,
        api_hash=TELEGRAM_API_HASH,
        channels=TELEGRAM_CHANNEL,
        session_string=TELEGRAM_SESSION_STRING,
    )

    log.info("=" * 58)
    mode_str = " | DRY_RUN" if DRY_RUN else ""
    mode_str += " | DEMO" if BYBIT_DEMO else ""
    mode_str += " | TESTNET" if BYBIT_TESTNET else ""
    log.info("Telegram -> Bybit Bot (Real-Time)" + mode_str)
    log.info("=" * 58)
    log.info(f"Config: CATEGORY={CATEGORY}, QUOTE={QUOTE}")
    log.info(f"Config: LEVERAGE={LEVERAGE}x (fixed), MARGIN={MARGIN_MODE}")
    log.info(f"Config: RISK_PCT={RISK_PCT}%, SL={SL_PCT}%")
    log.info(f"Config: TP_SPLITS={TP_SPLITS} ({sum(TP_SPLITS):.0f}% + {100-sum(TP_SPLITS):.0f}% runner)")
    log.info(f"Config: TRAIL after TP{TRAIL_AFTER_TP_INDEX}, dist={TRAIL_DISTANCE_PCT}%")
    log.info(f"Config: MAX_CONCURRENT={MAX_CONCURRENT_TRADES}, MAX_DAILY={MAX_TRADES_PER_DAY}, BATCH={SIGNALS_PER_WINDOW}/{SIGNAL_WINDOW_MIN}min")
    log.info(f"Config: DRY_RUN={DRY_RUN}, LOG_LEVEL={LOG_LEVEL}")
    if EXCLUDED_SYMBOLS:
        log.info(f"Config: EXCLUDED_SYMBOLS={EXCLUDED_SYMBOLS}")
    log.info(f"Listening to channel(s): {TELEGRAM_CHANNEL}")

    # Startup sync
    engine.startup_sync()

    # Start background threads
    ws_thread = threading.Thread(target=ws_loop, daemon=True)
    ws_thread.start()

    maint_thread = threading.Thread(target=maintenance_loop, daemon=True)
    maint_thread.start()

    # Set message handler and start Telegram
    telegram.set_message_handler(on_telegram_message)
    telegram.start()

    log.info("Bot is running. Waiting for signals...")

    # Run until disconnected (this blocks)
    try:
        telegram.run_forever()
    except KeyboardInterrupt:
        log.info("Bye")
        telegram.disconnect()


if __name__ == "__main__":
    main()
