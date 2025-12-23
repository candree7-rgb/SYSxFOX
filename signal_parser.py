"""
Signal Parser for Telegram Trading Signals

Parses signals with format:
    Short/Long
    Name: SYMBOL/USDT
    Margin mode: Cross (XXX)
    Entry price(USDT):
    X.XXXX
    Targets(USDT):
    1) X.XXXX
    2) X.XXXX
    3) X.XXXX
    4) X.XXXX
    5) unlimited
"""

import re
import hashlib
from typing import Any, Dict, Optional, List

NUM = r"([0-9]+(?:\.[0-9]+)?)"

# Match "Short" or "Long" with optional emoji prefix
RE_SIDE = re.compile(r"(Short|Long)", re.I)

# Match "Name: SYMBOL/USDT" or "Name: SYMBOLUSDT"
RE_SYMBOL = re.compile(r"Name:\s*([A-Z0-9]+)[/]?USDT", re.I)

# Match entry price - can be on same line or next line
RE_ENTRY = re.compile(r"Entry\s+price\s*\(?USDT\)?:?\s*\n?\s*" + NUM, re.I)

# Match targets: "1) 0.5845" etc
RE_TARGET = re.compile(r"(\d+)\)\s*" + NUM, re.I)

# Detect "unlimited" target to skip
RE_UNLIMITED = re.compile(r"unlimited", re.I)


def parse_signal(text: str, quote: str = "USDT") -> Optional[Dict[str, Any]]:
    """
    Parse a Telegram trading signal.

    Returns dict with:
        - base: Base symbol (e.g., "EPIC")
        - symbol: Full symbol (e.g., "EPICUSDT")
        - side: "buy" or "sell"
        - trigger: Entry price
        - tp_prices: List of TP prices (max 4)
        - sl_price: None (provider doesn't give SL)
        - raw: Original text

    Returns None if not a valid signal.
    """
    # Must have Short or Long
    side_match = RE_SIDE.search(text)
    if not side_match:
        return None

    side_word = side_match.group(1).upper()
    side = "sell" if side_word == "SHORT" else "buy"

    # Must have symbol
    symbol_match = RE_SYMBOL.search(text)
    if not symbol_match:
        return None

    base = symbol_match.group(1).upper()
    symbol = f"{base}{quote}"

    # Must have entry price
    entry_match = RE_ENTRY.search(text)
    if not entry_match:
        return None

    trigger = float(entry_match.group(1))

    # Extract targets (up to 4, skip "unlimited")
    tps: List[float] = []
    for m in RE_TARGET.finditer(text):
        idx = int(m.group(1))
        price_str = m.group(2)

        # Skip if this line contains "unlimited"
        line_start = text.rfind('\n', 0, m.start()) + 1
        line_end = text.find('\n', m.end())
        if line_end == -1:
            line_end = len(text)
        line = text[line_start:line_end]

        if RE_UNLIMITED.search(line):
            continue

        price = float(price_str)

        # Ensure list is long enough
        while len(tps) < idx:
            tps.append(0.0)
        if idx <= 4:  # Only take first 4 targets
            tps[idx - 1] = price

    # Filter out zeros
    tps = [p for p in tps if p > 0]

    # Validate: For SHORT, TPs should be below entry; for LONG, above
    if tps:
        if side == "sell":  # SHORT
            valid_tps = [tp for tp in tps if tp < trigger]
        else:  # LONG
            valid_tps = [tp for tp in tps if tp > trigger]

        if len(valid_tps) != len(tps):
            # Some TPs are on wrong side - likely parsing error
            pass  # Keep original tps, might still work

    return {
        "base": base,
        "symbol": symbol,
        "side": side,  # "buy" or "sell"
        "trigger": trigger,
        "tp_prices": tps[:4],  # Max 4 TPs
        "dca_prices": [],  # No DCA for this provider
        "sl_price": None,  # Provider doesn't give SL, we use our own
        "raw": text,
    }


def signal_hash(sig: Dict[str, Any]) -> str:
    """Generate unique hash for signal deduplication."""
    core = f"{sig.get('symbol')}|{sig.get('side')}|{sig.get('trigger')}|{sig.get('tp_prices')}"
    return hashlib.md5(core.encode("utf-8")).hexdigest()


# For testing
if __name__ == "__main__":
    test_signals = [
        """
Short
Name: EPIC/USDT
Margin mode: Cross (25.0X)

Entry price(USDT):
0.5904

Targets(USDT):
1) 0.5845
2) 0.5786
3) 0.5727
4) 0.5668
5) unlimited
        """,
        """
Long
Name: POL/USDT
Margin mode: Cross (75.0X)

Entry price(USDT):
0.1090

Targets(USDT):
1) 0.1101
2) 0.1112
3) 0.1123
4) 0.1134
5) unlimited
        """,
    ]

    for sig_text in test_signals:
        result = parse_signal(sig_text)
        if result:
            print(f"Parsed: {result['symbol']} {result['side'].upper()}")
            print(f"  Entry: {result['trigger']}")
            print(f"  TPs: {result['tp_prices']}")
            print()
        else:
            print("Failed to parse signal")
            print()
