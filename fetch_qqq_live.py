import asyncio
import os
import sys
import signal
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from tastytrade import Session, DXLinkStreamer
from tastytrade.instruments import get_option_chain
from tastytrade.market_data import get_market_data_by_type
from tastytrade.dxfeed import Greeks

from alerts import AlertManager
from signals import compute_all_signals

# Config
SYMBOL = 'QQQ'
ATM_RANGE_PCT = 0.05        # ±5% from spot
BATCH_SIZE = 30             # REST API batch size (avoid 414)
GREEKS_BATCH_SIZE = 50      # WebSocket subscription batch
GREEKS_TIMEOUT = 10         # Max seconds to wait for all Greeks
DATA_DIR = Path(__file__).parent / 'data'

# Globals
running = True


def signal_handler(sig, frame):
    global running
    print("\n\n🛑 Stopping... (Ctrl+C)")
    running = False
    # Flush any pending parquet data before exiting
    flush_all_buffers()

signal.signal(signal.SIGINT, signal_handler)


# Time Helpers 

def get_ny_time():
    """Get current NY time."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo("America/New_York")
        return datetime.now(tz)
    except Exception:
        return datetime.now()


def is_market_open(now=None):
    """Check if US market is open (9:30 AM - 4:00 PM ET, Mon-Fri)."""
    if now is None:
        now = get_ny_time()

    if now.weekday() >= 5:
        return False, "Weekend"

    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

    if now < market_open:
        return False, f"Pre-market (opens {market_open.strftime('%H:%M')} ET)"
    elif now > market_close:
        return False, "After-hours"
    else:
        return True, "Market Open"


def is_near_close(now=None, minutes_before=60):
    """Check if we're within N minutes of market close."""
    if now is None:
        now = get_ny_time()
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return 0 < (market_close - now).total_seconds() <= minutes_before * 60


def seconds_until_market_open(now=None):
    """Calculate seconds until next market open. Handles weekends."""
    if now is None:
        now = get_ny_time()

    # Next market open
    target = now.replace(hour=9, minute=30, second=0, microsecond=0)

    if now >= target:
        # Already past today's open, aim for tomorrow
        target += timedelta(days=1)

    # Skip weekends
    while target.weekday() >= 5:  # Sat=5, Sun=6
        target += timedelta(days=1)

    return (target - now).total_seconds()


# Data Helpers 

def get_data_dir(date):
    """Get/create data directory for a given date."""
    day_dir = DATA_DIR / date.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)
    return day_dir


# Buffer for batching parquet writes - avoids reading/writing entire file every 60s
# This is a global buffer: {filepath: [df1, df2, ...]}
_parquet_buffer = {}
PARQUET_BATCH_SIZE = 5  # Write after N snapshots accumulate


def append_to_parquet(df, filepath, batch_size: int = PARQUET_BATCH_SIZE):
    """
    Buffer dataframe for batched parquet writes.
    
    Instead of reading the entire file every poll (which gets slower over time),
    we buffer in memory and only write after batch_size snapshots accumulate.
    This reduces I/O from O(n) per poll to O(1) per poll.
    """
    filepath = Path(filepath)
    
    # Initialize buffer for this filepath if needed
    if filepath not in _parquet_buffer:
        _parquet_buffer[filepath] = []
    
    _parquet_buffer[filepath].append(df)
    
    # If we've accumulated enough, write all buffered data
    if len(_parquet_buffer[filepath]) >= batch_size:
        _flush_parquet_buffer(filepath)
        return True  # Indicates a flush happened
    
    return False  # Still buffering


def _flush_parquet_buffer(filepath: Path):
    """Flush buffered dataframes to parquet file."""
    if filepath not in _parquet_buffer or not _parquet_buffer[filepath]:
        return
    
    buffered = _parquet_buffer[filepath]
    
    if filepath.exists():
        # Read existing and append all buffered
        existing = pd.read_parquet(filepath)
        combined = pd.concat([existing] + buffered, ignore_index=True)
    else:
        combined = pd.concat(buffered, ignore_index=True)
    
    combined.to_parquet(filepath, index=False)
    
    # Clear buffer after successful write
    _parquet_buffer[filepath] = []
    print(f"   💾 Flushed {len(buffered)} snapshots to {filepath.name}")


def flush_all_buffers():
    """Flush all pending parquet buffers - call on shutdown."""
    for filepath in list(_parquet_buffer.keys()):
        _flush_parquet_buffer(filepath)


def filter_atm_range(options, spot_price, pct=ATM_RANGE_PCT):
    """Filter options to ATM ±pct range based on spot price."""
    lower = spot_price * (1 - pct)
    upper = spot_price * (1 + pct)

    filtered = []
    for opt in options:
        strike = float(opt.strike_price)
        if lower <= strike <= upper:
            filtered.append(opt)

    return filtered


def parse_option_symbol(symbol):
    """Parse OCC option symbol into (type, strike)."""
    clean = symbol.replace(' ', '')
    if 'C' in clean:
        parts = clean.split('C')
        return 'CALL', float(parts[1]) / 1000.0
    elif 'P' in clean:
        parts = clean.split('P')
        return 'PUT', float(parts[1]) / 1000.0
    return None, None


# Core Fetch Logic

async def get_underlying_price(session, symbol=SYMBOL):
    """Fetch current underlying price."""
    try:
        data = await get_market_data_by_type(session, equities=[symbol])
        if data:
            return float(data[0].last)
    except Exception:
        pass
    return None


async def fetch_quotes(session, symbols):
    """Fetch quotes in batches. Returns dict of symbol -> market_data."""
    quotes = {}
    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i + BATCH_SIZE]
        try:
            batch_data = await get_market_data_by_type(session, options=batch)
            for md in batch_data:
                quotes[md.symbol] = md
        except Exception:
            pass
    return quotes


async def fetch_greeks(streamer, streamer_symbols):
    """Subscribe and collect Greeks via DXLink. Returns dict of event_symbol -> greek."""
    if not streamer_symbols:
        return {}

    greeks = {}
    # Dedupe symbols to avoid infinite loop - if same symbol appears twice,
    # len(greeks) < len(streamer_symbols) will never be true
    unique_symbols = list(set(streamer_symbols))
    expected_count = len(unique_symbols)

    try:
        print(f"   📈 Subscribing to {len(streamer_symbols)} Greeks ({expected_count} unique)...")
        for i in range(0, len(streamer_symbols), GREEKS_BATCH_SIZE):
            batch = streamer_symbols[i:i + GREEKS_BATCH_SIZE]
            await streamer.subscribe(Greeks, batch)

        await asyncio.sleep(0.5)

        start = time.time()
        while len(greeks) < expected_count:
            elapsed = time.time() - start
            if elapsed > GREEKS_TIMEOUT:
                print(f"   ⚠ Greeks timeout after {elapsed:.1f}s ({len(greeks)}/{expected_count})")
                break
            try:
                greek = await asyncio.wait_for(
                    streamer.get_event(Greeks), timeout=0.5
                )
                greeks[greek.event_symbol] = greek
            except asyncio.TimeoutError:
                # If we got at least some greeks and timed out, that's probably fine
                # Market might be closed or data unavailable
                if len(greeks) > 0:
                    break
                # If we got nothing for 3+ seconds, keep trying (might be connection delay)
                if elapsed < 3:
                    continue
                break

        print(f"   ✓ Received {len(greeks)}/{expected_count} Greeks")
    except Exception as e:
        print(f"   ⚠ Greeks error: {e}")

    return greeks


def build_dataframe(options, quotes, greeks, streamer_map, spot_price, now, expiration_date, dte):
    """Build a pandas DataFrame from fetched data."""
    rows = []

    for opt in options:
        symbol = opt.symbol
        q = quotes.get(symbol)
        if not q:
            continue

        opt_type, strike = parse_option_symbol(symbol)
        if not opt_type:
            continue

        # Match Greek
        greek = None
        streamer_sym = getattr(opt, 'streamer_symbol', None)
        if streamer_sym:
            greek = greeks.get(streamer_sym)

        bid = getattr(q, 'bid', None)
        ask = getattr(q, 'ask', None)
        mid = None
        spread = None
        if bid is not None and ask is not None:
            try:
                bid_f, ask_f = float(bid), float(ask)
                mid = (bid_f + ask_f) / 2
                spread = ask_f - bid_f
            except (ValueError, TypeError):
                pass

        rows.append({
            'timestamp': now.strftime('%Y-%m-%d %H:%M:%S'),
            'strike': strike,
            'type': opt_type,
            'symbol': symbol,
            'bid': float(bid) if bid is not None else None,
            'ask': float(ask) if ask is not None else None,
            'last': float(getattr(q, 'last', None)) if getattr(q, 'last', None) is not None else None,
            'mid': round(mid, 4) if mid is not None else None,
            'spread': round(spread, 4) if spread is not None else None,
            'iv': round(greek.volatility * 100, 2) if greek and getattr(greek, 'volatility', None) else None,
            'volume': getattr(q, 'volume', None),
            'open_interest': getattr(q, 'open_interest', None),
            'delta': round(greek.delta, 6) if greek and getattr(greek, 'delta', None) is not None else None,
            'gamma': round(greek.gamma, 6) if greek and getattr(greek, 'gamma', None) is not None else None,
            'theta': round(greek.theta, 6) if greek and getattr(greek, 'theta', None) is not None else None,
            'vega': round(greek.vega, 6) if greek and getattr(greek, 'vega', None) is not None else None,
            'rho': round(greek.rho, 6) if greek and getattr(greek, 'rho', None) is not None else None,
            'underlying': spot_price,
            'expiration': expiration_date.isoformat(),
            'dte': dte,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(['strike', 'type']).reset_index(drop=True)

    return df


async def fetch_snapshot(session, fetch_1dte=False):
    """
    Fetch a single snapshot.

    Args:
        session: Tastytrade session
        fetch_1dte: If True, also fetch 1DTE data (typically near close)
    """
    now = get_ny_time()
    today = now.date()
    tomorrow = today + timedelta(days=1)

    is_open, status = is_market_open(now)
    print(f"\n⏰ {now.strftime('%Y-%m-%d %H:%M:%S')} ET — {status}")

    if not is_open:
        print("   ⚠ Market closed — Greeks may be unavailable")

    # Fetch option chain
    try:
        chain = await get_option_chain(session, SYMBOL)
    except Exception as e:
        print(f"   ✗ Failed to fetch chain: {e}")
        return None

    # Get underlying price
    spot = await get_underlying_price(session)
    if spot:
        print(f"   💰 {SYMBOL}: ${spot:.2f}")
    else:
        print("   ⚠ Could not fetch underlying price")
        return None

    # Determine which expirations to fetch
    targets = [(today, 0)]
    if fetch_1dte:
        targets.append((tomorrow, 1))

    results = {}
    day_dir = get_data_dir(today)

    try:
        async with DXLinkStreamer(session) as streamer:
            for target_date, dte in targets:
                if target_date not in chain:
                    print(f"   ⚠ No {dte}DTE chain found for {target_date}")
                    continue

                # Filter to ATM ±5%
                all_options = chain[target_date]
                options = filter_atm_range(all_options, spot)
                print(f"   🎯 {dte}DTE: {len(options)}/{len(all_options)} options (ATM ±{ATM_RANGE_PCT*100:.0f}%)")

                if not options:
                    continue

                # Build mappings
                symbols = [opt.symbol for opt in options]
                streamer_symbols = [
                    opt.streamer_symbol for opt in options
                    if hasattr(opt, 'streamer_symbol') and opt.streamer_symbol
                ]
                streamer_map = {
                    opt.streamer_symbol: opt.symbol
                    for opt in options
                    if hasattr(opt, 'streamer_symbol') and opt.streamer_symbol
                }

                # Fetch quotes + Greeks
                quotes = await fetch_quotes(session, symbols)
                greeks = await fetch_greeks(streamer, streamer_symbols)

                # Build dataframe
                df = build_dataframe(
                    options, quotes, greeks, streamer_map,
                    spot, now, target_date, dte
                )

                if df.empty:
                    print(f"   ⚠ No data for {dte}DTE")
                    continue

                # Save to parquet
                parquet_file = day_dir / f"{dte}dte.parquet"
                append_to_parquet(df, parquet_file)

                greeks_count = df['delta'].notna().sum()
                print(f"   ✓ {dte}DTE: {len(df)} options, {greeks_count} Greeks → {parquet_file}")

                results[dte] = {
                    'file': str(parquet_file),
                    'options': len(df),
                    'greeks': greeks_count,
                    'df': df,
                }

    except Exception as e:
        print(f"   ✗ Streamer error: {e}")
        import traceback
        traceback.print_exc()

    return results


# Main 

async def run_daemon(session, poll_interval=60, alert_manager=None):
    """
    Daemon mode: fully time-aware, runs forever on VPS.

    Lifecycle:
      1. Sleep until market open (handles weekends)
      2. Poll 0DTE every poll_interval seconds
      3. Auto-include 1DTE in the last 60 min before close
      4. After close → sleep until next market open
      5. Repeat forever
    """
    prev_signals = None
    poll_count = 0

    while running:
        now = get_ny_time()
        is_open, status = is_market_open(now)

        if not is_open:
            if prev_signals is not None and alert_manager and alert_manager.enabled:
                alert_manager.send_session_end(prev_signals, poll_count)
            prev_signals = None
            poll_count = 0

            # Calculate sleep time until next open
            wait_secs = seconds_until_market_open(now)
            hours = int(wait_secs // 3600)
            mins = int((wait_secs % 3600) // 60)

            print(f"\n😴 Market closed ({status})")
            print(f"   Next open in {hours}h {mins}m")
            print(f"   Sleeping until market open...")

            # Sleep in 60s chunks so we can respond to Ctrl+C
            slept = 0
            while slept < wait_secs and running:
                chunk = min(60, wait_secs - slept)
                await asyncio.sleep(chunk)
                slept += chunk

            if not running:
                break

            # Re-create session (token might have expired during sleep)
            print("\n🔐 Refreshing session...")
            try:
                env_path = Path(__file__).parent / '.env'
                load_dotenv(env_path, override=True)
                client_secret = os.environ.get('SECRET')
                refresh_token = os.environ.get('refresh')
                session = Session(client_secret, refresh_token)
                print("   ✓ Reconnected!")
            except Exception as e:
                print(f"   ✗ Reconnect failed: {e}")
                print("   Retrying in 60s...")
                await asyncio.sleep(60)
                continue

            continue

        # Market is open: poll
        # Auto-include 1DTE in the last 60 min
        should_1dte = is_near_close(now, minutes_before=60)
        if should_1dte:
            print("   📋 Last hour — including 1DTE for tomorrow")

        results = await fetch_snapshot(session, fetch_1dte=should_1dte)

        # --- ALERTS ---
        if results and 0 in results and alert_manager and alert_manager.enabled:
            df_0dte = results[0]['df']
            curr_signals = compute_all_signals(df_0dte)
            
            if poll_count == 0:
                alert_manager.send_session_start(curr_signals)
            else:
                alert_manager.check_and_alert(prev_signals, curr_signals)
                
            prev_signals = curr_signals

        poll_count += 1

        # Check if market just closed during our fetch
        now_after = get_ny_time()
        is_still_open, _ = is_market_open(now_after)
        if not is_still_open:
            print("\n🏁 Market closed! Session complete.")
            continue  # Will loop back and sleep until next open

        # Wait for next poll
        if running:
            print(f"   ⏳ Next poll in {poll_interval}s...")
            for _ in range(poll_interval):
                if not running:
                    break
                await asyncio.sleep(1)


async def main():
    env_path = Path(__file__).parent / '.env'
    load_dotenv(env_path)

    client_secret = os.environ.get('SECRET')
    refresh_token = os.environ.get('refresh')

    if not client_secret or not refresh_token:
        print("✗ Missing credentials in .env (need SECRET and refresh)")
        return

    # Parse args
    loop_interval = None
    duration_minutes = None
    daemon_mode = '--daemon' in sys.argv

    if '--loop' in sys.argv:
        idx = sys.argv.index('--loop')
        if idx + 1 < len(sys.argv):
            loop_interval = int(sys.argv[idx + 1])

    if '--duration' in sys.argv:
        idx = sys.argv.index('--duration')
        if idx + 1 < len(sys.argv):
            duration_minutes = int(sys.argv[idx + 1])

    fetch_1dte_flag = '--1dte' in sys.argv

    print("🚀 QQQ Options Fetcher")
    print("=" * 50)
    print(f"   Filter: ATM ±{ATM_RANGE_PCT*100:.0f}%")
    print(f"   Output: {DATA_DIR}/")

    # Initialize AlertManager - use factory method to load env
    alert_manager = AlertManager.from_env()

    # Create session
    print("\n🔐 Creating session...")
    try:
        session = Session(client_secret, refresh_token)
        print("   ✓ Connected!")
    except Exception as e:
        print(f"   ✗ Failed: {e}")
        return

    # ─── Daemon mode ──────────────────────────────────────────────
    if daemon_mode:
        poll_interval = loop_interval or 60
        print(f"\n🤖 DAEMON MODE — polling every {poll_interval}s")
        print("   Fully time-aware: sleeps when market closed, auto 1DTE near close")
        print("   Ctrl+C to stop\n")
        await run_daemon(session, poll_interval=poll_interval, alert_manager=alert_manager)

    # Loop mode 
    elif loop_interval:
        print(f"\n🔄 Polling every {loop_interval}s", end="")
        if duration_minutes:
            print(f" for {duration_minutes} min")
        else:
            print(" (Ctrl+C to stop)")

        start_time = asyncio.get_event_loop().time()
        poll_count = 0
        prev_signals = None

        while running:
            if duration_minutes:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= duration_minutes * 60:
                    print(f"\n⏱ Duration limit reached ({duration_minutes} min)")
                    break

            auto_1dte = is_near_close(minutes_before=60)
            should_fetch_1dte = fetch_1dte_flag or auto_1dte

            if auto_1dte and poll_count == 0:
                print("   📋 Near close — also fetching 1DTE for tomorrow")

            results = await fetch_snapshot(session, fetch_1dte=should_fetch_1dte)

            # --- ALERTS ---
            if results and 0 in results and alert_manager.enabled:
                df_0dte = results[0]['df']
                curr_signals = compute_all_signals(df_0dte)
                
                if poll_count == 0:
                    alert_manager.send_session_start(curr_signals)
                else:
                    alert_manager.check_and_alert(prev_signals, curr_signals)
                    
                prev_signals = curr_signals

            poll_count += 1

            if running:
                remaining = ""
                if duration_minutes:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    mins_left = duration_minutes - (elapsed / 60)
                    remaining = f" ({mins_left:.1f} min remaining)"

                print(f"\n   ⏳ Next fetch in {loop_interval}s...{remaining}")
                for _ in range(loop_interval):
                    if not running:
                        break
                    if duration_minutes:
                        elapsed = asyncio.get_event_loop().time() - start_time
                        if elapsed >= duration_minutes * 60:
                            break
                    await asyncio.sleep(1)

        print(f"\n📊 Total polls: {poll_count}")
        if prev_signals is not None and alert_manager.enabled:
            alert_manager.send_session_end(prev_signals, poll_count)

    # Single snapshot 
    else:
        print("\n📸 Single snapshot mode")
        await fetch_snapshot(session, fetch_1dte=fetch_1dte_flag)

    print("\n✅ Done!")


if __name__ == "__main__":
    asyncio.run(main())
