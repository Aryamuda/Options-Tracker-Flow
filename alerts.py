import os
import time
import urllib.request
import urllib.parse
import json
from dataclasses import dataclass, field
from typing import Optional, Dict

from dotenv import load_dotenv


# Config
@dataclass
class AlertCooldown:
    """Tracks cooldown per alert type to avoid spam."""
    last_sent: Dict[str, float] = field(default_factory=dict)

    def can_send(self, alert_type: str, cooldown_secs: int = 300) -> bool:
        """Check if enough time has passed since last alert of this type."""
        now = time.time()
        last = self.last_sent.get(alert_type, 0)
        return (now - last) >= cooldown_secs

    def mark_sent(self, alert_type: str):
        """Record that we just sent this alert type."""
        self.last_sent[alert_type] = time.time()


# Alert cooldowns in seconds
COOLDOWNS = {
    "regime_change": 600,       # 10 min — regime doesn't flip often
    "wall_break": 300,          # 5 min
    "confluence_shift": 300,    # 5 min
    "uoa": 180,                 # 3 min
    "iv_spike": 300,            # 5 min
    "gex_flip_move": 300,       # 5 min
    "session_start": 0,         # No cooldown
    "session_end": 0,           # No cooldown
}


# Telegram Sender
class TelegramSender:
    """Sends messages to Telegram using urllib."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"

    def send(self, text: str, silent: bool = False) -> bool:
        """
        Send HTML-formatted message to Telegram.

        Uses HTML parse_mode. Messages are built with intentional HTML tags
        by the msg_* functions, so we send them as-is.
        """
        params = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': 'HTML',
            'disable_notification': silent,
        }

        url = f"{self.base_url}/sendMessage"
        data = urllib.parse.urlencode(params).encode('utf-8')

        try:
            req = urllib.request.Request(url, data=data, method='POST')
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read())
                return result.get('ok', False)
        except Exception as e:
            print(f"   ⚠ Telegram send failed: {e}")
            return False


# Alert Message Builders 
def _fmt_price(val) -> str:
    """Format price value safely."""
    if val is None:
        return "—"
    try:
        return f"${float(val):.2f}"
    except (ValueError, TypeError):
        return str(val)


def _fmt_score(score) -> str:
    """Format score with +/- sign."""
    try:
        return f"{int(score):+d}"
    except (ValueError, TypeError):
        return str(score)


def msg_regime_change(old_regime: str, new_regime: str, spot: float) -> str:
    """Alert: gamma regime changed."""
    if new_regime == "positive_gamma":
        emoji = "🟢"
        regime_text = "POSITIVE GAMMA"
        action = "Mean-reversion mode — FADE extremes"
    elif new_regime == "negative_gamma":
        emoji = "🔴"
        regime_text = "NEGATIVE GAMMA"
        action = "Trend mode — RIDE momentum"
    else:
        emoji = "⚪"
        regime_text = "UNKNOWN"
        action = "Insufficient data"

    return (
        f"{emoji} <b>REGIME CHANGE</b>\n\n"
        f"QQQ {_fmt_price(spot)}\n"
        f"<code>{old_regime} → {new_regime}</code>\n\n"
        f"<b>{regime_text}</b>\n"
        f"{action}"
    )


def msg_wall_break(wall_type: str, wall_strike: float, spot: float, direction: str) -> str:
    """Alert: price broke through an OI wall."""
    if wall_type == "call":
        emoji = "🚀"
        label = "CALL WALL BREAK"
    else:
        emoji = "💥"
        label = "PUT WALL BREAK"

    return (
        f"{emoji} <b>{label}</b>\n\n"
        f"QQQ {_fmt_price(spot)} broke {direction} "
        f"{wall_type} wall at {_fmt_price(wall_strike)}\n\n"
        f"Expect acceleration in {direction} direction"
    )


def msg_confluence_shift(old_bias: str, new_bias: str, score: int, spot: float, summary: str) -> str:
    """Alert: confluence bias changed significantly."""
    bias_emoji = {"LONG": "🟢", "SHORT": "🔴", "SLIGHT LONG": "🟢",
                  "SLIGHT SHORT": "🔴", "NEUTRAL": "⚪"}.get(new_bias, "⚪")

    return (
        f"{bias_emoji} <b>CONFLUENCE SHIFT</b>\n\n"
        f"QQQ {_fmt_price(spot)}\n"
        f"<code>{old_bias} → {new_bias} ({_fmt_score(score)})</code>\n\n"
        f"{summary}"
    )


def msg_uoa(flags: list, spot: float) -> str:
    """Alert: unusual options activity detected."""
    lines = [f"⚠️ <b>UNUSUAL ACTIVITY</b>\n\nQQQ {_fmt_price(spot)}\n"]

    for flag in flags[:5]:
        emoji = "📈" if flag.opt_type == "CALL" else "📉"
        lines.append(
            f"{emoji} {_fmt_price(flag.strike)} {flag.opt_type} "
            f"— Vol: {flag.volume:,} | OI: {flag.oi:,} | "
            f"<b>V/OI: {flag.voi_ratio:.1f}x</b>"
        )

    return "\n".join(lines)


def msg_iv_spike(atm_iv: float, prev_iv: float, spot: float) -> str:
    """Alert: ATM IV spiked."""
    change = atm_iv - prev_iv
    direction = "📈 SPIKED" if change > 0 else "📉 DROPPED"

    return (
        f"🌪 <b>IV {direction}</b>\n\n"
        f"QQQ {_fmt_price(spot)}\n"
        f"ATM IV: {prev_iv:.1f}% → <b>{atm_iv:.1f}%</b> ({change:+.1f}%)\n\n"
        f"{'Widen stops, expect big move' if change > 0 else 'Tighter range expected'}"
    )


def msg_gex_flip_move(old_flip: float, new_flip: float, spot: float) -> str:
    """Alert: GEX flip point moved significantly."""
    direction = "⬆️ UP" if new_flip > old_flip else "⬇️ DOWN"
    distance = abs(new_flip - old_flip)

    return (
        f"🟡 <b>GEX FLIP MOVED {direction}</b>\n\n"
        f"QQQ {_fmt_price(spot)}\n"
        f"GEX Flip: {_fmt_price(old_flip)} → <b>{_fmt_price(new_flip)}</b> "
        f"(moved ${distance:.1f})"
    )


def msg_session_summary(signals, poll_count: int) -> str:
    """End-of-session summary."""
    spot = _fmt_price(signals.spot) if signals else "—"

    parts = [
        f"🏁 <b>SESSION COMPLETE</b>\n",
        f"QQQ Close: {spot}",
        f"Total Polls: {poll_count}",
    ]

    if signals and signals.confluence:
        c = signals.confluence
        bias_emoji = {"LONG": "🟢", "SHORT": "🔴"}.get(c.net_bias, "⚪")
        parts.append(f"\nFinal Bias: {bias_emoji} {c.net_bias} ({_fmt_score(c.net_score)})")
        parts.append(f"Confidence: {c.confidence.upper()} ({c.active_signals}/{c.total_signals})")

    if signals and signals.gex:
        parts.append(f"\nRegime: {signals.gex.regime}")
        if signals.gex.flip_point:
            parts.append(f"GEX Flip: {_fmt_price(signals.gex.flip_point)}")

    if signals and signals.oi_walls:
        if signals.oi_walls.call_wall:
            parts.append(f"Call Wall: {_fmt_price(signals.oi_walls.call_wall)}")
        if signals.oi_walls.put_wall:
            parts.append(f"Put Wall: {_fmt_price(signals.oi_walls.put_wall)}")

    return "\n".join(parts)


def msg_session_start(spot: float, regime: str, bias: str, score: int) -> str:
    """Session starting alert with initial state."""
    regime_emoji = "🟢" if regime == "positive_gamma" else ("🔴" if regime == "negative_gamma" else "⚪")
    bias_emoji = {"LONG": "🟢", "SHORT": "🔴"}.get(bias, "⚪")

    return (
        f"🔔 <b>SESSION STARTED</b>\n\n"
        f"QQQ {_fmt_price(spot)}\n"
        f"Regime: {regime_emoji} {regime}\n"
        f"Bias: {bias_emoji} {bias} ({_fmt_score(score)})"
    )


def msg_trade_suggestion(trade, spot: float) -> str:
    """Alert: new trade suggestion generated."""
    if trade.bias == "LONG":
        emoji = "🟢"
    elif trade.bias == "SHORT":
        emoji = "🔴"
    else:
        emoji = "⚪"

    return (
        f"{emoji} <b>TRADE: {trade.bias}</b>\n\n"
        f"QQQ {_fmt_price(spot)}\n"
        f"Entry: {_fmt_price(trade.entry)}\n"
        f"Stop: {_fmt_price(trade.stop)}\n"
        f"Target: {_fmt_price(trade.target)}\n\n"
        f"<i>{trade.reason}</i>"
    )


# Alert Manager
class AlertManager:
    """
    Manages alert logic: compares previous vs current signals,
    fires alerts when thresholds are crossed, respects cooldowns.
    """

    def __init__(self, token: str = None, chat_id: str = None):
        """
        Initialize AlertManager with explicit credentials.
        
        Args:
            token: Telegram bot token (or None to read from TELEGRAM_TOKEN env var)
            chat_id: Telegram chat ID (or None to read from TELEGRAM_CHAT_ID env var)
        """
        # Allow passing credentials directly or falling back to env vars
        # This makes it testable and avoids side effects in constructor
        if token is None:
            token = os.environ.get('TELEGRAM_TOKEN', '')
        if chat_id is None:
            chat_id = os.environ.get('TELEGRAM_CHAT_ID', '')

        if token and chat_id:
            self.sender = TelegramSender(token, chat_id)
            self.enabled = True
            print("   📱 Telegram alerts: ENABLED")
        else:
            self.sender = None
            self.enabled = False
            print("   📱 Telegram alerts: DISABLED (missing TELEGRAM_TOKEN / TELEGRAM_CHAT_ID)")

        self.cooldown = AlertCooldown()

    @classmethod
    def from_env(cls, env_path: str = None):
        """Factory method to create AlertManager from .env file."""
        from pathlib import Path
        if env_path is None:
            env_path = Path(__file__).parent / '.env'
        load_dotenv(env_path)
        return cls()

    def _send(self, alert_type: str, text: str, silent: bool = False) -> bool:
        """Send alert if enabled and cooldown allows."""
        if not self.enabled or not self.sender:
            return False

        cooldown_secs = COOLDOWNS.get(alert_type, 300)
        if not self.cooldown.can_send(alert_type, cooldown_secs):
            return False

        success = self.sender.send(text, silent=silent)
        if success:
            self.cooldown.mark_sent(alert_type)
            print(f"   📱 Alert sent: {alert_type}")
        return success

    def send_session_start(self, signals) -> bool:
        """Send session start alert."""
        if not signals or not signals.gex:
            return False

        regime = signals.gex.regime if signals.gex else "unknown"
        bias = signals.confluence.net_bias if signals.confluence else "NEUTRAL"
        score = signals.confluence.net_score if signals.confluence else 0

        text = msg_session_start(signals.spot, regime, bias, score)
        return self._send("session_start", text)

    def send_session_end(self, signals, poll_count: int) -> bool:
        """Send session end summary."""
        text = msg_session_summary(signals, poll_count)
        return self._send("session_end", text)

    def check_and_alert(self, prev, curr) -> list:
        """
        Compare previous and current SignalSnapshots, fire relevant alerts.

        Args:
            prev: Previous SignalSnapshot (can be None for first poll)
            curr: Current SignalSnapshot

        Returns:
            List of alert types that were sent
        """
        if not curr or not self.enabled:
            return []

        sent = []

        # ─── 1. Regime Change
        if prev and prev.gex and curr.gex:
            if prev.gex.regime != curr.gex.regime and curr.gex.regime != "unknown":
                text = msg_regime_change(prev.gex.regime, curr.gex.regime, curr.spot)
                if self._send("regime_change", text):
                    sent.append("regime_change")

        # ─── 2. OI Wall Break
        if prev and prev.oi_walls and curr.oi_walls:
            # Put wall break (bearish acceleration)
            if (prev.oi_walls.put_wall and curr.oi_walls.put_wall
                    and prev.spot >= prev.oi_walls.put_wall
                    and curr.spot < curr.oi_walls.put_wall):
                text = msg_wall_break("put", curr.oi_walls.put_wall, curr.spot, "below")
                if self._send("wall_break", text):
                    sent.append("wall_break_put")

            # Call wall break (bullish acceleration)
            if (prev.oi_walls.call_wall and curr.oi_walls.call_wall
                    and prev.spot <= prev.oi_walls.call_wall
                    and curr.spot > curr.oi_walls.call_wall):
                text = msg_wall_break("call", curr.oi_walls.call_wall, curr.spot, "above")
                if self._send("wall_break", text):
                    sent.append("wall_break_call")

        # ─── 3. Confluence Shift
        if prev and prev.confluence and curr.confluence:
            old_bias = prev.confluence.net_bias
            new_bias = curr.confluence.net_bias
            if old_bias != new_bias:
                text = msg_confluence_shift(
                    old_bias, new_bias,
                    curr.confluence.net_score,
                    curr.spot,
                    curr.confluence.summary,
                )
                if self._send("confluence_shift", text):
                    sent.append("confluence_shift")

        # ─── 4. UOA
        if curr.uoa_flags:
            # Only alert for new UOA (with V/OI > 3x for high-conviction)
            strong_uoa = [f for f in curr.uoa_flags if f.voi_ratio >= 3.0]
            if strong_uoa:
                text = msg_uoa(strong_uoa, curr.spot)
                if self._send("uoa", text):
                    sent.append("uoa")

        # ─── 5. IV Spike
        if prev and prev.iv and curr.iv:
            if prev.iv.atm_iv and curr.iv.atm_iv:
                iv_change = abs(curr.iv.atm_iv - prev.iv.atm_iv)
                if iv_change >= 2.0:  # 2% absolute change
                    text = msg_iv_spike(curr.iv.atm_iv, prev.iv.atm_iv, curr.spot)
                    if self._send("iv_spike", text):
                        sent.append("iv_spike")

        # ─── 6. GEX Flip Moved (INFO) ────────────────────────────
        if prev and prev.gex and curr.gex:
            if prev.gex.flip_point and curr.gex.flip_point:
                flip_move = abs(curr.gex.flip_point - prev.gex.flip_point)
                if flip_move >= 1.0:  # Moved $1+
                    text = msg_gex_flip_move(
                        prev.gex.flip_point, curr.gex.flip_point, curr.spot
                    )
                    if self._send("gex_flip_move", text, silent=True):
                        sent.append("gex_flip_move")

        return sent


# Quick Test
if __name__ == "__main__":
    """Quick test: sends a test message to verify Telegram setup."""
    load_dotenv()

    token = os.environ.get('TELEGRAM_TOKEN', '')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID', '')

    if not token or not chat_id:
        print("✗ Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID in .env")
        exit(1)

    sender = TelegramSender(token, chat_id)

    test_msg = (
        "🧪 <b>QQQ Options Alert Test</b>\n\n"
        "If you see this, alerts are working!\n\n"
        "<code>Regime: positive_gamma</code>\n"
        "Spot: $612.50\n"
        "GEX Flip: $610.25\n\n"
        "<i>This is a test — no action needed</i>"
    )

    print("📱 Sending test message...")
    success = sender.send(test_msg)
    if success:
        print("✓ Test message sent! Check Telegram.")
    else:
        print("✗ Failed to send test message.")
