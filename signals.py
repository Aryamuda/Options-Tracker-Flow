import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Optional


# Data Structures 

@dataclass
class GEXData:
    """Gamma Exposure data."""
    gex_by_strike: dict  # {strike: gex_value}
    flip_point: Optional[float] = None
    total_gex_dollar: float = 0.0  # GEX$ in dollars
    positive_gex_peak: Optional[float] = None
    negative_gex_peak: Optional[float] = None
    regime: str = "unknown"  # "positive_gamma" or "negative_gamma"


@dataclass
class OIWalls:
    """Open Interest wall data."""
    call_wall: Optional[float] = None
    call_wall_oi: int = 0
    put_wall: Optional[float] = None
    put_wall_oi: int = 0
    total_wall: Optional[float] = None  # highest combined OI


@dataclass
class IVMetrics:
    """Implied Volatility metrics."""
    atm_iv: Optional[float] = None
    iv_skew: Optional[float] = None  # 25Δ put IV - 25Δ call IV
    put_25d_iv: Optional[float] = None
    call_25d_iv: Optional[float] = None
    iv_by_strike: dict = field(default_factory=dict)


@dataclass
class PCRatios:
    """Put/Call ratios."""
    volume_pc: Optional[float] = None
    oi_pc: Optional[float] = None
    total_call_volume: int = 0
    total_put_volume: int = 0
    total_call_oi: int = 0
    total_put_oi: int = 0


@dataclass
class UOAFlag:
    """Unusual Options Activity flag."""
    strike: float
    opt_type: str  # "CALL" or "PUT"
    volume: int
    oi: int
    voi_ratio: float


@dataclass
class FlowDirection:
    """Charm/Vanna direction."""
    charm_bias: str = "neutral"  # "buy", "sell", "neutral"
    charm_strength: float = 0.0
    vanna_bias: str = "neutral"
    vanna_strength: float = 0.0


@dataclass
class TradeSuggestion:
    """Auto-generated trade suggestion."""
    bias: str  # "LONG", "SHORT", "NEUTRAL"
    entry: Optional[float] = None
    stop: Optional[float] = None
    target: Optional[float] = None
    reason: str = ""


@dataclass
class ConfluenceScore:
    """Confluence scoring result."""
    long_score: int = 0
    short_score: int = 0
    max_score: int = 16
    net_bias: str = "NEUTRAL"
    net_score: int = 0
    confidence: str = "low"  # "low", "medium", "high"
    active_signals: int = 0
    total_signals: int = 10
    breakdown: dict = field(default_factory=dict)  # {signal_name: (side, weight)}
    summary: str = ""


@dataclass
class SignalSnapshot:
    """All signals for a single timestamp."""
    timestamp: str = ""
    spot: float = 0.0
    dte: int = 0
    gex: Optional[GEXData] = None
    oi_walls: Optional[OIWalls] = None
    max_pain: Optional[float] = None
    iv: Optional[IVMetrics] = None
    pc_ratios: Optional[PCRatios] = None
    delta_weighted_oi: float = 0.0
    net_delta: float = 0.0
    uoa_flags: list = field(default_factory=list)
    flows: Optional[FlowDirection] = None
    confluence: Optional[ConfluenceScore] = None
    trade: Optional[TradeSuggestion] = None


# Signal Computation 
def compute_gex(df: pd.DataFrame, spot: float) -> GEXData:
    """
    Compute Gamma Exposure per strike.

    GEX = OI × Gamma × 100 × Spot² × 0.01
    Convention: Call GEX positive, Put GEX negative (dealer perspective)
    """
    gex_by_strike = {}

    for strike, group in df.groupby('strike'):
        strike_gex = 0.0
        for _, row in group.iterrows():
            oi = row.get('open_interest', 0) or 0
            gamma = row.get('gamma', 0) or 0
            if oi == 0 or gamma == 0:
                continue

            contract_gex = float(oi) * float(gamma) * 100 * spot**2 * 0.01

            if row['type'] == 'CALL':
                strike_gex += contract_gex  # Dealers short calls → positive GEX
            else:
                strike_gex -= contract_gex  # Dealers short puts → negative GEX

        gex_by_strike[float(strike)] = strike_gex

    if not gex_by_strike:
        return GEXData(gex_by_strike={})

    # Find GEX flip: nearest-to-spot strike pair where net GEX changes sign
    sorted_strikes = sorted(gex_by_strike.keys())
    flip_point = None

    # Collect all sign-change pairs, pick the one closest to spot
    sign_changes = []
    for i in range(len(sorted_strikes) - 1):
        s1, s2 = sorted_strikes[i], sorted_strikes[i + 1]
        g1, g2 = gex_by_strike[s1], gex_by_strike[s2]
        if (g1 > 0 and g2 < 0) or (g1 < 0 and g2 > 0):
            # Interpolate the zero crossing
            ratio = abs(g1) / (abs(g1) + abs(g2))
            cross = s1 + ratio * (s2 - s1)
            dist_to_spot = abs(cross - spot)
            sign_changes.append((cross, dist_to_spot))

    if sign_changes:
        # Pick the sign change nearest to spot
        sign_changes.sort(key=lambda x: x[1])
        flip_point = sign_changes[0][0]

    # GEX$ (total dollar gamma)
    total_gex_dollar = sum(gex_by_strike.values())

    # Peaks
    positive_strikes = {k: v for k, v in gex_by_strike.items() if v > 0}
    negative_strikes = {k: v for k, v in gex_by_strike.items() if v < 0}

    positive_gex_peak = max(positive_strikes, key=positive_strikes.get) if positive_strikes else None
    negative_gex_peak = min(negative_strikes, key=negative_strikes.get) if negative_strikes else None

    # Regime: based on flip point if found, otherwise total net GEX
    regime = "unknown"
    if flip_point is not None:
        regime = "positive_gamma" if spot >= flip_point else "negative_gamma"
    elif total_gex_dollar > 0:
        regime = "positive_gamma"
    elif total_gex_dollar < 0:
        regime = "negative_gamma"

    return GEXData(
        gex_by_strike=gex_by_strike,
        flip_point=round(flip_point, 2) if flip_point else None,
        total_gex_dollar=total_gex_dollar,
        positive_gex_peak=positive_gex_peak,
        negative_gex_peak=negative_gex_peak,
        regime=regime,
    )


def compute_oi_walls(df: pd.DataFrame) -> OIWalls:
    """Find strikes with highest OI concentration."""
    calls = df[df['type'] == 'CALL']
    puts = df[df['type'] == 'PUT']

    call_wall = call_wall_oi = put_wall = put_wall_oi = None

    if not calls.empty and calls['open_interest'].notna().any():
        call_oi = calls[calls['open_interest'].notna()]
        if not call_oi.empty:
            max_idx = call_oi['open_interest'].idxmax()
            call_wall = call_oi.loc[max_idx, 'strike']
            call_wall_oi = int(call_oi.loc[max_idx, 'open_interest'])

    if not puts.empty and puts['open_interest'].notna().any():
        put_oi = puts[puts['open_interest'].notna()]
        if not put_oi.empty:
            max_idx = put_oi['open_interest'].idxmax()
            put_wall = put_oi.loc[max_idx, 'strike']
            put_wall_oi = int(put_oi.loc[max_idx, 'open_interest'])

    # Total OI wall (combined)
    total_wall = None
    combined = df.groupby('strike')['open_interest'].sum()
    if not combined.empty and combined.notna().any():
        total_wall = combined.idxmax()

    return OIWalls(
        call_wall=call_wall,
        call_wall_oi=call_wall_oi or 0,
        put_wall=put_wall,
        put_wall_oi=put_wall_oi or 0,
        total_wall=total_wall,
    )


def compute_max_pain(df: pd.DataFrame) -> Optional[float]:
    """
    Calculate max pain — the strike where most options expire worthless.

    For each candidate strike K, compute total intrinsic value of all options
    if underlying expires at K. Max pain = K that minimizes this total.
    """
    calls = df[df['type'] == 'CALL'][['strike', 'open_interest']].dropna()
    puts = df[df['type'] == 'PUT'][['strike', 'open_interest']].dropna()

    if calls.empty and puts.empty:
        return None

    strikes = sorted(df['strike'].unique())
    min_pain = float('inf')
    max_pain_strike = None

    for K in strikes:
        total_pain = 0

        # Call pain: call holders lose money when price is below their strike
        # But calls are ITM when K > strike, so call OI at strike s:
        # pain = call_OI(s) * max(0, K - s) * 100
        for _, row in calls.iterrows():
            s = row['strike']
            oi = row['open_interest'] or 0
            total_pain += oi * max(0, K - s) * 100

        # Put pain: put_OI(s) * max(0, s - K) * 100
        for _, row in puts.iterrows():
            s = row['strike']
            oi = row['open_interest'] or 0
            total_pain += oi * max(0, s - K) * 100

        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = K

    return max_pain_strike


def compute_iv_metrics(df: pd.DataFrame, spot: float) -> IVMetrics:
    """Compute ATM IV, IV skew, and IV by strike."""
    iv_data = df[df['iv'].notna()].copy()
    if iv_data.empty:
        return IVMetrics()

    # ATM IV: average IV of calls and puts at strike nearest to spot
    iv_data['dist_to_spot'] = abs(iv_data['strike'] - spot)
    atm_strike = iv_data.loc[iv_data['dist_to_spot'].idxmin(), 'strike']
    atm_options = iv_data[iv_data['strike'] == atm_strike]
    atm_iv = atm_options['iv'].mean() if not atm_options.empty else None

    # IV by strike (average of put+call IV per strike)
    iv_by_strike = iv_data.groupby('strike')['iv'].mean().to_dict()

    # 25-delta IV for skew
    put_25d_iv = None
    call_25d_iv = None

    puts_with_delta = iv_data[(iv_data['type'] == 'PUT') & (iv_data['delta'].notna())]
    calls_with_delta = iv_data[(iv_data['type'] == 'CALL') & (iv_data['delta'].notna())]

    if not puts_with_delta.empty:
        puts_with_delta = puts_with_delta.copy()
        puts_with_delta['dist_25d'] = abs(puts_with_delta['delta'].abs() - 0.25)
        nearest = puts_with_delta.loc[puts_with_delta['dist_25d'].idxmin()]
        put_25d_iv = nearest['iv']

    if not calls_with_delta.empty:
        calls_with_delta = calls_with_delta.copy()
        calls_with_delta['dist_25d'] = abs(calls_with_delta['delta'].abs() - 0.25)
        nearest = calls_with_delta.loc[calls_with_delta['dist_25d'].idxmin()]
        call_25d_iv = nearest['iv']

    iv_skew = None
    if put_25d_iv is not None and call_25d_iv is not None:
        iv_skew = round(put_25d_iv - call_25d_iv, 2)

    return IVMetrics(
        atm_iv=round(atm_iv, 2) if atm_iv else None,
        iv_skew=iv_skew,
        put_25d_iv=round(put_25d_iv, 2) if put_25d_iv else None,
        call_25d_iv=round(call_25d_iv, 2) if call_25d_iv else None,
        iv_by_strike=iv_by_strike,
    )


def compute_pc_ratios(df: pd.DataFrame) -> PCRatios:
    """Compute put/call ratios for volume and OI."""
    calls = df[df['type'] == 'CALL']
    puts = df[df['type'] == 'PUT']

    total_call_vol = int(calls['volume'].sum()) if calls['volume'].notna().any() else 0
    total_put_vol = int(puts['volume'].sum()) if puts['volume'].notna().any() else 0
    total_call_oi = int(calls['open_interest'].sum()) if calls['open_interest'].notna().any() else 0
    total_put_oi = int(puts['open_interest'].sum()) if puts['open_interest'].notna().any() else 0

    volume_pc = round(total_put_vol / total_call_vol, 3) if total_call_vol > 0 else None
    oi_pc = round(total_put_oi / total_call_oi, 3) if total_call_oi > 0 else None

    return PCRatios(
        volume_pc=volume_pc,
        oi_pc=oi_pc,
        total_call_volume=total_call_vol,
        total_put_volume=total_put_vol,
        total_call_oi=total_call_oi,
        total_put_oi=total_put_oi,
    )


def compute_delta_weighted_oi(df: pd.DataFrame) -> float:
    """
    Net delta-weighted OI.
    Positive = market is net long delta (bullish positioning).
    """
    result = 0.0
    for _, row in df.iterrows():
        oi = row.get('open_interest', 0) or 0
        delta = row.get('delta', 0) or 0
        if oi == 0 or delta == 0:
            continue
        if row['type'] == 'CALL':
            result += oi * delta
        else:
            result -= oi * abs(delta)
    return round(result, 2)


def compute_net_delta(df: pd.DataFrame) -> float:
    """
    Net delta exposure in share equivalents.
    = Σ(Call_OI × Δ × 100) - Σ(Put_OI × |Δ| × 100)
    """
    result = 0.0
    for _, row in df.iterrows():
        oi = row.get('open_interest', 0) or 0
        delta = row.get('delta', 0) or 0
        if oi == 0 or delta == 0:
            continue
        if row['type'] == 'CALL':
            result += oi * delta * 100
        else:
            result -= oi * abs(delta) * 100
    return round(result, 2)


def compute_uoa(df: pd.DataFrame, voi_threshold: float = 2.0) -> list:
    """
    Flag unusual options activity where Volume/OI > threshold.
    """
    flags = []
    for _, row in df.iterrows():
        vol = row.get('volume', 0) or 0
        oi = row.get('open_interest', 0) or 0
        if oi > 0 and vol > 0:
            voi = vol / oi
            if voi > voi_threshold:
                flags.append(UOAFlag(
                    strike=row['strike'],
                    opt_type=row['type'],
                    volume=int(vol),
                    oi=int(oi),
                    voi_ratio=round(voi, 2),
                ))

    # Sort by voi_ratio descending
    flags.sort(key=lambda x: x.voi_ratio, reverse=True)
    return flags[:10]  # Top 10


def compute_flow_direction(df: pd.DataFrame, spot: float) -> FlowDirection:
    """
    Estimate charm and vanna directional bias.

    Charm: if more ITM calls than ITM puts → buying pressure into close.
    Vanna: if IV is falling → vanna supports rally (positive feedback).
    """
    calls = df[df['type'] == 'CALL']
    puts = df[df['type'] == 'PUT']

    # Charm: compare ITM call OI vs ITM put OI
    itm_call_oi = calls[calls['strike'] < spot]['open_interest'].sum() or 0
    itm_put_oi = puts[puts['strike'] > spot]['open_interest'].sum() or 0

    charm_diff = itm_call_oi - itm_put_oi
    if charm_diff > 0:
        charm_bias = "buy"
    elif charm_diff < 0:
        charm_bias = "sell"
    else:
        charm_bias = "neutral"

    charm_strength = abs(charm_diff) / max(itm_call_oi + itm_put_oi, 1)

    # Vanna: based on net vanna exposure
    # Simplified: if more OTM puts have high vanna, IV drop = buying pressure
    vanna_sum = 0.0
    for _, row in df.iterrows():
        oi = row.get('open_interest', 0) or 0
        vega = row.get('vega', 0) or 0  # Using vega as proxy when vanna unavailable
        if oi == 0 or vega == 0:
            continue
        if row['type'] == 'CALL':
            vanna_sum += oi * vega
        else:
            vanna_sum -= oi * vega

    vanna_bias = "buy" if vanna_sum > 0 else ("sell" if vanna_sum < 0 else "neutral")
    vanna_strength = min(abs(vanna_sum) / 10000, 1.0)  # Normalized

    return FlowDirection(
        charm_bias=charm_bias,
        charm_strength=round(charm_strength, 3),
        vanna_bias=vanna_bias,
        vanna_strength=round(vanna_strength, 3),
    )


def compute_confluence(
    spot: float,
    gex: GEXData,
    walls: OIWalls,
    max_pain_strike: Optional[float],
    iv: IVMetrics,
    pc: PCRatios,
    flows: FlowDirection,
    uoa_flags: list,
    is_last_2h: bool = False,
) -> ConfluenceScore:
    """
    Compute confluence score from all signals.

    Each signal contributes a weighted vote toward LONG or SHORT.
    """
    long_score = 0
    short_score = 0
    breakdown = {}
    active = 0

    # 1. GEX Regime (weight 3)
    if gex.regime == "positive_gamma":
        long_score += 3
        breakdown["GEX Regime"] = ("LONG", 3)
        active += 1
    elif gex.regime == "negative_gamma":
        short_score += 3
        breakdown["GEX Regime"] = ("SHORT", 3)
        active += 1

    # 2. OI Wall Proximity (weight 2)
    if walls.put_wall and walls.call_wall and spot:
        range_size = walls.call_wall - walls.put_wall
        if range_size > 0:
            dist_to_put = (spot - walls.put_wall) / range_size
            dist_to_call = (walls.call_wall - spot) / range_size

            if dist_to_put < 0.2:  # Near put wall → support
                long_score += 2
                breakdown["OI Wall Proximity"] = ("LONG", 2)
                active += 1
            elif dist_to_call < 0.2:  # Near call wall → resistance
                short_score += 2
                breakdown["OI Wall Proximity"] = ("SHORT", 2)
                active += 1

    # 3. OI Wall Break (weight 3)
    if walls.put_wall and spot < walls.put_wall:
        short_score += 3
        breakdown["Wall Break"] = ("SHORT", 3)
        active += 1
    elif walls.call_wall and spot > walls.call_wall:
        long_score += 3
        breakdown["Wall Break"] = ("LONG", 3)
        active += 1

    # 4. Max Pain (weight 1)
    if max_pain_strike and spot:
        if spot < max_pain_strike:
            long_score += 1
            breakdown["Max Pain"] = ("LONG", 1)
            active += 1
        elif spot > max_pain_strike:
            short_score += 1
            breakdown["Max Pain"] = ("SHORT", 1)
            active += 1

    # 5. P/C Volume Ratio (weight 1, contrarian)
    if pc.volume_pc is not None:
        if pc.volume_pc > 1.2:  # Heavy puts → contrarian long
            long_score += 1
            breakdown["P/C Volume"] = ("LONG", 1)
            active += 1
        elif pc.volume_pc < 0.6:  # Heavy calls → contrarian short
            short_score += 1
            breakdown["P/C Volume"] = ("SHORT", 1)
            active += 1

    # 6. IV Skew (weight 1)
    if iv.iv_skew is not None:
        if iv.iv_skew > 3:  # Steep skew → increasing fear → slight bear
            short_score += 1
            breakdown["IV Skew"] = ("SHORT", 1)
            active += 1
        elif iv.iv_skew < -1:  # Inverted skew → extreme greed → slight bear
            short_score += 1
            breakdown["IV Skew"] = ("SHORT", 1)
            active += 1

    # 7. UOA (weight 2)
    if uoa_flags:
        call_uoa = sum(1 for u in uoa_flags if u.opt_type == 'CALL')
        put_uoa = sum(1 for u in uoa_flags if u.opt_type == 'PUT')
        if call_uoa > put_uoa:
            long_score += 2
            breakdown["UOA"] = ("LONG", 2)
            active += 1
        elif put_uoa > call_uoa:
            short_score += 2
            breakdown["UOA"] = ("SHORT", 2)
            active += 1

    # 8. Charm Direction (weight 1 normally, 2 in last 2h)
    charm_weight = 2 if is_last_2h else 1
    if flows.charm_bias == "buy":
        long_score += charm_weight
        breakdown["Charm"] = ("LONG", charm_weight)
        active += 1
    elif flows.charm_bias == "sell":
        short_score += charm_weight
        breakdown["Charm"] = ("SHORT", charm_weight)
        active += 1

    # 9. Vanna Direction (weight 1)
    if flows.vanna_bias == "buy":
        long_score += 1
        breakdown["Vanna"] = ("LONG", 1)
        active += 1
    elif flows.vanna_bias == "sell":
        short_score += 1
        breakdown["Vanna"] = ("SHORT", 1)
        active += 1

    # Net score & bias
    net_score = long_score - short_score
    max_score = 16
    total_signals = 10

    if net_score >= 4:
        net_bias = "LONG"
    elif net_score <= -4:
        net_bias = "SHORT"
    elif net_score > 0:
        net_bias = "SLIGHT LONG"
    elif net_score < 0:
        net_bias = "SLIGHT SHORT"
    else:
        net_bias = "NEUTRAL"

    # Confidence based on how many signals are active
    if active >= 7:
        confidence = "high"
    elif active >= 4:
        confidence = "medium"
    else:
        confidence = "low"

    # Summary
    top_signals = sorted(breakdown.items(), key=lambda x: x[1][1], reverse=True)[:3]
    signal_descs = [f"{name} ({side})" for name, (side, _) in top_signals]
    summary = f"{net_bias} ({net_score:+d}) — {', '.join(signal_descs)}" if signal_descs else "No signals active"

    return ConfluenceScore(
        long_score=long_score,
        short_score=short_score,
        max_score=max_score,
        net_bias=net_bias,
        net_score=net_score,
        confidence=confidence,
        active_signals=active,
        total_signals=total_signals,
        breakdown=breakdown,
        summary=summary,
    )


def compute_trade_suggestion(
    spot: float,
    confluence: ConfluenceScore,
    gex: GEXData,
    walls: OIWalls,
    max_pain_strike: Optional[float],
) -> TradeSuggestion:
    """
    Generate trade entry/stop/target based on confluence and levels.
    """
    if abs(confluence.net_score) < 2:
        return TradeSuggestion(
            bias="NEUTRAL",
            reason="Conflicting signals — no high-conviction setup",
        )

    if confluence.net_score > 0:
        # LONG bias
        entry = None
        stop = None
        target = None
        reasons = []

        # Entry near support (put wall or GEX flip)
        support_levels = sorted(filter(None, [walls.put_wall, gex.flip_point]))
        resistance_levels = sorted(filter(None, [walls.call_wall, max_pain_strike]))

        if support_levels:
            # Nearest support below spot
            below_spot = [l for l in support_levels if l < spot]
            entry = below_spot[-1] if below_spot else support_levels[0]
            reasons.append(f"entry near support {entry}")

        if support_levels:
            # Stop below lowest support
            stop = min(support_levels) - 1
            reasons.append(f"stop below {min(support_levels)}")

        if resistance_levels:
            # Target at nearest resistance above spot
            above_spot = [l for l in resistance_levels if l > spot]
            target = above_spot[0] if above_spot else resistance_levels[-1]
            reasons.append(f"target {target}")

        return TradeSuggestion(
            bias="LONG",
            entry=entry,
            stop=stop,
            target=target,
            reason="; ".join(reasons),
        )
    else:
        # SHORT bias
        entry = None
        stop = None
        target = None
        reasons = []

        resistance_levels = sorted(filter(None, [walls.call_wall, gex.flip_point]))
        support_levels = sorted(filter(None, [walls.put_wall, max_pain_strike]))

        if resistance_levels:
            above_spot = [l for l in resistance_levels if l > spot]
            entry = above_spot[0] if above_spot else resistance_levels[-1]
            reasons.append(f"entry near resistance {entry}")

        if resistance_levels:
            stop = max(resistance_levels) + 1
            reasons.append(f"stop above {max(resistance_levels)}")

        if support_levels:
            below_spot = [l for l in support_levels if l < spot]
            target = below_spot[-1] if below_spot else support_levels[0]
            reasons.append(f"target {target}")

        return TradeSuggestion(
            bias="SHORT",
            entry=entry,
            stop=stop,
            target=target,
            reason="; ".join(reasons),
        )


# ─── Master Signal Computation ───────────────────────────────────────────────

def compute_all_signals(df: pd.DataFrame, is_last_2h: bool = False) -> SignalSnapshot:
    """
    Compute all signals from a single snapshot dataframe.

    Args:
        df: DataFrame with columns from fetch_qqq_live.py
        is_last_2h: True if within last 2 hours of session (boosts charm weight)

    Returns:
        SignalSnapshot with all computed signals
    """
    if df.empty:
        return SignalSnapshot()

    # Sanitize: convert Decimal/object columns to native float
    numeric_cols = ['strike', 'bid', 'ask', 'last', 'mid', 'spread', 'iv',
                    'volume', 'open_interest', 'delta', 'gamma', 'theta',
                    'vega', 'rho', 'underlying']
    df = df.copy()
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Get spot and metadata
    spot = float(df['underlying'].iloc[0])
    dte = int(df['dte'].iloc[0]) if 'dte' in df.columns else 0
    timestamp = df['timestamp'].iloc[0] if 'timestamp' in df.columns else ""

    # Get latest snapshot only (if dataframe has multiple timestamps)
    if 'timestamp' in df.columns and df['timestamp'].nunique() > 1:
        latest_ts = df['timestamp'].max()
        df = df[df['timestamp'] == latest_ts].copy()
        timestamp = latest_ts
        spot = float(df['underlying'].iloc[0])

    # Compute all signals
    gex = compute_gex(df, spot)
    walls = compute_oi_walls(df)
    max_pain = compute_max_pain(df)
    iv = compute_iv_metrics(df, spot)
    pc = compute_pc_ratios(df)
    dwoi = compute_delta_weighted_oi(df)
    net_delta = compute_net_delta(df)
    uoa = compute_uoa(df)
    flows = compute_flow_direction(df, spot)

    confluence = compute_confluence(
        spot=spot,
        gex=gex,
        walls=walls,
        max_pain_strike=max_pain,
        iv=iv,
        pc=pc,
        flows=flows,
        uoa_flags=uoa,
        is_last_2h=is_last_2h,
    )

    trade = compute_trade_suggestion(
        spot=spot,
        confluence=confluence,
        gex=gex,
        walls=walls,
        max_pain_strike=max_pain,
    )

    return SignalSnapshot(
        timestamp=timestamp,
        spot=spot,
        dte=dte,
        gex=gex,
        oi_walls=walls,
        max_pain=max_pain,
        iv=iv,
        pc_ratios=pc,
        delta_weighted_oi=dwoi,
        net_delta=net_delta,
        uoa_flags=uoa,
        flows=flows,
        confluence=confluence,
        trade=trade,
    )


def compute_signal_history(df: pd.DataFrame) -> list:
    """
    Compute signals for each timestamp in a multi-snapshot DataFrame.
    Returns list of SignalSnapshots ordered by timestamp.
    """
    if df.empty or 'timestamp' not in df.columns:
        return []

    snapshots = []
    for ts in sorted(df['timestamp'].unique()):
        ts_df = df[df['timestamp'] == ts].copy()
        snap = compute_all_signals(ts_df)
        snapshots.append(snap)

    return snapshots


def compute_volume_heatmap(df: pd.DataFrame, n_strikes: int = 5) -> pd.DataFrame:
    """
    Compute volume deltas per minute for ATM ±n strikes.

    Returns a DataFrame with:
        - index: label like "615C", "610P" (calls on top, puts on bottom)
        - columns: timestamps
        - values: volume change since previous snapshot

    Uses the LATEST spot to determine which strikes are ATM.
    """
    if df.empty or 'timestamp' not in df.columns:
        return pd.DataFrame()

    # Sanitize
    df = df.copy()
    for col in ['strike', 'volume', 'underlying']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    timestamps = sorted(df['timestamp'].unique())
    if len(timestamps) < 2:
        return pd.DataFrame()

    # Get spot from latest snapshot to determine ATM
    latest = df[df['timestamp'] == timestamps[-1]]
    spot = float(latest['underlying'].iloc[0])

    # Find nearest n strikes above and below spot
    all_strikes = sorted(df['strike'].dropna().unique())
    above = sorted([s for s in all_strikes if s >= spot])[:n_strikes]
    below = sorted([s for s in all_strikes if s < spot], reverse=True)[:n_strikes]

    # Build label order: calls (high to low) then puts (high to low)
    call_labels = [f"{int(s)}C" for s in reversed(above)] + [f"{int(s)}C" for s in below]
    put_labels = [f"{int(s)}P" for s in reversed(above)] + [f"{int(s)}P" for s in below]

    # We want: calls above ATM on top, then puts below ATM on bottom
    target_strikes_calls = list(reversed(above))  # high to low
    target_strikes_puts = list(below)  # high to low (already reversed)

    # Build rows: calls (high→low) on top, then puts (high→low) on bottom
    row_labels = []
    row_strikes = []
    row_types = []

    for s in target_strikes_calls:
        row_labels.append(f"{int(s)}C")
        row_strikes.append(s)
        row_types.append("CALL")

    for s in target_strikes_puts:
        row_labels.append(f"{int(s)}P")
        row_strikes.append(s)
        row_types.append("PUT")

    # Build the heatmap matrix
    heatmap_data = {label: [] for label in row_labels}
    time_labels = []

    prev_volumes = {}  # (strike, type) -> volume

    for ts in timestamps:
        ts_df = df[df['timestamp'] == ts]
        curr_volumes = {}

        for _, row in ts_df.iterrows():
            key = (row['strike'], row['type'])
            curr_volumes[key] = row.get('volume', 0) or 0

        # Only add deltas after first snapshot
        if prev_volumes:
            # Use just HH:MM from timestamp
            ts_short = ts[-8:-3] if len(ts) > 5 else ts  # "HH:MM" from "YYYY-MM-DD HH:MM:SS"
            time_labels.append(ts_short)

            for label, strike, opt_type in zip(row_labels, row_strikes, row_types):
                key = (strike, opt_type)
                curr = curr_volumes.get(key, 0)
                prev = prev_volumes.get(key, 0)
                delta = max(0, curr - prev)  # Volume can only increase intraday
                heatmap_data[label].append(delta)

        prev_volumes = curr_volumes

    if not time_labels:
        return pd.DataFrame()

    result = pd.DataFrame(heatmap_data, index=time_labels).T
    return result
