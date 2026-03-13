import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta

from signals import compute_all_signals, compute_signal_history, compute_volume_heatmap, SignalSnapshot

# Config
DATA_DIR = Path(__file__).parent / 'data'
POLL_INTERVAL = 15  # Dashboard auto-refresh seconds

st.set_page_config(
    page_title="QQQ Options Signals",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# Styling
def inject_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

    /* Global */
    .stApp {
        font-family: 'Inter', sans-serif;
        background: #0a0e17;
    }

    /* Header overrides */
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Inter', sans-serif !important;
        font-weight: 700 !important;
    }

    /* Custom metric cards */
    .signal-card {
        background: linear-gradient(135deg, #131927 0%, #1a2035 100%);
        border: 1px solid #1e2a42;
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 10px;
    }

    .signal-label {
        font-size: 11px;
        font-weight: 600;
        color: #6b7a99;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 4px;
    }

    .signal-value {
        font-size: 22px;
        font-weight: 800;
        color: #e8ecf4;
        margin: 0;
    }

    .signal-value.positive { color: #00d4aa; }
    .signal-value.negative { color: #ff4d6a; }
    .signal-value.neutral  { color: #6b7a99; }

    /* Regime Banner */
    .regime-banner {
        text-align: center;
        padding: 14px 20px;
        border-radius: 12px;
        margin: 8px 0 16px 0;
        font-size: 15px;
        font-weight: 700;
        letter-spacing: 1.5px;
        text-transform: uppercase;
    }

    .regime-positive {
        background: linear-gradient(90deg, #002e22 0%, #003d2d 50%, #002e22 100%);
        color: #00d4aa;
        border: 1px solid #00d4aa33;
        box-shadow: 0 0 30px #00d4aa11;
    }

    .regime-negative {
        background: linear-gradient(90deg, #2e0015 0%, #3d001e 50%, #2e0015 100%);
        color: #ff4d6a;
        border: 1px solid #ff4d6a33;
        box-shadow: 0 0 30px #ff4d6a11;
    }

    .regime-unknown {
        background: linear-gradient(90deg, #1a1a2e 0%, #232345 50%, #1a1a2e 100%);
        color: #6b7a99;
        border: 1px solid #6b7a9933;
    }

    /* Confluence bar */
    .confluence-bar {
        height: 10px;
        border-radius: 5px;
        overflow: hidden;
        background: #1a2035;
        margin: 6px 0;
    }

    .confluence-fill {
        height: 100%;
        border-radius: 5px;
        transition: width 0.3s ease;
    }

    .fill-long { background: linear-gradient(90deg, #00d4aa, #00f5c8); }
    .fill-short { background: linear-gradient(90deg, #ff4d6a, #ff6b81); }

    /* Level line list */
    .level-item {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 8px 12px;
        border-radius: 8px;
        margin: 4px 0;
        font-size: 14px;
        font-weight: 600;
    }

    .level-call { background: #1a0a0a; color: #ff4d6a; }
    .level-put { background: #0a1a0a; color: #00d4aa; }
    .level-gex { background: #1a1a0a; color: #ffd54f; }
    .level-mp { background: #0a0a1a; color: #90caf9; }
    .level-spot { background: #1e2a42; color: #e8ecf4; }

    /* Trade suggestion box */
    .trade-box {
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
    }

    .trade-long {
        background: linear-gradient(135deg, #002e22, #003d2d);
        border: 1px solid #00d4aa44;
    }

    .trade-short {
        background: linear-gradient(135deg, #2e0015, #3d001e);
        border: 1px solid #ff4d6a44;
    }

    .trade-neutral {
        background: linear-gradient(135deg, #1a1a2e, #232345);
        border: 1px solid #6b7a9944;
    }

    /* Alert log */
    .alert-item {
        padding: 8px 12px;
        border-radius: 8px;
        margin: 3px 0;
        font-size: 13px;
        background: #131927;
        border-left: 3px solid #6b7a99;
    }

    .alert-critical { border-left-color: #ff4d6a; }
    .alert-warning  { border-left-color: #ffd54f; }
    .alert-info     { border-left-color: #90caf9; }

    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Metric overrides */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #131927 0%, #1a2035 100%);
        border: 1px solid #1e2a42;
        border-radius: 12px;
        padding: 14px;
    }

    [data-testid="stMetricLabel"] {
        font-size: 11px !important;
        font-weight: 600 !important;
        color: #6b7a99 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    [data-testid="stMetricValue"] {
        font-size: 22px !important;
        font-weight: 800 !important;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: transparent;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: 600;
    }

    /* Mobile tweaks */
    @media (max-width: 768px) {
        .signal-value { font-size: 18px; }
        .regime-banner { font-size: 13px; padding: 10px 12px; }
        [data-testid="stMetricValue"] { font-size: 18px !important; }
    }
    </style>
    """, unsafe_allow_html=True)


# Data Loading
def get_ny_time():
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo("America/New_York")
        return datetime.now(tz)
    except Exception:
        return datetime.now()


def is_market_open(now=None):
    if now is None:
        now = get_ny_time()
    if now.weekday() >= 5:
        return False
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close


def load_latest_data(dte_type: str = "0dte") -> pd.DataFrame:
    """Load the most recent parquet file for given DTE type."""
    if not DATA_DIR.exists():
        return pd.DataFrame()

    # Find most recent date directory with data
    date_dirs = sorted(DATA_DIR.iterdir(), reverse=True)
    for d in date_dirs:
        if d.is_dir():
            parquet_file = d / f"{dte_type}.parquet"
            if parquet_file.exists():
                try:
                    return pd.read_parquet(parquet_file)
                except Exception:
                    continue
    return pd.DataFrame()


def load_data_for_date(date_str: str, dte_type: str = "0dte") -> pd.DataFrame:
    """Load parquet for a specific date."""
    parquet_file = DATA_DIR / date_str / f"{dte_type}.parquet"
    if parquet_file.exists():
        try:
            return pd.read_parquet(parquet_file)
        except Exception:
            pass
    return pd.DataFrame()


def get_available_dates() -> list:
    """Get list of dates with data."""
    if not DATA_DIR.exists():
        return []
    dates = []
    for d in sorted(DATA_DIR.iterdir(), reverse=True):
        if d.is_dir() and (d / "0dte.parquet").exists():
            dates.append(d.name)
    return dates


# Chart Builders
PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor='rgba(10,14,23,0)',
    plot_bgcolor='rgba(19,25,39,0.5)',
    font=dict(family="Inter", color="#e8ecf4"),
    margin=dict(l=10, r=10, t=40, b=10),
    xaxis=dict(gridcolor='#1e2a42', showgrid=True),
    yaxis=dict(gridcolor='#1e2a42', showgrid=True),
)


def chart_gex_profile(signals: SignalSnapshot) -> go.Figure:
    """Horizontal bar chart of GEX per strike."""
    if not signals.gex or not signals.gex.gex_by_strike:
        return go.Figure().update_layout(title="GEX Profile — No Data", **PLOTLY_LAYOUT)

    strikes = sorted(signals.gex.gex_by_strike.keys())
    values = [signals.gex.gex_by_strike[s] for s in strikes]

    colors = ['#00d4aa' if v >= 0 else '#ff4d6a' for v in values]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=strikes,
        x=values,
        orientation='h',
        marker_color=colors,
        marker_line_width=0,
        opacity=0.85,
    ))

    # Add lines for key levels
    if signals.gex.flip_point:
        fig.add_hline(y=signals.gex.flip_point, line_dash="dash", line_color="#ffd54f",
                      annotation_text=f"GEX Flip {signals.gex.flip_point}", annotation_font_color="#ffd54f")
    if signals.spot:
        fig.add_hline(y=signals.spot, line_dash="solid", line_color="#e8ecf4",
                      annotation_text=f"Spot {signals.spot}", annotation_font_color="#e8ecf4")

    fig.update_layout(
        title="GEX Profile ($ per strike)",
        xaxis_title="GEX ($)",
        yaxis_title="Strike",
        height=450,
        showlegend=False,
        **PLOTLY_LAYOUT,
    )
    return fig


def chart_oi_distribution(df: pd.DataFrame, signals: SignalSnapshot) -> go.Figure:
    """Dual bar chart of call vs put OI per strike."""
    if df.empty:
        return go.Figure().update_layout(title="OI Distribution — No Data", **PLOTLY_LAYOUT)

    # Get latest snapshot
    if 'timestamp' in df.columns and df['timestamp'].nunique() > 1:
        df = df[df['timestamp'] == df['timestamp'].max()]

    calls = df[df['type'] == 'CALL'].groupby('strike')['open_interest'].sum().reset_index()
    puts = df[df['type'] == 'PUT'].groupby('strike')['open_interest'].sum().reset_index()

    fig = go.Figure()
    if not calls.empty:
        fig.add_trace(go.Bar(
            x=calls['strike'], y=calls['open_interest'],
            name='Call OI', marker_color='#ff4d6a', opacity=0.7,
        ))
    if not puts.empty:
        fig.add_trace(go.Bar(
            x=puts['strike'], y=puts['open_interest'],
            name='Put OI', marker_color='#00d4aa', opacity=0.7,
        ))

    # Mark walls
    if signals.oi_walls:
        if signals.oi_walls.call_wall:
            fig.add_vline(x=signals.oi_walls.call_wall, line_dash="dash", line_color="#ff4d6a",
                          annotation_text=f"Call Wall", annotation_font_color="#ff4d6a")
        if signals.oi_walls.put_wall:
            fig.add_vline(x=signals.oi_walls.put_wall, line_dash="dash", line_color="#00d4aa",
                          annotation_text=f"Put Wall", annotation_font_color="#00d4aa")

    fig.update_layout(
        title="Open Interest Distribution",
        xaxis_title="Strike",
        yaxis_title="Open Interest",
        barmode='group',
        height=350,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        **PLOTLY_LAYOUT,
    )
    return fig


def chart_iv_skew(df: pd.DataFrame, signals: SignalSnapshot) -> go.Figure:
    """IV across strikes."""
    if df.empty or signals.iv is None or not signals.iv.iv_by_strike:
        return go.Figure().update_layout(title="IV Skew — No Data", **PLOTLY_LAYOUT)

    # Get latest snapshot
    if 'timestamp' in df.columns and df['timestamp'].nunique() > 1:
        df = df[df['timestamp'] == df['timestamp'].max()]

    # Separate calls and puts
    calls_iv = df[df['type'] == 'CALL'][['strike', 'iv']].dropna().sort_values('strike')
    puts_iv = df[df['type'] == 'PUT'][['strike', 'iv']].dropna().sort_values('strike')

    fig = go.Figure()
    if not calls_iv.empty:
        fig.add_trace(go.Scatter(
            x=calls_iv['strike'], y=calls_iv['iv'],
            mode='lines+markers', name='Call IV',
            line=dict(color='#ff4d6a', width=2),
            marker=dict(size=4),
        ))
    if not puts_iv.empty:
        fig.add_trace(go.Scatter(
            x=puts_iv['strike'], y=puts_iv['iv'],
            mode='lines+markers', name='Put IV',
            line=dict(color='#00d4aa', width=2),
            marker=dict(size=4),
        ))

    if signals.spot:
        fig.add_vline(x=signals.spot, line_dash="dot", line_color="#e8ecf4",
                      annotation_text="Spot", annotation_font_color="#e8ecf4")

    fig.update_layout(
        title="IV Smile / Skew",
        xaxis_title="Strike",
        yaxis_title="IV (%)",
        height=350,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        **PLOTLY_LAYOUT,
    )
    return fig


def chart_signal_history(history: list, metric: str, label: str) -> go.Figure:
    """Sparkline chart for signal evolution over time."""
    if not history:
        return go.Figure().update_layout(title=f"{label} — No History", **PLOTLY_LAYOUT)

    timestamps = []
    values = []

    for snap in history:
        timestamps.append(snap.timestamp)
        if metric == "gex_flip":
            values.append(snap.gex.flip_point if snap.gex and snap.gex.flip_point else None)
        elif metric == "pc_volume":
            values.append(snap.pc_ratios.volume_pc if snap.pc_ratios else None)
        elif metric == "atm_iv":
            values.append(snap.iv.atm_iv if snap.iv else None)
        elif metric == "net_delta":
            values.append(snap.net_delta)
        elif metric == "gex_dollar":
            values.append(snap.gex.total_gex_dollar if snap.gex else None)
        elif metric == "confluence":
            values.append(snap.confluence.net_score if snap.confluence else None)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=timestamps, y=values,
        mode='lines',
        line=dict(color='#90caf9', width=2),
        fill='tozeroy',
        fillcolor='rgba(144,202,249,0.1)',
    ))

    fig.update_layout(
        title=label,
        height=180,
        showlegend=False,
        margin=dict(l=10, r=10, t=35, b=10),
        **{k: v for k, v in PLOTLY_LAYOUT.items() if k not in ['margin']},
    )
    return fig


def chart_volume_heatmap(df: pd.DataFrame) -> go.Figure:
    """Volume delta heatmap for ATM ±5 strikes."""
    heatmap_df = compute_volume_heatmap(df, n_strikes=5)

    if heatmap_df.empty:
        return go.Figure().update_layout(title="Volume Flow — Need 2+ snapshots", **PLOTLY_LAYOUT)

    labels = list(heatmap_df.index)
    times = list(heatmap_df.columns)
    values = heatmap_df.values

    # Build custom colorscale: calls = green tones, puts = red tones
    # Determine which rows are calls vs puts
    row_colors = []
    for label in labels:
        if label.endswith('C'):
            row_colors.append('call')
        else:
            row_colors.append('put')

    # Find the split point
    split_idx = len([r for r in row_colors if r == 'call'])

    # Create two separate heatmaps stacked
    fig = go.Figure()

    # Call heatmap (top half — green)
    if split_idx > 0:
        fig.add_trace(go.Heatmap(
            z=values[:split_idx],
            x=times,
            y=labels[:split_idx],
            colorscale=[
                [0, '#0a0e17'],
                [0.3, '#003d2d'],
                [0.6, '#00d4aa'],
                [1, '#00ffcc'],
            ],
            showscale=False,
            hovertemplate='%{y}<br>%{x}<br>Volume Δ: %{z}<extra></extra>',
        ))

    # Put heatmap (bottom half — red)
    if split_idx < len(labels):
        fig.add_trace(go.Heatmap(
            z=values[split_idx:],
            x=times,
            y=labels[split_idx:],
            colorscale=[
                [0, '#0a0e17'],
                [0.3, '#3d001e'],
                [0.6, '#ff4d6a'],
                [1, '#ff6b81'],
            ],
            showscale=False,
            hovertemplate='%{y}<br>%{x}<br>Volume Δ: %{z}<extra></extra>',
        ))

    # Add a horizontal line at the ATM boundary
    if split_idx > 0 and split_idx < len(labels):
        fig.add_hline(
            y=(split_idx - 0.5),
            line_dash="dot",
            line_color="#ffd54f",
            line_width=1,
        )

    fig.update_layout(
        title="🔥 Volume Flow (Δ per poll) — Calls ↑ Puts ↓",
        xaxis_title="Time",
        height=380,
        yaxis=dict(autorange='reversed', gridcolor='#1e2a42'),
        **{k: v for k, v in PLOTLY_LAYOUT.items() if k not in ['yaxis']},
    )
    return fig


# UI Components 
def render_regime_banner(signals: SignalSnapshot):
    """Render the regime banner."""
    if not signals.gex:
        css_class = "regime-unknown"
        text = "⏳ NO DATA — Waiting for options data"
    elif signals.gex.regime == "positive_gamma":
        css_class = "regime-positive"
        text = "🟢 POSITIVE GAMMA — MEAN REVERSION MODE — FADE EXTREMES"
    elif signals.gex.regime == "negative_gamma":
        css_class = "regime-negative"
        text = "🔴 NEGATIVE GAMMA — TREND MODE — RIDE MOMENTUM"
    else:
        css_class = "regime-unknown"
        text = "⚪ REGIME UNKNOWN — Insufficient data"

    st.markdown(f'<div class="regime-banner {css_class}">{text}</div>', unsafe_allow_html=True)


def render_signal_card(label: str, value: str, css_class: str = ""):
    """Render a signal metric card."""
    st.markdown(f"""
    <div class="signal-card">
        <div class="signal-label">{label}</div>
        <div class="signal-value {css_class}">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def render_key_levels(signals: SignalSnapshot):
    """Render the key levels list."""
    levels = []

    if signals.oi_walls and signals.oi_walls.call_wall:
        levels.append(("level-call", f"🔴 Call Wall", f"${signals.oi_walls.call_wall:.0f}",
                        f"{signals.oi_walls.call_wall_oi:,} OI"))
    if signals.gex and signals.gex.flip_point:
        levels.append(("level-gex", "🟡 GEX Flip", f"${signals.gex.flip_point:.1f}", ""))
    if signals.spot:
        levels.append(("level-spot", "◆ Spot", f"${signals.spot:.2f}", ""))
    if signals.max_pain:
        levels.append(("level-mp", "★ Max Pain", f"${signals.max_pain:.0f}", ""))
    if signals.oi_walls and signals.oi_walls.put_wall:
        levels.append(("level-put", "🟢 Put Wall", f"${signals.oi_walls.put_wall:.0f}",
                        f"{signals.oi_walls.put_wall_oi:,} OI"))

    # Sort by value descending
    levels.sort(key=lambda x: float(x[2].replace('$', '').replace(',', '')), reverse=True)

    for css, label, value, extra in levels:
        extra_html = f" <span style='font-size:12px;opacity:0.7'>({extra})</span>" if extra else ""
        st.markdown(f"""
        <div class="level-item {css}">
            <span>{label}</span>
            <span>{value}{extra_html}</span>
        </div>
        """, unsafe_allow_html=True)


def render_confluence(signals: SignalSnapshot):
    """Render the confluence scoring display."""
    if not signals.confluence:
        st.info("No confluence data")
        return

    c = signals.confluence

    # Net bias with emoji
    bias_emoji = {"LONG": "🟢", "SHORT": "🔴", "SLIGHT LONG": "🟢",
                  "SLIGHT SHORT": "🔴", "NEUTRAL": "⚪"}.get(c.net_bias, "⚪")

    st.markdown(f"""
    <div class="signal-card">
        <div class="signal-label">Confluence Score</div>
        <div style="display: flex; justify-content: space-between; align-items: center; margin: 8px 0;">
            <span style="color: #00d4aa; font-weight: 700;">LONG {c.long_score}/{c.max_score}</span>
            <span style="color: #ff4d6a; font-weight: 700;">SHORT {c.short_score}/{c.max_score}</span>
        </div>
        <div class="confluence-bar">
            <div class="confluence-fill fill-long" style="width: {c.long_score / c.max_score * 100}%;"></div>
        </div>
        <div class="confluence-bar">
            <div class="confluence-fill fill-short" style="width: {c.short_score / c.max_score * 100}%;"></div>
        </div>
        <div style="margin-top: 10px;">
            <span style="font-size: 18px; font-weight: 800; color: #e8ecf4;">
                {bias_emoji} {c.net_bias} ({c.net_score:+d})
            </span>
            <span style="font-size: 12px; color: #6b7a99; margin-left: 10px;">
                Confidence: {c.confidence.upper()} ({c.active_signals}/{c.total_signals} signals)
            </span>
        </div>
        <div style="font-size: 13px; color: #90caf9; margin-top: 6px;">{c.summary}</div>
    </div>
    """, unsafe_allow_html=True)


def render_confluence_breakdown(signals: SignalSnapshot):
    """Render the signal breakdown table."""
    if not signals.confluence or not signals.confluence.breakdown:
        return

    for name, (side, weight) in signals.confluence.breakdown.items():
        color = "#00d4aa" if side == "LONG" else "#ff4d6a"
        blocks = "█" * weight + "░" * (3 - weight)
        st.markdown(f"""
        <div style="display:flex; justify-content:space-between; padding:4px 8px; font-size:13px;">
            <span style="color:#e8ecf4;">{name}</span>
            <span style="color:{color}; font-weight:700;">{blocks} {side} +{weight}</span>
        </div>
        """, unsafe_allow_html=True)


def render_trade_suggestion(signals: SignalSnapshot):
    """Render trade suggestion box."""
    if not signals.trade:
        return

    t = signals.trade
    if t.bias == "LONG":
        css = "trade-long"
        emoji = "🟢"
    elif t.bias == "SHORT":
        css = "trade-short"
        emoji = "🔴"
    else:
        css = "trade-neutral"
        emoji = "⚪"

    entry_text = f"${t.entry:.1f}" if t.entry else "—"
    stop_text = f"${t.stop:.1f}" if t.stop else "—"
    target_text = f"${t.target:.1f}" if t.target else "—"

    st.markdown(f"""
    <div class="trade-box {css}">
        <div style="font-size:15px; font-weight:700; margin-bottom:8px;">
            {emoji} TRADE: {t.bias}
        </div>
        <div style="display:flex; gap:20px; font-size:14px; font-weight:600;">
            <span>Entry: <span style="color:#90caf9;">{entry_text}</span></span>
            <span>Stop: <span style="color:#ff4d6a;">{stop_text}</span></span>
            <span>Target: <span style="color:#00d4aa;">{target_text}</span></span>
        </div>
        <div style="font-size:12px; color:#6b7a99; margin-top:6px;">{t.reason}</div>
    </div>
    """, unsafe_allow_html=True)


def render_uoa_flags(signals: SignalSnapshot):
    """Render unusual options activity flags."""
    if not signals.uoa_flags:
        st.markdown('<div style="color:#6b7a99; font-size:13px;">No unusual activity detected</div>',
                    unsafe_allow_html=True)
        return

    for flag in signals.uoa_flags[:5]:
        color = "#ff4d6a" if flag.opt_type == "CALL" else "#00d4aa"
        st.markdown(f"""
        <div class="alert-item alert-warning">
            <span style="color:{color}; font-weight:700;">{flag.strike} {flag.opt_type}</span>
            — Vol: {flag.volume:,} | OI: {flag.oi:,} |
            <span style="font-weight:700;">V/OI: {flag.voi_ratio:.1f}x</span>
        </div>
        """, unsafe_allow_html=True)


def render_game_plan(signals: SignalSnapshot):
    """Render auto-generated game plan for pre-session."""
    parts = []

    if signals.spot:
        parts.append(f"QQQ at **${signals.spot:.2f}**.")

    if signals.gex and signals.gex.flip_point:
        pos = "above" if signals.spot >= signals.gex.flip_point else "below"
        parts.append(f"Price is {pos} GEX flip (${signals.gex.flip_point:.1f}).")

    if signals.gex:
        if signals.gex.regime == "positive_gamma":
            parts.append("**Mean-reversion** environment — fade spikes, expect tight range.")
        elif signals.gex.regime == "negative_gamma":
            parts.append("**Trend** environment — ride momentum, expect wide range.")

    if signals.oi_walls and signals.oi_walls.put_wall and signals.oi_walls.call_wall:
        parts.append(
            f"Expected range: ${signals.oi_walls.put_wall:.0f} – ${signals.oi_walls.call_wall:.0f}.")

    if signals.max_pain:
        direction = "upward drift" if signals.spot < signals.max_pain else "downward drift"
        parts.append(f"Max pain at ${signals.max_pain:.0f} → {direction} expected.")

    if signals.iv and signals.iv.atm_iv:
        if signals.iv.atm_iv > 25:
            parts.append(f"IV elevated ({signals.iv.atm_iv:.1f}%) — use wider stops, bigger targets.")
        elif signals.iv.atm_iv < 15:
            parts.append(f"IV low ({signals.iv.atm_iv:.1f}%) — tight stops, small targets.")
        else:
            parts.append(f"IV normal ({signals.iv.atm_iv:.1f}%) — standard position sizing.")

    if parts:
        st.markdown(" ".join(parts))
    else:
        st.info("Insufficient data for game plan.")


# Main Views 

def view_pre_session():
    """Pre-session view using 1DTE data."""
    st.markdown("### 🌙 Pre-Session — Tomorrow's Setup")

    df_1dte = load_latest_data("1dte")

    if df_1dte.empty:
        st.warning("No 1DTE data available. Run the fetcher with `--1dte` or wait for auto-fetch near close.")
        return

    signals = compute_all_signals(df_1dte)

    if not signals.spot:
        st.error("No valid data in 1DTE snapshot.")
        return

    # Header info
    exp = df_1dte['expiration'].iloc[0] if 'expiration' in df_1dte.columns else "—"
    st.caption(f"Expiration: {exp} | Last Updated: {signals.timestamp}")

    # Regime Banner
    render_regime_banner(signals)

    # Bias + Game Plan
    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown("#### 📋 Tomorrow's Bias")
        render_confluence(signals)
        render_trade_suggestion(signals)

    with col2:
        st.markdown("#### 🗺️ Game Plan")
        render_game_plan(signals)
        st.markdown("---")
        st.markdown("#### 📍 Key Levels")
        render_key_levels(signals)

    st.markdown("---")

    # Charts
    col_gex, col_oi = st.columns(2)
    with col_gex:
        st.plotly_chart(chart_gex_profile(signals), width='stretch', key="pre_gex")
    with col_oi:
        st.plotly_chart(chart_oi_distribution(df_1dte, signals), width='stretch', key="pre_oi")

    # IV Skew
    st.plotly_chart(chart_iv_skew(df_1dte, signals), width='stretch', key="pre_iv")

    # Confluence breakdown
    with st.expander("📊 Signal Breakdown"):
        render_confluence_breakdown(signals)


def view_live_session():
    """Live session view using 0DTE data."""
    st.markdown("### 🔴 Live Session — 0DTE Signals")

    df_0dte = load_latest_data("0dte")

    if df_0dte.empty:
        st.warning("No 0DTE data available. Start the fetcher: `python fetch_qqq_live.py --loop 60`")
        return

    signals = compute_all_signals(df_0dte)

    if not signals.spot:
        st.error("No valid data in 0DTE snapshot.")
        return

    # Header
    now = get_ny_time()
    poll_status = f"Last Poll: {signals.timestamp}"
    st.caption(f"QQQ: ${signals.spot:.2f} | {now.strftime('%H:%M:%S')} ET | {poll_status}")

    # Regime Banner
    render_regime_banner(signals)

    # Top row: Confluence + Key Levels + Trade
    col1, col2, col3 = st.columns([2, 1, 2])

    with col1:
        render_confluence(signals)
        render_trade_suggestion(signals)

    with col2:
        st.markdown("##### 📍 Levels")
        render_key_levels(signals)

    with col3:
        # Signal gauges
        st.markdown("##### 📡 Signals")
        g1, g2 = st.columns(2)
        with g1:
            gex_flip = f"${signals.gex.flip_point:.1f}" if signals.gex and signals.gex.flip_point else "—"
            st.metric("GEX Flip", gex_flip)

            pc_vol = f"{signals.pc_ratios.volume_pc:.2f}" if signals.pc_ratios and signals.pc_ratios.volume_pc else "—"
            st.metric("P/C Volume", pc_vol)

            iv_val = f"{signals.iv.atm_iv:.1f}%" if signals.iv and signals.iv.atm_iv else "—"
            st.metric("ATM IV", iv_val)

        with g2:
            gex_d = signals.gex.total_gex_dollar if signals.gex else 0
            gex_label = f"${gex_d/1e6:.1f}M" if abs(gex_d) >= 1e6 else f"${gex_d/1e3:.0f}K"
            st.metric("GEX$", gex_label)

            skew = f"{signals.iv.iv_skew:+.1f}" if signals.iv and signals.iv.iv_skew else "—"
            st.metric("IV Skew", skew)

            charm = signals.flows.charm_bias.upper() if signals.flows else "—"
            st.metric("Charm", f"↑ {charm}" if charm == "BUY" else f"↓ {charm}")

    st.markdown("---")

    # Charts row
    col_gex, col_oi = st.columns(2)
    with col_gex:
        st.plotly_chart(chart_gex_profile(signals), width='stretch', key="live_gex")
    with col_oi:
        st.plotly_chart(chart_oi_distribution(df_0dte, signals), width='stretch', key="live_oi")

    # IV Skew + Volume Heatmap
    col_iv, col_vol = st.columns(2)
    with col_iv:
        st.plotly_chart(chart_iv_skew(df_0dte, signals), width='stretch', key="live_iv")
    with col_vol:
        st.plotly_chart(chart_volume_heatmap(df_0dte), width='stretch', key="live_vol_heatmap")

    # Signal evolution (time-series sparklines)
    history = compute_signal_history(df_0dte)
    if len(history) > 1:
        st.markdown("#### 📈 Signal Evolution")
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            st.plotly_chart(chart_signal_history(history, "gex_flip", "GEX Flip"),
                            width='stretch', key="hist_gex_flip")
        with sc2:
            st.plotly_chart(chart_signal_history(history, "pc_volume", "P/C Ratio"),
                            width='stretch', key="hist_pc_vol")
        with sc3:
            st.plotly_chart(chart_signal_history(history, "atm_iv", "ATM IV"),
                            width='stretch', key="hist_atm_iv")

        sc4, sc5, sc6 = st.columns(3)
        with sc4:
            st.plotly_chart(chart_signal_history(history, "net_delta", "Net Delta"),
                            width='stretch', key="hist_net_delta")
        with sc5:
            st.plotly_chart(chart_signal_history(history, "gex_dollar", "GEX$"),
                            width='stretch', key="hist_gex_dollar")
        with sc6:
            st.plotly_chart(chart_signal_history(history, "confluence", "Confluence"),
                            width='stretch', key="hist_confluence")

    # Bottom sections
    col_uoa, col_breakdown = st.columns(2)
    with col_uoa:
        st.markdown("##### ⚠️ Unusual Activity")
        render_uoa_flags(signals)

    with col_breakdown:
        st.markdown("##### 📊 Signal Breakdown")
        render_confluence_breakdown(signals)


# Main App
def main():
    inject_css()

    # Title
    st.markdown("""
    <div style="display:flex; align-items:center; gap:10px; margin-bottom:4px;">
        <span style="font-size:28px;">📊</span>
        <span style="font-size:24px; font-weight:800; color:#e8ecf4;">QQQ Options Signals</span>
    </div>
    """, unsafe_allow_html=True)

    # Auto-detect mode
    market_open = is_market_open()

    # Tab selection
    tab_pre, tab_live, tab_history = st.tabs(["🌙 Pre-Session", "🔴 Live Session", "📜 History"])

    with tab_pre:
        view_pre_session()

    with tab_live:
        view_live_session()
        if market_open:
            st.markdown(f"""
            <div style="text-align:center; color:#6b7a99; font-size:12px; margin-top:20px;">
                Auto-refreshes every {POLL_INTERVAL}s during market hours
            </div>
            """, unsafe_allow_html=True)

    with tab_history:
        st.markdown("### 📜 Historical Data")
        dates = get_available_dates()
        if dates:
            selected_date = st.selectbox("Select Date", dates)
            if selected_date:
                hist_df = load_data_for_date(selected_date, "0dte")
                if not hist_df.empty:
                    hist_signals = compute_all_signals(hist_df)
                    render_regime_banner(hist_signals)
                    render_confluence(hist_signals)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.plotly_chart(chart_gex_profile(hist_signals), width='stretch', key="tab_hist_gex")
                    with col2:
                        st.plotly_chart(chart_oi_distribution(hist_df, hist_signals), width='stretch', key="tab_hist_oi")

                    st.plotly_chart(chart_iv_skew(hist_df, hist_signals), width='stretch', key="tab_hist_iv")

                    with st.expander("📊 Raw Data"):
                        st.dataframe(hist_df)
                else:
                    st.info("No 0DTE data for this date.")
        else:
            st.info("No historical data yet. Run the fetcher to start collecting data.")

    # Auto-refresh during market hours
    if market_open:
        import time
        time.sleep(POLL_INTERVAL)
        st.rerun()


if __name__ == "__main__":
    main()
