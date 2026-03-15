# Options-Flow-Tracker

> Built for personal use. Live on VPS, streaming QQQ 0DTE/1DTE data via Tastytrade API.

A fully autonomous, time-aware 0DTE and 1DTE options tracker for QQQ. It streams live options data from Tastytrade, calculates 10+ advanced greek and flow signals, presents them on a mobile-friendly Streamlit dashboard, and fires Telegram alerts when actionable setups occur.

## Features

*   **Live Data Fetching:** Streams 0DTE options data every 60 seconds (or custom interval) during market hours.
*   **Time-Aware Daemon:** Automatically sleeps on nights and weekends, wakes up at market open (9:30 AM ET), and fetches 1DTE prep data in the final hour of trading (3:00 - 4:00 PM ET).
*   **Advanced Signals Engine (`signals.py`):**
    *   **GEX Profile & Flip Point:** Locates the exact price where dealers flip from positive to negative gamma (nearest to spot).
    *   **OI Walls:** Identifies Call and Put walls to define trading ranges.
    *   **Max Pain & IV Skew.**
    *   **Volume Flow Heatmap:** Tracks intraday volume deltas across ATM ±5 strikes.
    *   **Unusual Options Activity (UOA):** Flags strikes with abnormal Volume/OI ratios.
    *   **Confluence Scoring:** Weights signals to output a net LONG/SHORT/NEUTRAL bias along with automated Trade Suggestions (Entry, Stop, Target).
*   **Dual View Dashboard (`dashboard.py`):** 
    *   **Pre-Session:** Game plan for tomorrow (based on 1DTE data).
    *   **Live Session:** Real-time gauges, charts (GEX, OI, IV Skew, Volume Flow), and signal evolution sparklines.
*   **Telegram Alerts (`alerts.py`):** Real-time notifications for Regime Changes, Wall Breaks, Confluence Shifts, IV Spikes, and UOA.

## Directory Structure

```text
options/
├── fetch_qqq_live.py       # Main data scraper and daemon runner
├── signals.py              # Advanced options math and signal generation
├── dashboard.py            # Streamlit UI
├── alerts.py               # Telegram bot integration
├── data/
│   └── YYYY-MM-DD/         # Daily Parquet files (0dte.parquet, 1dte.parquet)
├── deploy/                 # VPS installation scripts and systemd services
├── .env                    # API credentials (Tastytrade & Telegram)
└── requirements.txt        # Python dependencies
```

## Setup & Installation (Local)

1. Clone or download the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file in the root directory:
   ```env
   SECRET=your_tastytrade_api_secret
   refresh=your_tastytrade_refresh_token
   TELEGRAM_TOKEN=your_telegram_bot_token
   TELEGRAM_CHAT_ID=your_telegram_chat_id
   ```

## Usage (Local)

**1. Run the Fetcher (Data Collection)**
```bash
# Poll every 60 seconds
python fetch_qqq_live.py --loop 60

# Or run in Daemon mode (handles open/close times automatically)
python fetch_qqq_live.py --daemon
```

**2. Run the Dashboard**
```bash
streamlit run dashboard.py
```

## VPS Deployment (Ubuntu)

This project is built to run 24/7 on a VPS. Once deployed, the fetcher runs in the background and automatically manages itself based on New York market hours.

1. Create a VPS on the US East Coast (for lowest latency to Tastytrade).
2. SSH into your VPS and copy this folder over.
3. Run the automated installer:
   ```bash
   sudo bash deploy/install.sh
   ```
4. Access your live dashboard by visiting `http://<YOUR_VPS_IP>` in your browser.

**Helpful VPS Commands:**
*   Check Fetcher Logs: `sudo journalctl -u options-fetcher -f`
*   Check Dashboard Logs: `sudo journalctl -u options-dashboard -f`
*   Restart Services: `sudo systemctl restart options-fetcher options-dashboard`

## Understanding the Signals

*   **Positive Gamma Regime:** Dealers buy dips and sell rips. Expect tight, choppy, mean-reverting price action. **Fade the edges.**
*   **Negative Gamma Regime:** Dealers sell into selling and buy into buying, expanding volatility. Expect large directional moves. **Ride the momentum.**
*   **GEX Flip:** The "magnet" and pivot line separating Positive and Negative Gamma. Price frequently gravitates here.
*   **Put/Call Walls:** Areas of massive open interest. Often act as hard support (Put) and resistance (Call). If a wall breaks, expect extreme acceleration.
*   **Confluence Score:** A weighted snapshot of all active signals at that specific minute, providing an aggregated directional bias.
