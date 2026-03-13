#!/bin/bash
# VPS Setup Script for QQQ Options Dashboard
# Run this script with: sudo bash deploy/install.sh

set -e

APP_DIR="/opt/qqq-options"

echo "🚀 Starting QQQ Options deployment setup..."

# 1. Update and install dependencies
echo "📦 Installing system packages..."
apt-get update
apt-get install -y python3 python3-pip python3-venv

# 2. Setup application directory
echo "📁 Setting up application directory ($APP_DIR)..."
mkdir -p $APP_DIR
cp -a . $APP_DIR/
chown -R root:root $APP_DIR

# 3. Setup Python Virtual Environment
echo "🐍 Setting up Python virtual environment..."
cd $APP_DIR
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt

# 4. Install Systemd Services
echo "⚙️ Installing systemd services..."
cp deploy/options-fetcher.service /etc/systemd/system/
cp deploy/options-dashboard.service /etc/systemd/system/

systemctl daemon-reload
systemctl enable options-fetcher
systemctl enable options-dashboard

# 5. Start Services
echo "▶️ Starting services..."
systemctl restart options-fetcher
systemctl restart options-dashboard

echo "✅ Setup complete! Both the fetcher and dashboard are now running."
echo ""
echo "You can view the dashboard by visiting: http://<YOUR_VPS_IP>"
echo ""
echo "To check the logs, use:"
echo "  sudo journalctl -u options-fetcher -f"
echo "  sudo journalctl -u options-dashboard -f"
