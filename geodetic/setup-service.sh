#!/bin/bash

# =================================================================
# == Script to automatically create and activate a systemd service for Geodetic Agent ==
# =================================================================

# --- Create the .service file content ---
sudo cat << EOF > /etc/systemd/system/geodetic-agent.service
[Unit]
Description=Geodetic Pi Agent for CORS System
After=network-online.target
Wants=network-online.target

[Service]
User=pi123
Group=pi123
WorkingDirectory=/home/pi123/pi/geodetic
ExecStart=/home/pi123/pi/geodetic/venv/bin/python /home/pi123/pi/geodetic/agent_universal.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# --- Execute terminal commands automatically ---

echo "✓ geodetic-agent.service file has been created."

echo "⏳ Reloading systemd configuration..."
sudo systemctl daemon-reload

echo "🚀 Enabling the service to start on system boot..."
sudo systemctl enable geodetic-agent.service

echo "⚡ Starting/Restarting the service immediately..."
sudo systemctl restart geodetic-agent.service

echo ""
echo "--- DONE! Please check the service status below ---"
sleep 2
sudo systemctl status geodetic-agent.service
