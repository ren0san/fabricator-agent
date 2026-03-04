# fabricator-agent

Minimal remote agent used by Fabricator core.

## Complete Install (Ubuntu)

```bash
# 1) Build package
sudo apt update
sudo apt install -y git build-essential debhelper dh-python python3 python3-venv
cd /root
git clone https://github.com/ren0san/fabricator-agent.git
cd fabricator-agent
dpkg-buildpackage -us -uc -b

# 2) Install package
cd ..
sudo apt install -y ./fabricator-agent_0.1.0-1_all.deb

# 3) Configure runtime env
sudo tee /etc/default/fabricator-agent >/dev/null <<'EOF'
AGENT_BACKEND_URL=https://api.thun-der.ru
AGENT_HTTP_PORT=8010
AGENT_LOCAL_API_URL=http://127.0.0.1:8000
AGENT_TEST_MODE=0

# Optional secure auto-bind flow
AGENT_BOOTSTRAP_TOKEN=change_me
AGENT_SLUG=$(hostname -s)

# Optional local diagnostic endpoint protection
AGENT_ADMIN_TOKEN=change_me
EOF

# 4) Start service
sudo systemctl daemon-reload
sudo systemctl enable --now fabricator-agent
sudo systemctl restart fabricator-agent

# 5) Verify
systemctl status fabricator-agent --no-pager
curl -s http://127.0.0.1:8010/health
curl -s http://127.0.0.1:8010/status
```

## Complete Update (Ubuntu)

```bash
# Option A: remote self-update from Fabricator core
# Queue instruction kind: self-update-agent to target agent.
# Agent executes by default:
#   apt-get update && apt-get install -y --only-upgrade fabricator-agent
# and schedules service restart.

# Option B: manual update from repository
sudo apt update
sudo apt install -y git build-essential debhelper dh-python python3 python3-venv
cd /root
if [ ! -d fabricator-agent/.git ]; then
  git clone https://github.com/ren0san/fabricator-agent.git
fi
cd fabricator-agent
git fetch --all --prune
git checkout main
git pull --ff-only origin main
dpkg-buildpackage -us -uc -b
cd ..
sudo apt install -y ./fabricator-agent_0.1.0-1_all.deb
sudo systemctl daemon-reload
sudo systemctl restart fabricator-agent
systemctl status fabricator-agent --no-pager
```

## Complete Uninstall (Ubuntu)

```bash
# 1) Stop and disable service
sudo systemctl stop fabricator-agent || true
sudo systemctl disable fabricator-agent || true

# 2) Remove package
sudo apt purge -y fabricator-agent
sudo apt autoremove -y

# 3) Remove leftover files
sudo rm -rf /opt/fabricator-agent
sudo rm -f /etc/default/fabricator-agent
sudo rm -f /etc/fabricator-agent/config.toml
sudo rm -f /lib/systemd/system/fabricator-agent.service

# 4) Reload systemd
sudo systemctl daemon-reload
sudo systemctl reset-failed
systemctl status fabricator-agent --no-pager || true
```
