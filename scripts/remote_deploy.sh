#!/usr/bin/env bash
set -euo pipefail

DEPLOY_PATH="${1:-/opt/fabricator-agent-src}"

if [ "$(id -u)" -eq 0 ]; then
  SUDO=""
elif command -v sudo >/dev/null 2>&1; then
  SUDO="sudo -n"
else
  echo "ERROR: run as root or provide passwordless sudo" >&2
  exit 1
fi

run() {
  if [ -n "${SUDO}" ]; then
    ${SUDO} "$@"
  else
    "$@"
  fi
}

cd "${DEPLOY_PATH}"

if command -v apt-get >/dev/null 2>&1; then
  run apt-get update
  run apt-get install -y git build-essential debhelper dh-python python3 python3-venv
fi

dpkg-buildpackage -us -uc -b

DEB_FILE="$(ls -1t ../fabricator-agent_*_all.deb | head -n1)"
if [ -z "${DEB_FILE}" ]; then
  echo "ERROR: built .deb package not found" >&2
  exit 2
fi

# --reinstall allows deploying fixes without bumping version every commit.
run apt-get install -y --reinstall "${DEB_FILE}"

# Ensure runtime mode by default (legacy docs often left AGENT_TEST_MODE=1).
if [ -f /etc/default/fabricator-agent ]; then
  if grep -q '^AGENT_TEST_MODE=' /etc/default/fabricator-agent; then
    run sed -i 's/^AGENT_TEST_MODE=.*/AGENT_TEST_MODE=0/' /etc/default/fabricator-agent
  else
    echo "AGENT_TEST_MODE=0" | run tee -a /etc/default/fabricator-agent >/dev/null
  fi
else
  echo "AGENT_TEST_MODE=0" | run tee /etc/default/fabricator-agent >/dev/null
fi

run systemctl daemon-reload
run systemctl enable --now fabricator-agent.service
run systemctl restart fabricator-agent.service
run systemctl --no-pager --full status fabricator-agent.service || true
