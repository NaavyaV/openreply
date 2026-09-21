#!/bin/bash
# Starts the DM worker on an always-on box (Oracle / any VPS).
# Loads /etc/openreply.env because this app does not read a .env file itself.
set -a
source /etc/openreply.env
set +a
cd "$(dirname "$0")/.."
exec npm run worker
