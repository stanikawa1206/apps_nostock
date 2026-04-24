#!/bin/bash

cd /opt/apps_nostock || exit 1

export WORKER_STATUS_DIR=/opt/apps_nostock

python3 -u -m apps.inventory.watchdog