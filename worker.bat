@echo off
cd /d D:\apps_nostock

echo === AUTO COMMIT ^& PUSH ===
git add .
git commit -m "auto deploy"
git push

echo === DEPLOY TO VPS (SAFE MODE) ===

start "" cmd /k ssh root@162.43.15.160 -t "pkill -f scrape_worker || true; cd /opt/apps_nostock && git reset --hard && git pull && python3 -m apps.inventory.scrape_worker"

start "" cmd /k ssh root@162.43.29.154 -t "pkill -f scrape_worker || true; cd /opt/apps_nostock && git reset --hard && git pull && python3 -m apps.inventory.scrape_worker"

start "" cmd /k ssh root@162.43.42.135 -t "pkill -f scrape_worker || true; cd /opt/apps_nostock && git reset --hard && git pull && python3 -m apps.inventory.scrape_worker"

echo === START LOCAL WORKER (SAFE) ===
powershell -Command "Get-Process python -ErrorAction SilentlyContinue | Where-Object {$_.Path -like '*apps_nostock*'} | Stop-Process -Force"

start "" powershell -NoExit -Command "cd 'D:\apps_nostock'; python -m apps.inventory.scrape_worker"

pause