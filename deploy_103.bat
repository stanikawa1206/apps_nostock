@echo off
cd /d D:\apps_nostock

echo === AUTO COMMIT ^& PUSH ===
git add .
git commit -m "auto deploy"
git push

echo === DEPLOY TO VPS ===
start "" cmd /k ssh root@210.131.209.103 -t "cd /opt/apps_nostock && git reset --hard && git pull; bash"

pause
