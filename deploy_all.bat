@echo off
echo ===============================
echo 🚀 PUSH & DEPLOY START
echo ===============================

cd /d D:\apps_nostock

echo ---- Git Push ----
git push

echo ---- Launch VPS ----

set CMD=cd /opt/apps_nostock && git reset --hard && git pull && bash -l

start "VPS_160" cmd /k ssh -tt root@162.43.15.160 "%CMD%"
start "VPS_154" cmd /k ssh -tt root@162.43.29.154 "%CMD%"
start "VPS_135" cmd /k ssh -tt root@162.43.42.135 "%CMD%"

echo ===============================
echo ✅ DEPLOY COMPLETE
echo ===============================

pause
