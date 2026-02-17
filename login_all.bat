@echo off
echo ===============================
echo 🔐 VPS LOGIN ONLY
echo ===============================

start "VPS_160" cmd /k ssh -tt root@162.43.15.160
start "VPS_154" cmd /k ssh -tt root@162.43.29.154
start "VPS_135" cmd /k ssh -tt root@162.43.42.135

echo ===============================
echo ✅ LOGIN COMPLETE
echo ===============================

pause
