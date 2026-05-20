@echo off
echo ===============================
echo 🔐 VPS LOGIN ONLY
echo ===============================

start "VPS_160" cmd /k ssh -tt root@162.43.15.160
start "VPS_154" cmd /k ssh -tt root@162.43.29.154
start "VPS_135" cmd /k ssh -tt root@162.43.42.135
start "VPS_127" cmd /k ssh -tt root@85.131.251.127
start "VPS_209" cmd /k ssh -tt root@162.43.39.209
start "VPS_232" cmd /k ssh -tt root@210.131.209.232
start "VPS_103" cmd /k ssh -tt root@210.131.209.103

echo ===============================
echo ✅ LOGIN COMPLETE
echo ===============================

pause
