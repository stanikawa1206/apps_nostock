@echo off
cmd /k ssh -tt root@162.43.42.135 "cd /opt/apps_nostock && git reset --hard && git pull && bash -l"
