@echo off
cmd /k ssh -tt root@162.43.42.135 "cd /opt/apps_nostock && git pull && bash -l"
