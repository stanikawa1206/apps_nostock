@echo off
rem Task Scheduler: run publish_daily.py every day at 03:00 JST
cd /d D:\apps_nostock
python -m apps.publish.publish_daily
