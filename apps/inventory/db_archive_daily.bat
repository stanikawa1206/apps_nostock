@echo off
rem Task Scheduler: nostock_db_archive_daily, daily 23:00 (purge old trx.scrape_job, archive trx.vendor_item; stops at 02:30)
cd /d D:\apps_nostock
set PYTHONIOENCODING=utf-8
if not exist logs\db_archive mkdir logs\db_archive
python -m apps.inventory.db_archive_daily >> logs\db_archive\task_stdout.log 2>&1
