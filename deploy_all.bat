@echo off
setlocal EnableExtensions

cd /d D:\apps_nostock
if errorlevel 1 goto :err_cd

echo ==========================================
echo  PRE-DEPLOY SAFETY CHECKS
echo ==========================================

echo.
echo --- STEP 1/5: CHECK CURRENT BRANCH ---
set "BRANCH="
for /f "delims=" %%i in ('git rev-parse --abbrev-ref HEAD') do set "BRANCH=%%i"
if not defined BRANCH goto :err_branch_read
if not "%BRANCH%"=="main" goto :err_not_main
echo   current branch = %BRANCH%   [OK]

echo.
echo --- STEP 2/5: GIT ADD ---
git add .
if errorlevel 1 goto :err_add
echo   git add .   [OK]

echo.
echo --- STEP 3/5: GIT COMMIT ---
git diff --cached --quiet
if errorlevel 1 goto :do_commit
echo   no staged changes - nothing to commit (treated as success)   [OK]
goto :after_commit

:do_commit
git commit -m "auto deploy"
if errorlevel 1 goto :err_commit
echo   git commit   [OK]

:after_commit

echo.
echo --- STEP 4/5: GIT PUSH origin main ---
git push origin main
if errorlevel 1 goto :err_push
echo   git push origin main   [OK]

echo.
echo --- STEP 5/5: VERIFY local main == origin/main ---
set "LOCAL_SHA="
set "REMOTE_SHA="
for /f "delims=" %%i in ('git rev-parse main') do set "LOCAL_SHA=%%i"
for /f "tokens=1" %%i in ('git ls-remote origin refs/heads/main') do set "REMOTE_SHA=%%i"
if not defined LOCAL_SHA goto :err_verify_read
if not defined REMOTE_SHA goto :err_verify_read
echo   local  main = %LOCAL_SHA%
echo   origin main = %REMOTE_SHA%
if not "%LOCAL_SHA%"=="%REMOTE_SHA%" goto :err_mismatch
echo   local main matches origin/main   [OK]

echo.
echo ==========================================
echo  ALL CHECKS PASSED - DEPLOY TO VPS
echo ==========================================

start "" cmd /k ssh root@162.43.15.160 -t "cd /opt/apps_nostock && git reset --hard && git pull; bash"
start "" cmd /k ssh root@162.43.29.154 -t "cd /opt/apps_nostock && git reset --hard && git pull; bash"
start "" cmd /k ssh root@162.43.42.135 -t "cd /opt/apps_nostock && git reset --hard && git pull; bash"
start "" cmd /k ssh root@210.131.209.103 -t "cd /opt/apps_nostock && git reset --hard && git pull; bash"
start "" cmd /k ssh root@162.43.39.209 -t "cd /opt/apps_nostock && git reset --hard && git pull; bash"
start "" cmd /k ssh root@85.131.251.127 -t "cd /opt/apps_nostock && git reset --hard && git pull; bash"
start "" cmd /k ssh root@210.131.209.232 -t "cd /opt/apps_nostock && git reset --hard && git pull; bash"

echo.
echo Deploy windows launched for 7 VPS hosts.
pause
exit /b 0


:err_cd
echo.
echo [ERROR] STEP 0: failed to change directory to D:\apps_nostock
echo         DEPLOY ABORTED. No VPS was contacted.
pause
exit /b 1

:err_branch_read
echo.
echo [ERROR] STEP 1: failed to read current git branch.
echo         Is this a git repository? Is git on PATH?
echo         DEPLOY ABORTED. No VPS was contacted.
pause
exit /b 1

:err_not_main
echo.
echo [ERROR] STEP 1: current branch is "%BRANCH%", not "main".
echo         Deploy is allowed only from main.
echo         Run: git switch main
echo         DEPLOY ABORTED. No VPS was contacted.
pause
exit /b 1

:err_add
echo.
echo [ERROR] STEP 2: git add . failed.
echo         DEPLOY ABORTED. No VPS was contacted.
pause
exit /b 1

:err_commit
echo.
echo [ERROR] STEP 3: git commit failed.
echo         This is a real commit error (NOT the "nothing to commit" case).
echo         Check: git config user.name / user.email, hooks, index state.
echo         DEPLOY ABORTED. No VPS was contacted.
pause
exit /b 1

:err_push
echo.
echo [ERROR] STEP 4: git push origin main failed.
echo         GitHub was NOT updated. Possible causes:
echo           - origin/main has moved ahead (fetch and integrate first)
echo           - authentication failure
echo           - network failure
echo         DEPLOY ABORTED. No VPS was contacted.
pause
exit /b 1

:err_verify_read
echo.
echo [ERROR] STEP 5: failed to read local main or origin/main commit id.
echo         Could not verify that GitHub is up to date.
echo         DEPLOY ABORTED. No VPS was contacted.
pause
exit /b 1

:err_mismatch
echo.
echo [ERROR] STEP 5: local main does not match origin/main.
echo           local  main = %LOCAL_SHA%
echo           origin main = %REMOTE_SHA%
echo         GitHub does not have the expected commit.
echo         DEPLOY ABORTED. No VPS was contacted.
pause
exit /b 1
