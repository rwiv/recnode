cd ..
set ENV_FILE=.\dev\.env

@echo off
setlocal enabledelayedexpansion

if not exist %ENV_FILE% (
    echo not found env file
    pause
    exit /b 1
)

for /f "usebackq tokens=1,2* delims== " %%a in (%ENV_FILE%) do (
    set "key=%%a"
    set "value=%%b"
    if "!key!" neq "" if "!key:~0,1!" neq "#" (
        setlocal disabledelayedexpansion
        set "value=%%b"
        setlocal enabledelayedexpansion
        set "!key!=!value!"
    )
)

.\.venv\Scripts\python.exe .\main_batch.py
pause