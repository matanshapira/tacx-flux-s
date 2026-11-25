@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"
set "PORT=8765"

where python >nul 2>&1
if errorlevel 1 (
    where py >nul 2>&1
    if errorlevel 1 (
        echo Python was not found in PATH.
        echo Please install Python 3 from https://www.python.org/ and try again.
        pause
        exit /b 1
    )
    set "PY=py -3"
) else (
    set "PY=python"
)

%PY% -c "import bleak, aiohttp" >nul 2>&1
if errorlevel 1 (
    echo Installing Bluetooth bridge dependencies ^(bleak, aiohttp^)...
    %PY% -m pip install --user bleak aiohttp
    %PY% -c "import bleak, aiohttp" >nul 2>&1
    if errorlevel 1 (
        echo.
        echo Could not install bleak/aiohttp. Falling back to local HTTP server + Edge Web Bluetooth...
        start "" msedge "http://127.0.0.1:%PORT%/flux-s.html"
        %PY% -m http.server %PORT% --bind 127.0.0.1
        goto :eof
    )
)

%PY% "%~dp0flux_bridge.py" %*
if errorlevel 1 (
    echo.
    pause
)

