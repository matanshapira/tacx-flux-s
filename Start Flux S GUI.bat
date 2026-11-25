@echo off
REM Starts the Flux S GUI.
REM Preferred: Python Bluetooth bridge (in-app device search, any browser).
REM Fallback:  plain local web server + Edge's built-in Web Bluetooth.
cd /d "%~dp0"
title Flux S GUI

REM ---- Python 3.10+ is needed (the Microsoft Store "python" shortcut does not count) ----
python -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>nul
if errorlevel 1 (
  echo.
  echo  Python 3.10 or newer was not found.
  python --version 2>nul
  echo  Install it from https://www.python.org/downloads/ and tick "Add python.exe to PATH"
  echo  during setup. Then start this file again.
  echo.
  pause
  exit /b 1
)

REM ---- bridge packages, exact tested versions from requirements.txt ----
python -c "import importlib.metadata as m,sys;r=[l.split('==') for l in open('requirements.txt').read().split() if '==' in l];sys.exit(any(m.version(a)!=b for a,b in r))" >nul 2>nul
if errorlevel 1 (
  echo Installing the tested versions of the Bluetooth bridge packages - one time only...
  python -m pip install --user --disable-pip-version-check -r requirements.txt
  if errorlevel 1 echo  Could not install the exact versions - continuing with what is installed.
)

python -c "import bleak, aiohttp" 2>nul
if errorlevel 1 (
  echo Bridge packages unavailable - using the browser's Bluetooth instead.
  echo Settings and rides can't be saved to files in this mode.
  start "Flux S web server - close to stop" /min python -m http.server 8765 --bind 127.0.0.1
  timeout /t 1 >nul
  start msedge "http://localhost:8765/flux-s.html"
  exit /b
)

echo Starting the Flux S Bluetooth bridge. Keep this window open while riding.
python flux_bridge.py
pause
