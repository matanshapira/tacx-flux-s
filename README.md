# Tacx Flux S · Live Control

A standalone training application and Python Bluetooth LE bridge for the **Tacx Flux S** (and other FTMS / Tacx FE-C smart trainers) plus Bluetooth heart-rate straps.

Built with plain HTML/CSS/JavaScript (zero browser libraries) and a lightweight Python bridge (`bleak` + `aiohttp`).

---

## Features

- **Trainer Control Modes:**
  - **ERG Mode:** Target power (0–1500 W), with `% FTP`, Coggan power zones, and `W/kg`.
  - **Slope Mode:** Road grade simulation (−10% to +15%), wind speed, `Crr`, `Cw`, rider & bike weight.
  - **Resistance Mode:** Direct brake resistance (0–100%).
  - **Heart-Rate Control Mode (HR Mode):** Closed-loop ERG power adjustment to hold a target heart rate or HR zone, plus aerobic decoupling tracking.
  - **Listen-Only Mode:** Watch and record rides while Zwift / MyWhoosh / Tacx controls the trainer.
- **Workouts, Library & FTP Tests:**
  - **42 Built-in Workouts** with SVG mini-profiles, favorites, NP / IF / TSS estimates, and one-click load or start.
  - **4 Built-in FTP Tests:** Ramp test (with automatic failure detection), 20-minute test, 2×8-minute test, and Critical Power (CP) test.
  - **Workout Builder & Player:** Live mid-workout edits, countdown beeps, full-screen big-numbers Ride View, and keyboard/numpad shortcuts (`Keys`).
  - **Zero-Library Excel (`.xlsx`), `.csv`, and `.json` Export & Import.**
- **Ride Recording, Analysis & FIT Export:**
  - Live-streamed ride recording (`Rides/*.csv` + `.json` with connection-drop tracking and automatic stale-ride finalization).
  - **Rides History & Analysis modal** (`📊 Rides`) with NP, IF, TSS, charts, and re-export.
  - **Binary `.fit` Export** ready for Garmin Connect, Strava, TrainingPeaks, and intervals.icu.
- **Trainer Calibration & Reliability:**
  - **Spin-down Calibration** (`Calibrate`) with history tracking and periodic reminders.
  - **Connection Health (`📶`) & Watchdog:** Live packet rate, drop counter (`⚡`), 15 s silent-drop reconnect, 6 fast + 10 min slow auto-reconnect, and Windows direct-connect fallback.
  - **Single-Tab Ownership:** Prevents conflicting tabs from fighting over the trainer (`Use this tab instead`), with persistent disk storage (`flux_config.json`, `Workouts/`, `Rides/`, `Logs/flux_bridge.log`).
  - **Demo Mode:** Full physics & HR simulator for testing without hardware.

---

## Quick Start for Windows

1. Install **Python 3.10+** from [python.org](https://www.python.org/downloads/) (tick **"Add python.exe to PATH"**).
2. Double-click **`Start Flux S GUI.bat`**.
   - First run installs the tested bridge dependencies from `requirements.txt` (`bleak==3.1.0`, `aiohttp==3.14.5`).
   - Opens `http://localhost:8765/flux-s.html` in your browser.
3. Keep the black `Flux S GUI` console window open while riding.

---

## Quick Start for Mac / Linux

```bash
pip install --user -r requirements.txt
python3 flux_bridge.py
```

### Bridge CLI Options
```bash
python flux_bridge.py --port 8765 --no-browser --replace \
  --config ./flux_config.json --workouts ./Workouts --rides ./Rides --logs ./Logs
python flux_bridge.py --stop
```

---

## Project Structure

- `flux-s.html`: Single-file web app (GUI, telemetry, 42-workout library, FTP tests, HR control, calibration, ride viewer, XLSX & FIT encoders).
- `flux_bridge.py`: Python WebSocket BLE bridge with file-backed config, rides, workouts, rotating logs, single-tab control, and Windows direct-connect fallback.
- `Start Flux S GUI.bat`: One-click Windows launcher.
- `requirements.txt`: Exact tested Python package versions.
- `tests/`: Independent local end-to-end test suite (`test_bridge.py` + headless Chrome `e2e_browser.mjs`).

---

## Running Local E2E Tests

Run the full zero-dependency Python bridge + real headless Chrome E2E suite (uses temporary directories so your local settings/rides are untouched):

```bash
./tests/run_all.sh
```
