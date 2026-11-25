# Tacx Flux S · Live Control

A standalone web application and Python Bluetooth LE bridge to control and monitor a **Tacx Flux S** smart trainer and Bluetooth heart-rate strap.

Built with plain HTML/CSS/JavaScript (no external dependencies or libraries) and a lightweight Python bridge (`bleak` + `aiohttp`).

---

## Features

- **Direct Web Bluetooth & Python Bridge Support:**
  - Works natively via Web Bluetooth in Chrome / Edge.
  - Or run with `flux_bridge.py` for reliable Windows BLE discovery, RSSI signal indicators, multi-device management, and background connection recovery.
- **FTMS & FE-C Over BLE:**
  - FTMS Indoor Bike Data parsing (power, cadence, speed, resistance, HR).
  - Tacx FE-C over Bluetooth (pages `0x10`, `0x19`, `0x30`, `0x31`, `0x32`, `0x33`, `0x37`).
  - Auto protocol negotiation with manual override (FTMS / FE-C).
- **Heart-Rate Strap Integration:**
  - Standard BLE Heart Rate Service (`0x180D`), battery level indicator (`0x180F`).
  - Automatic fallback to trainer-reported heart rate when no strap is connected.
- **Trainer Control Modes:**
  - **ERG Mode:** Target power (0–1500 W), with % FTP and W/kg calculations.
  - **Slope Mode:** Grade simulation (-10% to +15%), wind speed, Crr, and aerodynamic drag.
  - **Resistance Mode:** Direct brake resistance (0–100%).
- **Interactive Workout Builder & Player:**
  - Profile chart, countdown timer, audio beeps for interval transitions.
  - Full **Excel (.xlsx)** export and import (compatible with Microsoft Excel, built with pure zero-dependency OpenXML zip generator/parser).
  - Also supports JSON and CSV workout formats.
- **Telemetry & Logging:**
  - Real-time rolling chart for Power, Target, Cadence, Speed, and Heart Rate.
  - Session recording with auto-pause and CSV data export.
  - Built-in physics-based **Demo Mode** for testing without physical trainer hardware.

---

## Quick Start for Windows

1. Double-click `Start Flux S GUI.bat`.
2. It will automatically install `bleak` and `aiohttp` (if not already installed) and launch the app in your browser at `http://localhost:8765/flux-s.html`.
3. In the **Devices** panel on the right, click **Search devices**, select your Tacx Flux S (and your HR strap), and click **Connect**.

---

## Quick Start for Mac / Linux

1. Install requirements:
   ```bash
   pip install --user bleak aiohttp
   ```
2. Run the bridge:
   ```bash
   python3 flux_bridge.py
   ```
3. Your browser will automatically open `http://localhost:8765/flux-s.html`.

*(Alternatively, you can open `flux-s.html` directly in Google Chrome or Microsoft Edge and connect using native Web Bluetooth without Python).*

---

## Project Structure

- `flux-s.html`: Single-file web app (GUI, telemetry charts, workout player, XLSX generator/parser).
- `flux_bridge.py`: Local WebSocket BLE bridge using `bleak` and `aiohttp`.
- `Start Flux S GUI.bat`: One-click Windows starter script.
