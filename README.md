# UBi-Guardian — Software Stack

Smart edge-AI guardian that senses risks in small aquatic habitats and restores balance with gentle air bursts.  
This repo contains all software pieces: ESP32 firmware, data collector, alerts, dashboard and the ML model assets.

---

## 1) Repo layout

```
/firmware/
  esp32s3.ino                # main firmware (ESP32-S3, Arduino)
  model_data.h               # TFLite model as a C array (included by firmware)
  model_data.cpp             # separate TU
  esp32s3_ripple_classifier.keras # model
  esp32s3_ripple_classifier.tflite # model
/ml/
  train_model.ipynb          # training model
  data_preprocessing.py      # prepare data to train
/server/
  main.py                    # FastAPI collector + CSV logging + Discord alerts
  data_preprocessing.py      # one time script
  Data_collector_phase_1.py  # early phase script for data collector
  dashboard.html             # zero-dependency, opens in a browser
  data/                      # dataset
    telemetry.csv            # timeseries real recorded data
    events.csv               # alerts & pump recommendations
    telemetry.ndjson         # raw backup
    training.csv             # train dataset
```

---

## 2) Quick start

### A) Collector + Alerts (FastAPI)
Requirements: Python 3.9+.

```bash
cd server
python -m venv .venv
source .venv/bin/activate
pip install fastapi uvicorn requests
```

Discord alerts:
- The server already includes a **built-in default webhook URL** in `main.py` (you can replace the `DEFAULT_WEBHOOK` string).
- Alternatively set an env var to override at runtime:
  ```bash
  export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/XXX/YYY"
  ```

Run:
```bash
uvicorn main:app --host 0.0.0.0 --port 5001
```

You now have:
- POST `http://<server>:5001/ingest`  # device posts telemetry here
- GET  `/health`, `/latest`, `/export.csv`, `/events.csv`
- POST `/alert/test` (sends a demo alert to Discord)
- POST `/alert/webhook` (persist a new webhook if no env var is set)

> Data files are written to `server/data/` (created automatically).

---

### B) Firmware (ESP32-S3, Arduino)
1. Open **Arduino IDE** (2.x), install:
   - **ESP32** board support, select an **ESP32-S3** dev board.
   - Libraries: `OneWire`, `DallasTemperature`, `Adafruit AHTX0`, `Adafruit VEML7700`, `Adafruit MLX90614`, `Adafruit BMP085/BMP180`, `ESPmDNS`, `Preferences`.

2. Pins (as in the code):
   - I²C: `SDA=8`, `SCL=9`
   - I²S mic (INMP441): `BCLK=18`, `LRCL=17`, `DIN=16`, `SAMPLE_RATE=16000`
   - Relay: `GPIO15`
   - OneWire DS18B20: `GPIO6`
   - TDS ADC pin: `GPIO4`

3. ML model embedding (already provided):
   - We ship `model_data.h/.cpp` with `esp32s3_ripple_classifier_tflite` byte array.
   - If you retrain: regenerate the header from the `.tflite`:
     ```bash
     xxd -i esp32s3_ripple_classifier.tflite > firmware/model_data.h
     ```
     (Keep the exported symbol name unchanged.)

4. Flash `esp32s3.ino`.

#### Device web API (on the ESP32)
- If it can’t join Wi-Fi, it opens AP **`UBiGuardian-Setup`** (captive portal at `192.168.4.1`) to save your SSID/password.
- In STA mode:
  - `GET /` # live status page
  - `GET /sensors` # telemetry JSON snapshot
  - `GET /ml`, `/caps`, `/health`
  - `POST /pump?on=1&sec=10` (manual pump control)
  - `POST /collector?host=<RaspberryPi_IP>&port=5001&path=/ingest`  
    **Important:** point the device to your server’s IP/port/path so it can POST telemetry.

The device posts once per second; JSON fields match the CSV headers listed below.

---

### C) Dashboard
- Open `dashboard/dashboard.html` directly in your browser (no server needed).
- In the top-right **Base** box, enter your collector base (if not default) and click **Save**.  
  The dashboard will persist this and your chosen **time range** in `localStorage`.
- Panels:
  - **System Snapshot**: shows current state. The **Alert** tile turns **red** for 5 seconds after any alert, **green** otherwise.
  - Charts: **C\* DO**, **micRMS**, **Lux**, **TDS** (auto-downsampled for performance).
  - **All Sensors**: key–value card grid.
  - **Recent Events**: table of alerts and recommendations.
- CSV exports: **Export CSV** and **Events CSV** buttons.

---

## 3) Data schema

Every /ingest POST (and CSV row) uses these keys:

```
ts (epoch seconds) | ms (device ms) | pump | manual_override | alert | reason | context
rec_ms | tTop | tMid | tBot | dT_tb | pressure_hPa | lux | irObj | irAmb
airT | airRH | tds_mV | tds_sat | micRMS | DOproxy | ml_on | ml_pred | ml_conf | ml_used
```

- The collector coerces types, appends a line to `telemetry.csv`, and—if `alert==true` **or** `rec_ms>0`—to `events.csv`.
- Discord messages are compact tables with the most relevant fields (deduped for 60s to avoid spam).

**Example POST to test without hardware:**
```bash
curl -X POST http://<server>:5001/ingest   -H "content-type: application/json"   -d '{
    "ts": '"$(date +%s)"',
    "pump": false, "manual_override": false,
    "alert": true, "reason": "demo_alert", "context": "day",
    "rec_ms": 0,
    "tTop": 23.6, "tMid": 23.5, "tBot": 23.4, "dT_tb": 0.2,
    "pressure_hPa": 1007.8, "lux": 120.0, "micRMS": 3.2,
    "tds_mV": 420, "tds_sat": false, "DOproxy": 6.9,
    "ml_on": true, "ml_pred": "disturbance", "ml_conf": 0.91, "ml_used": true
  }'
```

To send a demo alert embed to Discord:
```bash
curl -X POST http://<server>:5001/alert/test
```

---

## 4) Machine learning model

- Notebook: `ml/train_model.ipynb` trains a multi-class classifier on features:
  `micRMS, lux, ΔT (top–bot), ΔT over 60s at mid, DO* proxy, ΔTDS`.
- Export to TFLite: `esp32s3_ripple_classifier.tflite`.
- Embedded in firmware via `model_data.h` as `esp32s3_ripple_classifier_tflite`.
- Interpreter: TFLM with ops resolver (FullyConnected, Reshape, Softmax, Quantize, Dequantize).  
  Input accepts float/quantized; output is class scores for:
  ```
  calm, cold-shock, cooling-hot, disturbance, flashlight-night,
  glare, human-tap, manual-override, other, pump-self, tds-spike, uniform-overheat
  ```

---

## 5) Operating logic (firmware summary)

- **Sensors**: 3× DS18B20 (top/mid/bot), AHT (air T/RH), VEML7700 (lux), MLX90614 (IR), BMP180/085 (pressure), INMP441 I²S mic (RMS), TDS analog.
- **Derived signals**: calm baseline (micRMS), stratification/inversion, DO* proxy (temp+pressure), barometric drop, flashlight/night spikes, TDS spikes, tap/disturbance timing windows.
- **Pump policy**:
  - Gentle bursts at night if water is calm and DO* borderline.
  - Stronger/longer bursts for stratification, inversion, or overheating.
  - Caps: per-hour and per-night duty.
  - Blockers: sensor fault, heater lamp glare/hot IR, abrupt darkness, cold shock, TDS spike, human tap, pump self-masking.
- **ML gate** (optional): if `cfg.ml_gate==true` the model can override/confirm ripple/tap/flashlight detection; result is recorded in telemetry.

---

## 6) Configuration

Runtime config is persisted in ESP32 `Preferences` and exposed via HTTP:

- `GET /config/get` # JSON
- `POST /config/set?key=<k>&val=<v>` # update a single key  
  Keys include: `do_lo`, `do_hi`, `night_lux`, `glare_lux`, `dt_strat`, `dt_inv`,  
  `overheat_un`, `cap_hour_ms`, `cap_night_ms`, `cool_on_c`, `cool_off_c`, `cool_burst_ms`,  
  `day_on_factor`, `sudden_light_factor`, `sudden_dark_factor`, `sudden_window_ms`,  
  `sunrise_grace_ms`, `ml_gate`.

DS18B20 mapping:
- `GET /dsmap` view known addresses;  
- `POST /dsmap/byindex?top=0&mid=1&bot=2` or `/dsmap/setaddr?top=<hex16>&mid=<hex16>&bot=<hex16>`.

---

## 7) Dashboard tips

- Use the **Range** control to pick `15m to 30d` or **Custom** (From/To).
- The selection **persists** across refreshes.
- Alert tile stays **red for 5 seconds** after any alert; otherwise green.

---

## 8) Deployment notes

- Put the collector on a stable host (Raspberry Pi, mini-PC, VM).  
- Keep the ESP32 and server on the same network or reachable over UDP-to-TCP NATs.
- If you rotate the Discord webhook, either update `DEFAULT_WEBHOOK` in `main.py` or run with the `DISCORD_WEBHOOK_URL` env var.

---

## 9) Troubleshooting

- **No data in dashboard:** confirm the **Base** URL points to the collector and `/export.csv` returns a file.  
- **Device not posting:** visit the device IP  `/` works? Then `POST /collector?host=<server_ip>&port=5001&path=/ingest`.  
- **Bad sensor values:** `/sensors` on the device shows raw readings; check wiring and I²C addresses.

---

## 10) License & attribution

Open source for non-commercial research and conservation use.  
“UBi-Guardian” software © the project team.  

Designed by the Edge Computing research team, supervised by Dr. Atakan Aral.

example of our data ingest: curl "http://ubiguard.local/collector?host=10.10.216.221&port=5001&path=/ingest"# UBi_Guardian
