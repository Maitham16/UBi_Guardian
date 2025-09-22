# collector.py
import csv, time, json
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, JSONResponse, FileResponse

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
NDJSON_PATH = DATA_DIR / "telemetry.ndjson"
CSV_PATH = DATA_DIR / "telemetry.csv"
EVENTS_CSV_PATH = DATA_DIR / "events.csv"

CSV_FIELDS = [
    "ts",
    "ms",
    "pump",
    "manual_override",
    "alert",
    "reason",
    "context",
    "rec_ms",
    "tTop",
    "tMid",
    "tBot",
    "dT_tb",
    "pressure_hPa",
    "lux",
    "irObj",
    "irAmb",
    "airT",
    "airRH",
    "tds_mV",
    "tds_sat",
    "micRMS",
    "DOproxy",
    "ml_on",
    "ml_pred",
    "ml_conf",
    "ml_used",
]

ML_EVENT_LABELS = {
    "human-tap",
    "disturbance",
    "flashlight-night",
    "pump-self",
    "glare",
    "cold-shock",
    "tds-spike",
    "uniform-overheat",
    "cooling-hot",
}

app = FastAPI(title="UBi-Guardian Collector")
_last: Optional[Dict[str, Any]] = None


def _append_ndjson(obj: Dict[str, Any]) -> None:
    with NDJSON_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _append_csv(obj: Dict[str, Any]) -> None:
    newfile = not CSV_PATH.exists()
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if newfile:
            w.writeheader()
        row = {k: obj.get(k, "") for k in CSV_FIELDS}
        w.writerow(row)


def _append_events_csv(obj: Dict[str, Any]) -> None:
    alert = bool(obj.get("alert", False))
    try:
        rec_ms = int(obj.get("rec_ms", 0))
    except Exception:
        rec_ms = 0
    ml_used = bool(obj.get("ml_used", False))
    ml_pred = str(obj.get("ml_pred", "")).strip().lower()

    if alert or rec_ms > 0 or ml_used or ml_pred in ML_EVENT_LABELS:
        newfile = not EVENTS_CSV_PATH.exists()
        with EVENTS_CSV_PATH.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if newfile:
                w.writeheader()
            row = {k: obj.get(k, "") for k in CSV_FIELDS}
            w.writerow(row)


def _coerce_types(p: Dict[str, Any]) -> Dict[str, Any]:
    def to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    def to_int(x):
        try:
            return int(x)
        except Exception:
            f = to_float(x)
            return int(f) if f is not None else None

    for bkey in ("pump", "manual_override", "alert", "tds_sat", "ml_on", "ml_used"):
        if bkey in p and not isinstance(p[bkey], bool):
            s = str(p[bkey]).strip().lower()
            p[bkey] = s in ("1", "true", "t", "yes", "y")

    for ikey in ("ms", "rec_ms"):
        if ikey in p and not isinstance(p[ikey], int):
            iv = to_int(p[ikey])
            if iv is not None:
                p[ikey] = iv

    for fkey in (
        "tTop","tMid","tBot","dT_tb","pressure_hPa","lux","irObj","irAmb",
        "airT","airRH","tds_mV","micRMS","DOproxy","ml_conf",
    ):
        if fkey in p and not isinstance(p[fkey], (int, float)):
            fv = to_float(p[fkey])
            if fv is not None:
                p[fkey] = fv

    for skey in ("reason", "context", "ml_pred"):
        if skey in p and p[skey] is None:
            p[skey] = ""
        if skey not in p:
            p[skey] = ""

    for missing in ("ml_on", "ml_used"):
        if missing not in p:
            p[missing] = False
    if "ml_conf" not in p:
        p["ml_conf"] = None

    return p


@app.post("/ingest", response_class=PlainTextResponse)
async def ingest(req: Request) -> PlainTextResponse:
    try:
        payload = await req.json()
        payload["ts"] = time.time()
        payload = _coerce_types(payload)

        _append_ndjson(payload)
        _append_csv(payload)
        _append_events_csv(payload)

        global _last
        _last = payload
        return PlainTextResponse("OK", status_code=200)
    except Exception as e:
        return PlainTextResponse(f"ERR: {e}", status_code=400)


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "csv": CSV_PATH.exists(),
        "ndjson": NDJSON_PATH.exists(),
        "events_csv": EVENTS_CSV_PATH.exists(),
    }


@app.get("/latest")
def latest() -> JSONResponse:
    if _last is None:
        return JSONResponse({"error": "no data yet"}, status_code=404)
    return JSONResponse(_last, status_code=200)


@app.get("/export.csv")
def export_csv() -> FileResponse:
    if not CSV_PATH.exists():
        _append_csv({})
    return FileResponse(CSV_PATH, media_type="text/csv", filename="telemetry.csv")


@app.get("/events.csv")
def export_events_csv() -> FileResponse:
    if not EVENTS_CSV_PATH.exists():
        with EVENTS_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()
    return FileResponse(EVENTS_CSV_PATH, media_type="text/csv", filename="events.csv")