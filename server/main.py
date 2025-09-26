import requests
from pathlib import Path
from typing import Any, Dict, Optional, Deque, List
from fastapi import FastAPI, Request, BackgroundTasks
import csv, time, json, hashlib, statistics, collections
from fastapi.responses import PlainTextResponse, JSONResponse, FileResponse

DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
NDJSON_PATH = DATA_DIR / "telemetry.ndjson"
CSV_PATH = DATA_DIR / "telemetry.csv"
EVENTS_CSV_PATH = DATA_DIR / "events.csv"

DEFAULT_WEBHOOK = "https://discord.com/api/webhooks/1419184883426918423/ByE0LHg0Ew45EkqyVI2NgCsQHBlQ15MiZ3toEHC8DHJ_AkVejZQveJhf_VTymFYfapMv"

CSV_FIELDS = [
    "ts","ms","pump","manual_override","alert","reason","context","rec_ms",
    "tTop","tMid","tBot","dT_tb","pressure_hPa","lux","irObj","irAmb",
    "airT","airRH","tds_mV","tds_sat","micRMS","DOproxy",
    "ml_on","ml_pred","ml_conf","ml_used"
]

app = FastAPI(title="UBi-Guardian Collector")
_last: Optional[Dict[str, Any]] = None
_last_sent: Dict[str, float] = {}
DEDUP_SECS = 60.0

_hist: Dict[str, Deque[float]] = {k: collections.deque(maxlen=20) for k in ("micRMS","lux","tds_mV","dT_tb")}
_events: List[Dict[str, Any]] = []

def _median_mad(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals: return None
    med = statistics.median(vals)
    mad = statistics.median([abs(v - med) for v in vals])
    return med if mad == 0 else med

def _debounced(flag: bool, history: Deque[bool], hold: int = 3) -> bool:
    history.append(flag)
    return sum(history) >= hold

def _pump_duty(events: List[Dict[str, Any]], window_s: int = 3600) -> float:
    now = time.time()
    recs = [e for e in events if (now - e["ts"]) <= window_s and int(e.get("rec_ms",0) or 0) > 0]
    return sum(int(e.get("rec_ms",0) or 0) for e in recs) / 1000.0

def _burst_effect(events: List[Dict[str, Any]], dt: float = 60.0, min_drop: float = 0.1) -> bool:
    recent = [e for e in events if e.get("dT_tb") is not None]
    if len(recent) < 2: return True
    recent.sort(key=lambda x: x["ts"])
    end = recent[-1]; start = [e for e in recent if end["ts"] - e["ts"] >= dt]
    if not start: return True
    return abs(end["dT_tb"] - start[0]["dT_tb"]) >= min_drop

def _risk_band(do_val: Optional[float]) -> str:
    if do_val is None: return "n/a"
    if do_val < 5: return "low"
    if do_val < 7: return "medium"
    return "safe"

def _get_webhook_url() -> str:
    return DEFAULT_WEBHOOK

def _append_ndjson(obj: Dict[str, Any]) -> None:
    with NDJSON_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def _append_csv(obj: Dict[str, Any]) -> None:
    newfile = not CSV_PATH.exists()
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if newfile: w.writeheader()
        w.writerow({k: obj.get(k, "") for k in CSV_FIELDS})

def _append_events_csv(obj: Dict[str, Any]) -> None:
    newfile = not EVENTS_CSV_PATH.exists()
    with EVENTS_CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS + ["extra"])
        if newfile: w.writeheader()
        w.writerow({**{k: obj.get(k, "") for k in CSV_FIELDS}, "extra": obj.get("extra","")})

def _coerce_types(p: Dict[str, Any]) -> Dict[str, Any]:
    def to_float(x): 
        try: return float(x)
        except Exception: return None
    def to_int(x): 
        try: return int(x)
        except Exception:
            fx = to_float(x); return int(fx) if fx is not None else None
    for b in ("pump","manual_override","alert","tds_sat","ml_on","ml_used"):
        if b in p and not isinstance(p[b], bool):
            s = str(p[b]).strip().lower()
            p[b] = s in ("1","true","t","yes","y")
    for i in ("ms","rec_ms"):
        if i in p and not isinstance(p[i], int):
            iv = to_int(p[i]); p[i] = iv if iv is not None else p[i]
    for f in ("tTop","tMid","tBot","dT_tb","pressure_hPa","lux","irObj","irAmb",
              "airT","airRH","tds_mV","micRMS","DOproxy","ml_conf"):
        if f in p and not isinstance(p[f], (int,float)):
            fv = to_float(p[f]); p[f] = fv if fv is not None else p[f]
    for s in ("reason","context","ml_pred"):
        if s in p and p[s] is None: p[s] = ""
    return p

def _key_for(payload: Dict[str, Any]) -> str:
    kind = "alert" if payload.get("alert") else ("rec" if int(payload.get("rec_ms",0) or 0)>0 else "info")
    reason = str(payload.get("reason","none"))
    ctx = str(payload.get("context",""))
    return hashlib.sha1(f"{kind}|{reason}|{ctx}".encode("utf-8")).hexdigest()

def _should_send(payload: Dict[str, Any]) -> bool:
    now = time.time()
    k = _key_for(payload)
    last = _last_sent.get(k, 0.0)
    if now - last >= DEDUP_SECS:
        _last_sent[k] = now
        return True
    return False

def _fmt(v, digits=None):
    if v is None: return "n/a"
    try:
        if isinstance(v, float) and digits is not None:
            return f"{v:.{digits}f}"
        return str(v)
    except Exception:
        return str(v)

def _ascii_table(rows):
    k_w = max(len("key"), max(len(k) for k,_ in rows))
    v_w = max(len("value"), max(len(str(v)) for _,v in rows))
    h = f"+{'-'*(k_w+2)}+{'-'*(v_w+2)}+"
    r = [h, f"| {'key'.ljust(k_w)} | {'value'.ljust(v_w)} |", h]
    for k,v in rows:
        r.append(f"| {k.ljust(k_w)} | {str(v).ljust(v_w)} |")
    r.append(h)
    return "```\n" + "\n".join(r) + "\n```"

def _build_embed(payload: Dict[str, Any]) -> Dict[str, Any]:
    alert = bool(payload.get("alert", False))
    rec_ms = int(payload.get("rec_ms", 0) or 0)
    reason = str(payload.get("reason", "none"))
    ctx    = str(payload.get("context", ""))
    ml_on  = bool(payload.get("ml_on", False))
    ml_pred= str(payload.get("ml_pred", "n/a"))
    ml_conf= payload.get("ml_conf", None)
    ml_used= bool(payload.get("ml_used", False))
    title = "UBi-Guardian ALERT" if alert else ("Pump Recommendation" if rec_ms>0 else "Event")
    color = 0xE74C3C if alert else (0x2ECC71 if rec_ms>0 else 0x95A5A6)
    duty = _pump_duty(_events)
    eff_ok = _burst_effect(_events)
    band = _risk_band(payload.get("DOproxy"))
    extra = []
    if duty > 1800: extra.append("high_duty")
    if not eff_ok: extra.append("ineffective_burst")
    rows = [
        ("context", ctx or "n/a"),
        ("reason", reason or "none"),
        ("action", f"pump ON {rec_ms} ms" if rec_ms>0 else "-"),
        ("pump", "ON" if payload.get("pump") else "OFF"),
        ("manual_override", "true" if payload.get("manual_override") else "false"),
        ("C* DO (mg/L)", _fmt(payload.get("DOproxy"),2)),
        ("%DO band", band),
        ("tMid (°C)", _fmt(payload.get("tMid"),2)),
        ("dT_tb (°C)", _fmt(payload.get("dT_tb"),2)),
        ("lux", _fmt(payload.get("lux"),1)),
        ("micRMS", _fmt(payload.get("micRMS"),2)),
        ("TDS (mV)", _fmt(payload.get("tds_mV"),0)),
        ("pressure (hPa)", _fmt(payload.get("pressure_hPa"),1)),
        ("ml_on", "true" if ml_on else "false"),
        ("ml_pred/conf", f"{ml_pred} ({_fmt(ml_conf,3)})"),
        ("ml_used", "true" if ml_used else "false"),
        ("pump_duty (s/hr)", _fmt(duty,1)),
        ("efficacy_ok", str(eff_ok)),
        ("extra", ",".join(extra) if extra else "-")
    ]
    return {
        "title": title,
        "description": _ascii_table(rows),
        "color": color,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

def _post_discord(payload: Dict[str, Any]) -> None:
    url = _get_webhook_url()
    if not url: return
    if not payload.get("alert") and int(payload.get("rec_ms",0) or 0) <= 0 and str(payload.get("reason","none")) in ("","none"): return
    if not _should_send(payload): return
    body = {"content": None, "embeds": [_build_embed(payload)], "username": "UBi-Guardian"}
    try: requests.post(url, json=body, timeout=6)
    except Exception: pass

@app.post("/ingest", response_class=PlainTextResponse)
async def ingest(req: Request, bg: BackgroundTasks) -> PlainTextResponse:
    try:
        payload = await req.json()
        payload["ts"] = time.time()
        payload = _coerce_types(payload)
        for k in _hist: _hist[k].append(payload.get(k))
        _events.append(payload)
        _append_ndjson(payload)
        _append_csv(payload)
        _append_events_csv(payload)
        global _last; _last = payload
        bg.add_task(_post_discord, payload)
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
        "discord_webhook_set": bool(_get_webhook_url()),
    }

@app.get("/latest")
def latest() -> JSONResponse:
    if _last is None:
        return JSONResponse({"error": "no data yet"}, status_code=404)
    return JSONResponse(_last, status_code=200)

@app.get("/export.csv")
def export_csv() -> FileResponse:
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
    return FileResponse(CSV_PATH, media_type="text/csv", filename="telemetry.csv")

@app.get("/events.csv")
def export_events_csv() -> FileResponse:
    if not EVENTS_CSV_PATH.exists():
        with EVENTS_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
    return FileResponse(EVENTS_CSV_PATH, media_type="text/csv", filename="events.csv")

@app.post("/alert/test")
def alert_test() -> JSONResponse:
    demo = {
        "alert": True,
        "reason": "demo_alert",
        "context": "day",
        "pump": False,
        "manual_override": False,
        "rec_ms": 0,
        "tMid": 26.4, "dT_tb": 0.1, "pressure_hPa": 1007.8,
        "lux": 120.0, "micRMS": 3.2, "tds_mV": 420, "DOproxy": 6.9,
        "ml_on": True, "ml_pred": "disturbance", "ml_conf": 0.91, "ml_used": True,
    }
    _post_discord(demo)
    return JSONResponse({"ok": True})

@app.get("/dashboard")
def dashboard():
    return FileResponse("dashboard.html", media_type="text/html")