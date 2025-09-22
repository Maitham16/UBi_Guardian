from __future__ import annotations


import csv, math
from pathlib import Path
import statistics as stats

from collections import Counter, deque
from typing import Dict, Any, List, Tuple, Optional

CSV_IN  = Path("data/old/telemetry_b_test.csv")
CSV_OUT = Path("data/training.csv")

NIGHT_LUX            = 30.0
GLARE_LUX            = 2000.0
IR_DELTA_HOT         = 5.0
IR_ABS_HOT           = 45.0
AIR_FIRE_ABS         = 50.0
AIR_HOT_T            = 40.0
AIR_HOT_LOW_RH       = 15.0
DT_STRAT             = 1.0
DT_INV               = -0.8
DT60_COLD            = -0.5
TDS_JUMP_ABS         = 200.0
TDS_JUMP_FRAC        = 0.25
TDS_DWELL_SEC        = 10
DAY_ON_FACTOR        = 5.0
SUDDEN_LIGHT_FACTOR  = 10.0
SUDDEN_DARK_FACTOR   = 0.2
SUDDEN_WINDOW_SEC    = 3
SUDDEN_HOLD_SEC      = 3
P_DROP_HPA           = 6.0
OVERHEAT_UN          = 30.0
COOL_ON_C            = 30.0
COOL_OFF_C           = 29.5
TAP_MIN_SEC          = 0.7
TAP_MAX_SEC          = 2.0
DISTURB_DWELL_SEC    = 5.0
PUMP_SELF_MASK_SEC   = 8.0

FEATURES = [
    "micRMS","lux","tMid","dT_tb","DOproxy","tds_mV",
    "irObj","irAmb","airT","airRH","pressure_hPa",
    "pump","manual_override"
]

def F(r: Dict[str, Any], k: str) -> float:
    try:
        v = r.get(k, "")
        if v is None or v == "":
            return float("nan")
        return float(v)
    except Exception:
        return float("nan")

def I(r: Dict[str, Any], k: str) -> Optional[int]:
    try:
        x = r.get(k, "")
        if x in ("", None): return None
        return int(float(x))
    except Exception:
        return None

def B(r: Dict[str, Any], k: str) -> bool:
    s = str(r.get(k, "")).strip().lower()
    return s in ("1","true","t","yes","y")

def finite(x: Any) -> bool:
    try: return math.isfinite(x)
    except Exception: return False

rows: List[Dict[str, Any]] = []
if not CSV_IN.exists():
    raise SystemExit(f"Missing {CSV_IN}")

with CSV_IN.open(newline="") as f:
    rdr = csv.DictReader(f)
    for r in rdr:
        rows.append({
            "ts": F(r,"ts"),
            "ms": F(r,"ms"),
            "pump": B(r,"pump"),
            "manual_override": B(r,"manual_override"),
            "alert": B(r,"alert"),
            "reason": (r.get("reason") or "").strip(),
            "rec_ms": I(r,"rec_ms") or 0,
            "tTop": F(r,"tTop"),
            "tMid": F(r,"tMid"),
            "tBot": F(r,"tBot"),
            "dT_tb": F(r,"dT_tb"),
            "pressure_hPa": F(r,"pressure_hPa"),
            "lux": F(r,"lux"),
            "irObj": F(r,"irObj"),
            "irAmb": F(r,"irAmb"),
            "airT": F(r,"airT"),
            "airRH": F(r,"airRH"),
            "tds_mV": F(r,"tds_mV"),
            "tds_sat": B(r,"tds_sat"),
            "micRMS": F(r,"micRMS"),
            "DOproxy": F(r,"DOproxy"),
        })

if not rows:
    raise SystemExit("No data in telemetry.csv")

mic_vals = [r["micRMS"] for r in rows if finite(r["micRMS"])]
mu_mic = stats.median(mic_vals) if mic_vals else 0.0
sd_mic = stats.pstdev(mic_vals) if len(mic_vals) > 1 else 1.0
sd_mic = sd_mic or 1.0
def z_mic(x: float) -> float:
    return 0.0 if not finite(x) else (x - mu_mic) / sd_mic

def row_dt(prev: Dict[str, Any], cur: Dict[str, Any]) -> float:
    if finite(prev.get("ts")) and finite(cur.get("ts")):
        dt = cur["ts"] - prev["ts"]
        if 0.2 <= dt <= 5.0:
            return float(dt)
    return 1.0

win_tMid_60 = deque(maxlen=60)
lux_hist    = deque(maxlen=max(5, SUDDEN_WINDOW_SEC+1))
baro_hist: deque[Tuple[float,float]] = deque()
tds_base: Optional[float] = None
tds_since_s: float = 0.0
abrupt_dark_since_s: float = 0.0
disturb_since_s: Optional[float] = None
pump_on_at_s: Optional[float] = None
is_day = False
day_switch_ts: Optional[float] = None

feats: List[List[float]] = []
labels: List[str] = []

VALID_LABELS = {
    "calm","cold-shock","cooling-hot","disturbance","flashlight-night","glare",
    "human-tap","manual-override","other","pump-self","tds-spike","uniform-overheat"
}

def map_reason_to_label(reason: str) -> str:
    r = (reason or "").strip().lower()
    if r in ("", "none"):
        return "none"

    exact = {
        "cold_shock": "cold-shock",
        "tds_spike": "tds-spike",
        "uniform_overheat": "uniform-overheat",
        "cooling_hot": "cooling-hot",
        "human_tap": "human-tap",
        "disturbance": "disturbance",
        "flashlight_night": "flashlight-night",
        "heater_lamp": "glare",
    }
    if r in exact: return exact[r]

    if "cooling_hot" in r: return "cooling-hot"
    if "flashlight" in r and "night" in r: return "flashlight-night"
    if "heater_lamp" in r: return "glare"
    if "cold_shock" in r: return "cold-shock"
    if "tds_spike" in r: return "tds-spike"
    if "uniform_overheat" in r: return "uniform-overheat"

    if ("strat" in r) or ("inv" in r) or ("lowc" in r) or ("baro" in r) or ("night_mild" in r):
        return "pump-hint"

    if "safe_hold_sensor" in r or "abrupt_dark" in r:
        return "other"

    return "other"

def recompute_signals(idx: int, prev: Optional[Dict[str, Any]], cur: Dict[str, Any], dt_s: float) -> Dict[str, bool]:
    global is_day, day_switch_ts, tds_base, tds_since_s, abrupt_dark_since_s, disturb_since_s, pump_on_at_s

    mic = cur["micRMS"]; lux = cur["lux"]; tMid = cur["tMid"]; dT = cur["dT_tb"]
    irO = cur["irObj"];  irA = cur["irAmb"]; airT = cur["airT"]; RH = cur["airRH"]
    tds = cur["tds_mV"]; P   = cur["pressure_hPa"]
    pump = cur["pump"];  m_override = cur["manual_override"]

    if pump and pump_on_at_s is None:
        pump_on_at_s = cur["ts"] if finite(cur["ts"]) else 0.0
    if not pump:
        pump_on_at_s = None

    win_tMid_60.append(tMid)
    cold_shock = False
    if len(win_tMid_60) == win_tMid_60.maxlen and finite(tMid) and finite(win_tMid_60[0]):
        cold_shock = (tMid - win_tMid_60[0]) <= DT60_COLD

    stratified = (finite(dT) and dT > DT_STRAT)
    inversion  = (finite(dT) and dT < DT_INV)

    glare = (finite(lux) and lux >= GLARE_LUX)
    heater_lamp = glare and (
        (finite(irO) and finite(irA) and (irO - irA) >= IR_DELTA_HOT) or
        (finite(irO) and irO >= IR_ABS_HOT)
    )

    ambient_fire = (finite(airT) and airT >= AIR_FIRE_ABS) or \
                   (finite(airT) and finite(RH) and (airT >= AIR_HOT_T and RH <= AIR_HOT_LOW_RH))
    overheat_un = (finite(tMid) and tMid > OVERHEAT_UN and (finite(dT) and abs(dT) < 0.3))

    if finite(tds):
        if tds_base is None: tds_base = tds
        tds_base = (1 - 0.01)*tds_base + 0.01*tds
    tds_spike_now = (finite(tds) and tds_base is not None and
                     (tds - tds_base) > max(TDS_JUMP_ABS, TDS_JUMP_FRAC * tds_base))
    if tds_spike_now: tds_since_s += dt_s
    else:             tds_since_s = 0.0
    tds_spike = (tds_since_s >= TDS_DWELL_SEC)

    if finite(P) and finite(cur["ts"]):
        baro_hist.append((cur["ts"], P))
        cutoff = cur["ts"] - 3*3600
        while baro_hist and baro_hist[0][0] < cutoff:
            baro_hist.popleft()
    baro_drop = False
    if baro_hist:
        p_now = baro_hist[-1][1]; p_old = baro_hist[0][1]
        if finite(p_now) and finite(p_old):
            baro_drop = (p_old - p_now) >= P_DROP_HPA

    if not is_day and finite(lux) and lux > NIGHT_LUX * max(2.0, DAY_ON_FACTOR):
        is_day = True; day_switch_ts = cur["ts"]
    if is_day and finite(lux) and lux < NIGHT_LUX:
        is_day = False; day_switch_ts = cur["ts"]

    lux_hist.append((cur["ts"], lux if finite(lux) else 0.0))
    flashlight_night = False
    abrupt_dark_day  = False
    if len(lux_hist) >= 2:
        t_now, L_now = lux_hist[-1]
        base_L = L_now
        base_t = t_now
        for t, L in reversed(lux_hist):
            base_L = L; base_t = t
            if not finite(t_now) or not finite(t): break
            if (t_now - t) >= SUDDEN_WINDOW_SEC: break
        ratio_up = (L_now/base_L) if base_L > 0 else (999.0 if L_now > 0 else 1.0)
        ratio_down = (base_L/L_now) if L_now > 0 else 1.0

        night_flash_now = (not is_day) and (ratio_up >= SUDDEN_LIGHT_FACTOR)
        if night_flash_now:
            flashlight_night = True

        day_dark_now = is_day and (ratio_down <= SUDDEN_DARK_FACTOR)
        if day_dark_now:
            abrupt_dark_since_s += dt_s
        else:
            abrupt_dark_since_s = 0.0
        abrupt_dark_day = (abrupt_dark_since_s >= SUDDEN_HOLD_SEC)

    zr = z_mic(mic)
    within_self_mask = False
    if pump_on_at_s is not None and finite(cur["ts"]):
        within_self_mask = (cur["ts"] - pump_on_at_s) < PUMP_SELF_MASK_SEC

    ripple_now = (zr >= 1.0) and not within_self_mask
    if ripple_now:
        if disturb_since_s is None: disturb_since_s = 0.0
        else: disturb_since_s += dt_s
    else:
        disturb_since_s = None

    human_tap = False
    disturbance = False
    if disturb_since_s is not None:
        dur = disturb_since_s
        if (not pump) and (TAP_MIN_SEC <= dur <= TAP_MAX_SEC):
            human_tap = True
        if dur >= DISTURB_DWELL_SEC:
            disturbance = True

    cooling_hot = (finite(tMid) and tMid >= COOL_ON_C)

    return dict(
        cold_shock=cold_shock,
        stratified=stratified,
        inversion=inversion,
        glare=glare,
        heater_lamp=heater_lamp,
        ambient_fire=ambient_fire,
        overheat_un=overheat_un,
        tds_spike=tds_spike,
        baro_drop=baro_drop,
        flashlight_night=flashlight_night,
        abrupt_dark_day=abrupt_dark_day,
        human_tap=human_tap,
        disturbance=disturbance,
        cooling_hot=cooling_hot,
        zr=zr,
        is_day=is_day,
    )

def decide_label(cur: Dict[str, Any], sig: Dict[str, bool], reason_lbl: str) -> str:
    pump = cur["pump"]; m_override = cur["manual_override"]

    if pump and m_override:
        return "manual-override"
    if pump and not m_override:
        if reason_lbl in {"cooling-hot","cold-shock","tds-spike","uniform-overheat",
                          "human-tap","disturbance","flashlight-night","glare"}:
            return reason_lbl
        if reason_lbl == "pump-hint":
            return "pump-self"
        if   sig["cooling_hot"]:      return "cooling-hot"
        elif sig["tds_spike"]:        return "tds-spike"
        elif sig["cold_shock"]:       return "cold-shock"
        elif sig["overheat_un"]:      return "uniform-overheat"
        elif sig["flashlight_night"]: return "flashlight-night"
        elif sig["glare"] or sig["heater_lamp"]: return "glare"
        else: return "pump-self"

    if reason_lbl in VALID_LABELS and reason_lbl not in {"pump-self","manual-override"}:
        return reason_lbl

    if   sig["flashlight_night"]: return "flashlight-night"
    elif sig["glare"] or sig["heater_lamp"]: return "glare"
    elif sig["tds_spike"]:        return "tds-spike"
    elif sig["cold_shock"]:       return "cold-shock"
    elif sig["overheat_un"]:      return "uniform-overheat"
    elif sig["human_tap"]:        return "human-tap"
    elif sig["disturbance"]:      return "disturbance"
    elif sig["cooling_hot"]:      return "cooling-hot"

    L = cur["lux"]; zr = sig["zr"]
    if (not finite(L) or L < 0.5*GLARE_LUX) and zr <= 1.0:
        return "calm"
    return "other"

for i, r in enumerate(rows):
    prev = rows[i-1] if i > 0 else None
    dt_s = row_dt(prev, r) if prev else 1.0

    sig = recompute_signals(i, prev, r, dt_s)

    reason_lbl = map_reason_to_label(r["reason"])

    lbl = decide_label(r, sig, reason_lbl)

    feats.append([
        r["micRMS"], r["lux"], r["tMid"], r["dT_tb"], r["DOproxy"], r["tds_mV"],
        r["irObj"], r["irAmb"], r["airT"], r["airRH"], r["pressure_hPa"],
        1.0 if r["pump"] else 0.0, 1.0 if r["manual_override"] else 0.0
    ])
    labels.append(lbl)

CSV_OUT.parent.mkdir(exist_ok=True)
with CSV_OUT.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(FEATURES + ["label"])
    for v, l in zip(feats, labels):
        w.writerow(v + [l])

cnt = Counter(labels)
print(f"OK: wrote {CSV_OUT} rows={len(labels)}")
print("Label counts:", dict(sorted(cnt.items(), key=lambda x: x[0])))