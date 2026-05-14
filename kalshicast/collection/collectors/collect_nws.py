# collectors/collect_nws.py
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from kalshicast.config import HEADERS
from kalshicast.collection.collectors.base import _to_float, reindex_axis
from kalshicast.collection.time_axis import (
    axis_start_end,
    build_hourly_axis_z,
    daily_targets_from_axis,
    hourly_axis_set,
    truncate_issued_at_to_hour_z,
)

"""
NWS collector contract REQUIRED by morning.py:

{
  "issued_at": "YYYY-MM-DDTHH:00:00Z",
  "daily": [ {"target_date":"YYYY-MM-DD","high_f":float,"low_f":float}, ... ],  # always 4 rows
  "hourly": {
    "time":[...],  # always 96 rows
    "temperature_f":[...],
    "dewpoint_f":[...],
    "humidity_pct":[...],
    "wind_speed_mph":[...],
    "wind_dir_deg":[...],
    "cloud_cover_pct":[...],
    "precip_prob_pct":[...],
  }
}

Policy:
- Always collect 4 days and 96 hours (forward-looking axis).
- Daily comes from /forecast (NWS API).
- If daily is incomplete, backfill missing dates from hourly temperature.
- Hourly comes from /forecastGridData plus PoP filled from /forecast/hourly.
- All hourly timestamps are UTC hour-truncated "...:00:00Z" via time_axis axis.
"""

# -------------------------
# NWS endpoints
# -------------------------


def _extract_points_urls(lat: float, lon: float) -> Tuple[str, str, str]:
    """
    Returns (forecast_url, grid_url, hourly_url)
    from api.weather.gov/points/{lat},{lon}
    """
    url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
    r = requests.get(url, headers=dict(HEADERS), timeout=20)
    r.raise_for_status()
    payload = r.json()
    props = (payload or {}).get("properties") or {}
    fc = props.get("forecast")
    grid = props.get("forecastGridData")
    hourly = props.get("forecastHourly")
    if not (isinstance(fc, str) and isinstance(grid, str) and isinstance(hourly, str)):
        raise ValueError("NWS points lookup missing forecast URLs")
    return fc, grid, hourly


# -------------------------
# Parsing / unit helpers
# -------------------------


def _c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


def _uom_to_mph(uom: str, v: float) -> float:
    u = (uom or "").lower()
    if "km_h" in u or "km/h" in u:
        return v * 0.621371
    if "m_s" in u or "m/s" in u:
        return v * 2.236936
    if "kn" in u or "knot" in u:
        return v * 1.150779
    return v


def _uom_to_f(uom: str, v: float) -> float:
    u = (uom or "").lower()
    if "degc" in u or "celsius" in u:
        return _c_to_f(v)
    if "degf" in u or "fahrenheit" in u:
        return v
    return _c_to_f(v)


def _parse_iso(dt_str: str) -> Optional[datetime]:
    if not isinstance(dt_str, str) or not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# -------------------------
# Hourly: grid expansion + axis reindex
# -------------------------


_DURATION_RE = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", re.IGNORECASE)


def _parse_duration_hours(dur: str) -> int:
    if not isinstance(dur, str) or not dur:
        return 1
    m = _DURATION_RE.match(dur.strip())
    if not m:
        return 1
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    total = h * 3600 + mi * 60 + s
    if total <= 0:
        return 1
    return max(1, int(round(total / 3600.0)))


def _expand_grid_values(
    values: Iterable[dict],
    *,
    uom: str,
    kind: str,
    start_utc: datetime,
    end_utc: datetime,
) -> Dict[str, float]:
    """
    Expand NWS grid data values into hour-truncated UTC timestamps (Z strings).
    Returns map: "YYYY-MM-DDTHH:00:00Z" -> value (converted to desired units)
    """
    out: Dict[str, float] = {}

    for it in values:
        if not isinstance(it, dict):
            continue
        vt = it.get("validTime")
        val = _to_float(it.get("value"))
        if not isinstance(vt, str) or val is None:
            continue

        try:
            start_s, dur_s = vt.split("/", 1)
        except ValueError:
            continue

        dt0 = _parse_iso(start_s)
        if not dt0:
            continue

        hours = _parse_duration_hours(dur_s)

        if kind in ("temperature_f", "dewpoint_f"):
            val_conv = _uom_to_f(uom, float(val))
        elif kind == "wind_speed_mph":
            val_conv = _uom_to_mph(uom, float(val))
        else:
            val_conv = float(val)

        for h in range(hours):
            t = (dt0 + timedelta(hours=h)).replace(minute=0, second=0, microsecond=0)
            if t < start_utc or t > end_utc:
                continue
            key = t.isoformat().replace("+00:00", "Z")
            out[key] = val_conv

    return out


def _series(props: dict, name: str) -> Tuple[str, List[dict]]:
    obj = props.get(name) or {}
    uom = str(obj.get("uom") or "")
    vals = obj.get("values") or []
    return uom, vals if isinstance(vals, list) else []




# -------------------------
# Hourly: PoP from /forecast/hourly (axis-aligned)
# -------------------------


def _expand_hourly_pop(payload: dict, *, start_utc: datetime, end_utc: datetime) -> Dict[str, float]:
    """
    Map hour timestamps to precipitation probability pct using /forecast/hourly.
    """
    props = (payload or {}).get("properties") or {}
    periods = props.get("periods") or []
    if not isinstance(periods, list):
        return {}

    out: Dict[str, float] = {}

    for p in periods:
        if not isinstance(p, dict):
            continue

        st = _parse_iso(p.get("startTime", ""))
        et = _parse_iso(p.get("endTime", ""))
        if not st or not et:
            continue

        st = st.replace(minute=0, second=0, microsecond=0)
        et = et.replace(minute=0, second=0, microsecond=0)

        pop_obj = p.get("probabilityOfPrecipitation") or {}
        pop = _to_float(pop_obj.get("value") if isinstance(pop_obj, dict) else pop_obj)
        if pop is None:
            continue

        t = st
        while t < et:
            if start_utc <= t <= end_utc:
                key = t.isoformat().replace("+00:00", "Z")
                out[key] = float(pop)
            t += timedelta(hours=1)

    return out


# -------------------------
# Daily: from /forecast, fallback from hourly temps
# -------------------------


def _extract_daily_from_forecast(payload: dict, target_dates: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Return by_date mapping for highs/lows (may be incomplete).
    """
    props = (payload or {}).get("properties") or {}
    periods = props.get("periods") or []
    if not isinstance(periods, list):
        return {}

    want = set(target_dates)
    by_date: Dict[str, Dict[str, Optional[float]]] = {d: {"high_f": None, "low_f": None} for d in target_dates}

    for p in periods:
        if not isinstance(p, dict):
            continue
        st = _parse_iso(p.get("startTime", ""))
        if not st:
            continue

        d = st.date().isoformat()
        if d not in want:
            continue

        is_day = bool(p.get("isDaytime", False))
        temp = _to_float(p.get("temperature"))
        if temp is None:
            continue

        rec = by_date[d]
        if is_day:
            rec["high_f"] = temp if rec["high_f"] is None else max(rec["high_f"], temp)
        else:
            rec["low_f"] = temp if rec["low_f"] is None else min(rec["low_f"], temp)

    return by_date


def _backfill_daily_from_hourly_temps(
    by_date: Dict[str, Dict[str, Optional[float]]],
    axis: List[str],
    temps: List[Optional[float]],
) -> None:
    """
    Fill missing highs/lows from hourly temperature_f aligned to axis.
    """
    if not axis or not temps or len(axis) != len(temps):
        return

    # Collect temps per date
    per: Dict[str, List[float]] = {}
    for t, v in zip(axis, temps):
        if v is None:
            continue
        d = t[:10]
        per.setdefault(d, []).append(float(v))

    for d, rec in by_date.items():
        if rec.get("high_f") is not None and rec.get("low_f") is not None:
            continue
        vals = per.get(d) or []
        if not vals:
            continue
        if rec.get("high_f") is None:
            rec["high_f"] = max(vals)
        if rec.get("low_f") is None:
            rec["low_f"] = min(vals)


# -------------------------
# Public fetcher
# -------------------------


def fetch_nws_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    NWS collector with standardized forward-looking axis.
    Always returns 4 daily rows and 96 hourly rows (axis-length), with None fills if needed.
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("NWS fetch requires station['lat'] and station['lon'].")

    # Standard horizon: 4 days / 96 hours
    days = 4
    axis = build_hourly_axis_z(days)
    axis_s = hourly_axis_set(axis)
    start_utc, end_utc = axis_start_end(axis)
    target_dates = daily_targets_from_axis(axis, station.get("timezone", "UTC"))[:days]

    forecast_url, grid_url, hourly_url = _extract_points_urls(float(lat), float(lon))

    # --- /forecast (daily + issued_at candidate) ---
    r_fc = requests.get(forecast_url, headers=dict(HEADERS), timeout=25)
    r_fc.raise_for_status()
    payload_fc = r_fc.json()
    props_fc = payload_fc.get("properties") or {}
    issued_at_raw = props_fc.get("generatedAt") or props_fc.get("updated")
    issued_at = truncate_issued_at_to_hour_z(str(issued_at_raw)) if issued_at_raw else None

    # --- /forecastGridData (hourly vars + issued_at candidate) ---
    r_grid = requests.get(grid_url, headers=dict(HEADERS), timeout=25)
    r_grid.raise_for_status()
    payload_grid = r_grid.json()
    props_grid = payload_grid.get("properties") or {}
    grid_gen = props_grid.get("generatedAt")
    issued2 = truncate_issued_at_to_hour_z(str(grid_gen)) if grid_gen else None
    if issued2:
        issued_at = issued2

    if not issued_at:
        issued_at = truncate_issued_at_to_hour_z(datetime.now(timezone.utc))  # always returns Z

    # --- Hourly from grid (expand maps then reindex to axis) ---
    temp_uom, temp_vals = _series(props_grid, "temperature")
    dew_uom, dew_vals = _series(props_grid, "dewpoint")
    rh_uom, rh_vals = _series(props_grid, "relativeHumidity")
    ws_uom, ws_vals = _series(props_grid, "windSpeed")
    wd_uom, wd_vals = _series(props_grid, "windDirection")
    sc_uom, sc_vals = _series(props_grid, "skyCover")

    temp_map = _expand_grid_values(temp_vals, uom=temp_uom, kind="temperature_f", start_utc=start_utc, end_utc=end_utc)
    dew_map = _expand_grid_values(dew_vals, uom=dew_uom, kind="dewpoint_f", start_utc=start_utc, end_utc=end_utc)
    rh_map = _expand_grid_values(rh_vals, uom=rh_uom, kind="humidity_pct", start_utc=start_utc, end_utc=end_utc)
    ws_map = _expand_grid_values(ws_vals, uom=ws_uom, kind="wind_speed_mph", start_utc=start_utc, end_utc=end_utc)
    wd_map = _expand_grid_values(wd_vals, uom=wd_uom, kind="wind_dir_deg", start_utc=start_utc, end_utc=end_utc)
    sc_map = _expand_grid_values(sc_vals, uom=sc_uom, kind="cloud_cover_pct", start_utc=start_utc, end_utc=end_utc)

    # --- PoP from /forecast/hourly (map then reindex) ---
    r_h = requests.get(hourly_url, headers=dict(HEADERS), timeout=25)
    r_h.raise_for_status()
    payload_h = r_h.json()
    pop_map = _expand_hourly_pop(payload_h, start_utc=start_utc, end_utc=end_utc)

    # Restrict to axis membership defensively (should already be in bounds)
    pop_map = {k: v for k, v in pop_map.items() if k in axis_s}

    hourly = {
        "time": axis,  # always 96
        "temperature_f": reindex_axis(axis, temp_map),
        "dewpoint_f": reindex_axis(axis, dew_map),
        "humidity_pct": reindex_axis(axis, rh_map),
        "wind_speed_mph": reindex_axis(axis, ws_map),
        "wind_dir_deg": reindex_axis(axis, wd_map),
        "cloud_cover_pct": reindex_axis(axis, sc_map),
        "precip_prob_pct": reindex_axis(axis, pop_map),
    }

    # --- Daily from /forecast, fallback from hourly temperature if incomplete ---
    by_date = _extract_daily_from_forecast(payload_fc, target_dates)
    # Backfill only if needed
    if any((by_date[d].get("high_f") is None or by_date[d].get("low_f") is None) for d in target_dates):
        _backfill_daily_from_hourly_temps(by_date, axis, hourly["temperature_f"])

    daily: List[Dict[str, Any]] = []
    for d in target_dates:
        rec = by_date.get(d) or {}
        hi = rec.get("high_f")
        lo = rec.get("low_f")
        # If still missing, fail closed by skipping (caller will log fewer rows)
        # but in practice the hourly fallback should cover almost all cases.
        if hi is None or lo is None:
            continue
        daily.append({"target_date": d, "high_f": float(hi), "low_f": float(lo)})

    # Guarantee daily length of 4 if hourly has temps across dates and forecast API missed pairs.
    # (If it still doesn't, that's truly missing hourly temperature coverage for a date.)
    if len(daily) < days:
        # second pass: attempt compute remaining dates only if not already computed
        have = {r["target_date"] for r in daily}
        missing = [d for d in target_dates if d not in have]
        if missing:
            # Compute from hourly only for missing dates (fallback rule)
            tmp_by = {d: {"high_f": None, "low_f": None} for d in missing}
            _backfill_daily_from_hourly_temps(tmp_by, axis, hourly["temperature_f"])
            for d in missing:
                rec = tmp_by.get(d) or {}
                hi = rec.get("high_f")
                lo = rec.get("low_f")
                if hi is None or lo is None:
                    continue
                daily.append({"target_date": d, "high_f": float(hi), "low_f": float(lo)})

        daily.sort(key=lambda r: r["target_date"])

    return {"issued_at": issued_at, "daily": daily[:days], "hourly": hourly}
