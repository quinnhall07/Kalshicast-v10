# collectors/collect_ome.py  (OME_BASE)
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter

from kalshicast.config import HEADERS
from kalshicast.config.params_bootstrap import get_param_int
from kalshicast.collection.collectors.base import (
    _ensure_time_hour_z,
    _get_with_retries,
    backfill_daily_from_hourly_temps,
    reindex_axis,
)
from kalshicast.collection.time_axis import (
    axis_start_end,
    build_hourly_axis_z,
    daily_targets_from_axis,
    hourly_axis_set,
    truncate_issued_at_to_hour_z,
)

OME_URL = "https://api.open-meteo.com/v1/forecast"

_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
]

_HOURLY_VARS = [
    "temperature_2m",
    "dew_point_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover",
    "precipitation_probability",
]

# Shared per-process client resources (kept local: collect_ome and
# collect_ome_model run with independent semaphores by design).
_OME_SEM = threading.BoundedSemaphore(value=get_param_int("ome.max_inflight"))
_OME_SESSION = requests.Session()
_OME_SESSION.headers.update(dict(HEADERS))
_OME_SESSION.mount(
    "https://",
    HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=0),
)


def fetch_ome_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    STRICT payload shape (consumed by morning.py):
      {
        "issued_at": "...Z",
        "daily": [ {"target_date":"YYYY-MM-DD","high_f":float,"low_f":float}, ... ],
        "hourly": { "time":[...], "<var>":[...], ... }   # Open-Meteo arrays (axis-aligned)
      }

    Uses collectors.time_axis to enforce a forward-looking UTC axis.
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    # Horizon (centralized)
    days = get_param_int("pipeline.forecast_days")
    days = max(1, min(14, days))

    # Shared axis
    axis = build_hourly_axis_z(days)
    axis_s = hourly_axis_set(axis)
    start_utc, end_utc = axis_start_end(axis)
    target_dates = daily_targets_from_axis(axis, station.get("timezone", "UTC"))[:days]

    # Open-Meteo only accepts date windows; request the UTC dates that cover the axis.
    start_date = start_utc.date().isoformat()
    end_date = end_utc.date().isoformat()

    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),

        "timezone": "UTC",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",

        "start_date": start_date,
        "end_date": end_date,

        "daily": ",".join(_DAILY_VARS),
        "hourly": ",".join(_HOURLY_VARS),
    }

    if params:
        q.update(params)

    data = _get_with_retries(OME_URL, params=q, session=_OME_SESSION, semaphore=_OME_SEM)

    issued_at = truncate_issued_at_to_hour_z(datetime.now(timezone.utc))
    if not issued_at:
        issued_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat().replace(
            "+00:00", "Z"
        )

    # ---- Hourly (build maps from provider arrays, then reindex to axis) ----
    hourly = data.get("hourly") or {}
    h_time = hourly.get("time") or []
    if not isinstance(h_time, list):
        h_time = []

    # normalize provider times to hour-truncated Z and filter to axis
    provider_times: List[str] = []
    for t in h_time:
        tz = _ensure_time_hour_z(t)
        if tz is None:
            provider_times = []
            break
        provider_times.append(tz)

    # Build per-variable maps keyed by axis timestamps
    var_maps: Dict[str, Dict[str, float]] = {}
    if provider_times:
        for var in _HOURLY_VARS:
            arr = hourly.get(var)
            if not isinstance(arr, list):
                continue
            m: Dict[str, float] = {}
            for t, v in zip(provider_times, arr):
                if t not in axis_s:
                    continue
                try:
                    fv = float(v)
                except Exception:
                    continue
                m[t] = fv
            var_maps[var] = m

    # Axis-aligned hourly arrays (always axis length)
    hourly_out: Dict[str, Any] = {"time": axis}
    for var in _HOURLY_VARS:
        m = var_maps.get(var) or {}
        hourly_out[var] = reindex_axis(axis, m)

    # ---- Daily (from API; align to target_dates; fallback from hourly temps if needed) ----
    daily = data.get("daily") or {}
    d_time = daily.get("time") or []
    d_hi = daily.get("temperature_2m_max") or []
    d_lo = daily.get("temperature_2m_min") or []

    daily_by_date: Dict[str, Dict[str, Optional[float]]] = {d: {"high_f": None, "low_f": None} for d in target_dates}

    if isinstance(d_time, list) and isinstance(d_hi, list) and isinstance(d_lo, list):
        n = min(len(d_time), len(d_hi), len(d_lo))
        for i in range(n):
            d = str(d_time[i])[:10]
            if d not in daily_by_date:
                continue
            try:
                hi = float(d_hi[i])
                lo = float(d_lo[i])
            except Exception:
                continue
            daily_by_date[d]["high_f"] = hi
            daily_by_date[d]["low_f"] = lo

    # Fallback only for missing dates (allowed)
    if any(
        (daily_by_date[d].get("high_f") is None or daily_by_date[d].get("low_f") is None) for d in target_dates
    ):
        backfill_daily_from_hourly_temps(
            target_dates,
            axis,
            hourly_out.get("temperature_2m") or [],
            daily_by_date,
        )

    out_daily: List[dict] = []
    for d in target_dates:
        rec = daily_by_date.get(d) or {}
        hi = rec.get("high_f")
        lo = rec.get("low_f")
        if hi is None or lo is None:
            continue
        out_daily.append({"target_date": d, "high_f": float(hi), "low_f": float(lo)})

    out: Dict[str, Any] = {"issued_at": issued_at, "daily": out_daily}
    out["hourly"] = hourly_out
    return out
