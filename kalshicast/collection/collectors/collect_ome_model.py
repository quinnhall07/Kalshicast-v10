# collectors/collect_ome_model.py
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
    _to_float,
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

_DAILY = "temperature_2m_max,temperature_2m_min"
_HOURLY_VARS = [
    "temperature_2m",
    "dew_point_2m",  # FIXED: Open-Meteo API uses dew_point_2m, not dewpoint_2m
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


def _find_ome_key(data_dict: dict, base_var: str) -> str:
    """
    Open-Meteo dynamically appends model names to keys when the 'models' parameter is used.
    (e.g., 'temperature_2m' becomes 'temperature_2m_gfs_seamless').
    This helper searches the dictionary for the correctly suffixed key.
    """
    if base_var in data_dict:
        return base_var
    prefix = base_var + "_"
    for k in data_dict.keys():
        if k.startswith(prefix):
            return k
    return base_var


def fetch_ome_model_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Open-Meteo multi-model collector (non-base models).

    params should include model selection, e.g.:
      {"models": "gfs_seamless"} / {"models": "ecmwf_ifs025"}
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    p: Dict[str, Any] = dict(params or {})

    days = get_param_int("pipeline.forecast_days")
    days = max(1, min(14, days))

    axis = build_hourly_axis_z(days)
    axis_s = hourly_axis_set(axis)
    start_utc, end_utc = axis_start_end(axis)
    target_dates = daily_targets_from_axis(axis)[:days]

    start_date = start_utc.date().isoformat()
    end_date = end_utc.date().isoformat()

    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),
        "timezone": "UTC",
        "start_date": start_date,
        "end_date": end_date,
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "daily": _DAILY,
        "hourly": ",".join(_HOURLY_VARS),
    }
    q.update(p)

    data = _get_with_retries(OME_URL, params=q, session=_OME_SESSION, semaphore=_OME_SEM)

    issued_at = truncate_issued_at_to_hour_z(datetime.now(timezone.utc))
    if not issued_at:
        issued_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat().replace(
            "+00:00", "Z"
        )

    # ---- Hourly (maps -> axis arrays) ----
    hourly0 = data.get("hourly") or {}
    h_time = hourly0.get("time") or []
    if not isinstance(h_time, list):
        h_time = []

    provider_times: List[str] = []
    for t in h_time:
        tz = _ensure_time_hour_z(t)
        if tz is None:
            provider_times = []
            break
        provider_times.append(tz)

    var_maps: Dict[str, Dict[str, float]] = {}
    if provider_times:
        for var in _HOURLY_VARS:
            actual_key = _find_ome_key(hourly0, var)
            arr = hourly0.get(actual_key)
            if not isinstance(arr, list):
                continue
            m: Dict[str, float] = {}
            for t, v in zip(provider_times, arr):
                if t not in axis_s:
                    continue
                fv = _to_float(v)
                if fv is None:
                    continue
                m[t] = float(fv)
            var_maps[var] = m

    hourly_out: Dict[str, Any] = {"time": axis}
    for var in _HOURLY_VARS:
        hourly_out[var] = reindex_axis(axis, var_maps.get(var) or {})

    # BACKWARD COMPATIBILITY: Ensure the downstream ETL script still finds the data
    # if it specifically maps 'dewpoint_2m' to the 'dewpoint_f' database column.
    if "dew_point_2m" in hourly_out:
        hourly_out["dewpoint_2m"] = hourly_out["dew_point_2m"]

    # ---- Daily (API first; align to target_dates; fallback if missing) ----
    daily = data.get("daily") or {}
    d_time = daily.get("time") or []

    d_hi_key = _find_ome_key(daily, "temperature_2m_max")
    d_lo_key = _find_ome_key(daily, "temperature_2m_min")

    d_hi = daily.get(d_hi_key) or []
    d_lo = daily.get(d_lo_key) or []

    by_date: Dict[str, Dict[str, Optional[float]]] = {d: {"high_f": None, "low_f": None} for d in target_dates}

    if isinstance(d_time, list) and isinstance(d_hi, list) and isinstance(d_lo, list):
        n = min(len(d_time), len(d_hi), len(d_lo))
        for i in range(n):
            td = str(d_time[i])[:10]
            if td not in by_date:
                continue
            hi = _to_float(d_hi[i])
            lo = _to_float(d_lo[i])
            if hi is None or lo is None:
                continue
            by_date[td]["high_f"] = float(hi)
            by_date[td]["low_f"] = float(lo)

    if any((by_date[d]["high_f"] is None or by_date[d]["low_f"] is None) for d in target_dates):
        backfill_daily_from_hourly_temps(target_dates, axis, hourly_out["temperature_2m"], by_date)

    daily_rows: List[Dict[str, Any]] = []
    for td in target_dates:
        rec = by_date.get(td) or {}
        hi = rec.get("high_f")
        lo = rec.get("low_f")
        if hi is None or lo is None:
            continue
        daily_rows.append({"target_date": td, "high_f": float(hi), "low_f": float(lo)})

    return {"issued_at": issued_at, "daily": daily_rows, "hourly": hourly_out}
