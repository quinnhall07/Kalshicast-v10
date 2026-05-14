# collectors/collect_tom.py
from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests

from kalshicast.config import HEADERS
from kalshicast.config.params_bootstrap import get_param_int
from kalshicast.collection.collectors.base import (
    _ensure_time_hour_z,
    backfill_daily_from_hourly_temps,
    to_float,
)
from kalshicast.collection.time_axis import (
    axis_start_end,
    build_hourly_axis_z,
    daily_targets_from_axis,
    hourly_axis_set,
    truncate_issued_at_to_hour_z,
)

TOM_URL = "https://api.tomorrow.io/v4/timelines"

"""
STRICT payload shape required by sources_registry + morning.py:

{
  "issued_at": "...Z",
  "daily": [
    {"target_date": "YYYY-MM-DD", "high_f": float, "low_f": float},
    ...
  ],
  "hourly": {
    "time": ["YYYY-MM-DDTHH:00:00Z", ...],      # ALWAYS axis length (FORECAST_DAYS*24)
    "temperature_f": [float|None, ...],
    "dewpoint_f": [float|None, ...],
    "humidity_pct": [float|None, ...],
    "wind_speed_mph": [float|None, ...],
    "wind_dir_deg": [float|None, ...],
    "cloud_cover_pct": [float|None, ...],
    "precip_prob_pct": [float|None, ...],
  }
}

Uses collectors.time_axis to enforce a shared forward-looking UTC axis.
Tomorrow.io typically supports 1h/1d timelines; we request a window covering the axis.
If Tomorrow returns fewer points (rare), we keep axis and fill missing with None.
"""


def fetch_tom_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Tomorrow.io collector -> STRICT payload, axis-aligned.

    Params:
      - include_hourly: bool (default True)
    """
    params = params or {}

    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Tomorrow.io fetch requires station['lat'] and station['lon'].")

    key = os.getenv("TOMORROW_API_KEY")
    if not key:
        raise RuntimeError("Missing TOMORROW_API_KEY env var")

    include_hourly = True
    if params.get("include_hourly") is not None:
        include_hourly = bool(params["include_hourly"])

    # Shared axis
    ndays = max(1, get_param_int("pipeline.forecast_days"))
    axis = build_hourly_axis_z(ndays)
    axis_s = hourly_axis_set(axis)
    start_utc, end_utc = axis_start_end(axis)
    target_dates = daily_targets_from_axis(axis, station.get("timezone", "UTC"))[:ndays]

    # Tomorrow timeline window (cover axis; endTime is inclusive-ish, add 1h pad)
    start_time = start_utc.isoformat().replace("+00:00", "Z")
    end_time = (end_utc + timedelta(hours=1)).isoformat().replace("+00:00", "Z")

    daily_fields = [
        "temperatureMax",
        "temperatureMin",
    ]

    hourly_fields = [
        "temperature",
        "humidity",
        "windSpeed",
        "windDirection",
        "cloudCover",
        "precipitationProbability",
        "dewPoint",
    ]

    timesteps = ["1d"]
    fields = list(daily_fields)
    if include_hourly:
        timesteps.append("1h")
        fields.extend([f for f in hourly_fields if f not in fields])

    payload = {
        "location": f"{float(lat)},{float(lon)}",
        "fields": fields,
        "timesteps": timesteps,
        "units": "imperial",
        "startTime": start_time,
        "endTime": end_time,
        "timezone": "UTC",
    }

    r = requests.post(
        TOM_URL,
        params={"apikey": key},
        json=payload,
        headers=dict(HEADERS),
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()

    issued_at = truncate_issued_at_to_hour_z(datetime.now(timezone.utc))
    if not issued_at:
        issued_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat().replace(
            "+00:00", "Z"
        )

    timelines = (data.get("data") or {}).get("timelines") or []
    if not isinstance(timelines, list) or not timelines:
        out: Dict[str, Any] = {"issued_at": issued_at, "daily": []}
        if include_hourly:
            out["hourly"] = {
                "time": axis,
                "temperature_f": [None] * len(axis),
                "dewpoint_f": [None] * len(axis),
                "humidity_pct": [None] * len(axis),
                "wind_speed_mph": [None] * len(axis),
                "wind_dir_deg": [None] * len(axis),
                "cloud_cover_pct": [None] * len(axis),
                "precip_prob_pct": [None] * len(axis),
            }
        return out

    t_by_step: Dict[str, dict] = {}
    for tl in timelines:
        step = (tl.get("timestep") or "").strip()
        if step:
            t_by_step[step] = tl

    # Axis-aligned hourly output
    hourly_out: Dict[str, List[Any]] = {
        "time": axis,
        "temperature_f": [None] * len(axis),
        "dewpoint_f": [None] * len(axis),
        "humidity_pct": [None] * len(axis),
        "wind_speed_mph": [None] * len(axis),
        "wind_dir_deg": [None] * len(axis),
        "cloud_cover_pct": [None] * len(axis),
        "precip_prob_pct": [None] * len(axis),
    }
    idx_map = {t: i for i, t in enumerate(axis)}

    # Daily by date (API first)
    daily_by_date: Dict[str, Dict[str, Optional[float]]] = {d: {"high_f": None, "low_f": None} for d in target_dates}

    # ----- Daily (1d) -----
    d_tl = t_by_step.get("1d")
    if isinstance(d_tl, dict):
        intervals = d_tl.get("intervals") or []
        if isinstance(intervals, list):
            for it in intervals:
                if not isinstance(it, dict):
                    continue
                start = it.get("startTime")
                vals = it.get("values") or {}
                if not isinstance(start, str) or not isinstance(vals, dict):
                    continue
                d = start[:10]
                if d not in daily_by_date:
                    continue
                hi = to_float(vals.get("temperatureMax"))
                lo = to_float(vals.get("temperatureMin"))
                if hi is not None:
                    daily_by_date[d]["high_f"] = float(hi)
                if lo is not None:
                    daily_by_date[d]["low_f"] = float(lo)

    # ----- Hourly (1h) -----
    if include_hourly:
        h_tl = t_by_step.get("1h")
        if isinstance(h_tl, dict):
            intervals = h_tl.get("intervals") or []
            if isinstance(intervals, list):
                for it in intervals:
                    if not isinstance(it, dict):
                        continue
                    start = _ensure_time_hour_z(it.get("startTime"))
                    vals = it.get("values") or {}
                    if start is None or not isinstance(vals, dict):
                        continue
                    if start not in axis_s:
                        continue
                    i = idx_map.get(start)
                    if i is None:
                        continue

                    hourly_out["temperature_f"][i] = to_float(vals.get("temperature"))
                    hourly_out["dewpoint_f"][i] = to_float(vals.get("dewPoint"))
                    hourly_out["humidity_pct"][i] = to_float(vals.get("humidity"))
                    hourly_out["wind_speed_mph"][i] = to_float(vals.get("windSpeed"))
                    hourly_out["wind_dir_deg"][i] = to_float(vals.get("windDirection"))
                    hourly_out["cloud_cover_pct"][i] = to_float(vals.get("cloudCover"))
                    hourly_out["precip_prob_pct"][i] = to_float(vals.get("precipitationProbability"))

    # Daily fallback from hourly temps if missing
    if any(
        (daily_by_date[d].get("high_f") is None or daily_by_date[d].get("low_f") is None) for d in target_dates
    ):
        backfill_daily_from_hourly_temps(target_dates, axis, hourly_out["temperature_f"], daily_by_date)

    daily: List[Dict[str, Any]] = []
    for d in target_dates:
        rec = daily_by_date.get(d) or {}
        hi = rec.get("high_f")
        lo = rec.get("low_f")
        if hi is None or lo is None:
            continue
        daily.append({"target_date": d, "high_f": float(hi), "low_f": float(lo)})

    out: Dict[str, Any] = {"issued_at": issued_at, "daily": daily}
    if include_hourly:
        out["hourly"] = hourly_out
    return out
