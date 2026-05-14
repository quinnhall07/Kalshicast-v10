"""Shared helpers for weather data collectors."""

from __future__ import annotations

import math
import os
import random
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from kalshicast.config.params_bootstrap import get_param_int, get_param_float


def to_float(x: Any) -> Optional[float]:
    """Safely convert a value to float, returning None on failure."""
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    """Float conversion that also rejects non-finite values (NaN/inf).

    Used by collectors that must avoid storing NaN/inf in numeric arrays.
    """
    try:
        if x is None:
            return None
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None


def reindex_axis(axis: List[str], m: Dict[str, float]) -> List[Optional[float]]:
    """Map a dict keyed by time-string onto a uniform time axis."""
    return [float(m[t]) if t in m else None for t in axis]


def backfill_daily_from_hourly_temps(
    target_dates: List[str],
    axis: List[str],
    temps: List[Optional[float]],
    daily_by_date: Dict[str, Dict[str, Optional[float]]],
) -> None:
    """Derive daily high/low from hourly temps when daily data is missing."""
    if not axis or not temps or len(axis) != len(temps):
        return

    per: Dict[str, List[float]] = {}
    for t, v in zip(axis, temps):
        if v is None:
            continue
        d = t[:10]
        if d in target_dates:
            per.setdefault(d, []).append(float(v))

    for d in target_dates:
        rec = daily_by_date.setdefault(d, {"high_f": None, "low_f": None})
        if rec.get("high_f") is not None and rec.get("low_f") is not None:
            continue
        vals = per.get(d) or []
        if not vals:
            continue
        if rec.get("high_f") is None:
            rec["high_f"] = max(vals)
        if rec.get("low_f") is None:
            rec["low_f"] = min(vals)


def _ensure_time_hour_z(ts: Any) -> Optional[str]:
    """Normalize an ISO timestamp to hour-truncated UTC "YYYY-MM-DDTHH:00:00Z".

    Accepts naive ISO strings (assumed UTC), tz-aware ISO, and the bare
    "YYYY-MM-DDTHH:MM" form (used by Open-Meteo when timezone=UTC).
    """
    if not isinstance(ts, str) or not ts.strip():
        return None
    s = ts.strip()
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        # fallback for "YYYY-MM-DDTHH:MM"
        if len(s) >= 13 and s[10] == "T":
            return s[:13] + ":00:00Z"
        return None


def _parse_timeout_from_env(default: Tuple[float, float]) -> Tuple[float, float]:
    """Parse an "OME_TIMEOUT" env var as either a single float or "connect,read"."""
    env_t = (os.getenv("OME_TIMEOUT") or "").strip()
    if not env_t:
        return default
    try:
        if "," in env_t:
            a, b = env_t.split(",", 1)
            return (float(a.strip()), float(b.strip()))
        v = float(env_t)
        return (v, v)
    except Exception:
        return default


def _is_retryable_exc(e: Exception) -> bool:
    """Classify a requests-layer exception as retryable (timeout/conn/5xx/429)."""
    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(e, requests.HTTPError):
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
        return code == 429 or (isinstance(code, int) and code >= 500)
    return False


def _get_with_retries(
    url: str,
    *,
    params: Dict[str, Any],
    session: requests.Session,
    semaphore: threading.BoundedSemaphore,
) -> dict:
    """Open-Meteo style GET with semaphore-gated session, retries, and backoff.

    Session and semaphore are passed in so callers retain their own concurrency
    isolation (collect_ome and collect_ome_model use independent semaphores).
    """
    timeout = _parse_timeout_from_env(
        (get_param_float("ome.timeout_connect"), get_param_float("ome.timeout_read"))
    )

    last: Optional[Exception] = None

    for attempt in range(1, get_param_int("ome.max_attempts") + 1):
        try:
            semaphore.acquire()
            try:
                r = session.get(url, params=params, timeout=timeout)
            finally:
                semaphore.release()

            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(
                    f"Open-Meteo error: {data.get('reason') or data.get('message') or data}"
                )
            return data

        except Exception as e:
            last = e
            if attempt >= get_param_int("ome.max_attempts") or not _is_retryable_exc(e):
                raise

            base = get_param_float("ome.backoff_base_s") * (2 ** (attempt - 1))
            sleep_s = base * random.uniform(0.75, 1.25)
            time.sleep(sleep_s)

    raise last  # pragma: no cover
