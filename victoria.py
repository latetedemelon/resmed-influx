"""
VictoriaMetrics backend for the ResMed/myAir importer.

Mirrors the small surface of ``influx.InfluxConnector`` (``measurement``,
``get_last_recorded_time`` and ``add_samples``) so ``main.py`` can use either
backend interchangeably. Data is written via the InfluxDB line protocol and
every series is tagged ``provider=resmed`` -- the unified platform's
"store separate, display together" model.

Because VictoriaMetrics has no Flux engine, the "last recorded time" is tracked
in a small JSON state file instead of being queried back from the database.
"""
import datetime as _dt
import json
import logging
import os
import time
from numbers import Number
from typing import Any, Dict, List, Optional

import requests


def _escape_measurement(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ")


def _escape_tag(value: str) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace("=", "\\=")
        .replace(" ", "\\ ")
    )


def _format_field(value: Any) -> Optional[str]:
    """Line-protocol field token, or None to skip. VictoriaMetrics is numeric
    only and drops the whole line on a string field, so strings are skipped."""
    if value is None or isinstance(value, str):
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Number):
        return str(value) if isinstance(value, int) else repr(float(value))
    return None


def _to_nanoseconds(value: Any) -> Optional[int]:
    if isinstance(value, _dt.datetime):
        return int(value.timestamp() * 1_000_000_000)
    if isinstance(value, Number):
        return int(value * 1_000_000_000) if value < 1_000_000_000_000 else int(value)
    if isinstance(value, str):
        try:
            return int(_dt.datetime.fromisoformat(value).timestamp() * 1_000_000_000)
        except ValueError:
            return None
    return None


class VictoriaConnector:
    def __init__(
        self,
        url: str,
        measurement: str,
        state_file: Optional[str] = None,
        provider: str = "resmed",
        timeout: int = 30,
    ) -> None:
        url = url.rstrip("/")
        for suffix in ("/write", "/api/v1/import/prometheus", "/api/v1/import"):
            if url.endswith(suffix):
                url = url[: -len(suffix)]
                break
        self.base_url = url
        self.write_url = url + "/write"
        self.measurement = measurement
        self.provider = provider
        self.timeout = timeout
        self.state_file = state_file or "/app/resmed_state.json"

    # --- last recorded time tracked in a small state file ---
    def get_last_recorded_time(self, max_days: int, to_time: _dt.datetime) -> _dt.datetime:
        fallback = to_time - _dt.timedelta(days=max_days)
        try:
            with open(self.state_file, "r") as handle:
                stored = json.load(handle).get("last_time")
            if stored:
                parsed = _dt.datetime.fromisoformat(stored)
                logging.info(f"Last recorded time from state file: {parsed}")
                return parsed
        except FileNotFoundError:
            logging.info("No state file yet; starting from %s", fallback)
        except Exception as err:  # noqa: BLE001
            logging.warning(f"Could not read state file ({err}); starting from {fallback}")
        return fallback

    def _save_last_time(self, latest: _dt.datetime) -> None:
        try:
            os.makedirs(os.path.dirname(self.state_file) or ".", exist_ok=True)
            with open(self.state_file, "w") as handle:
                json.dump({"last_time": latest.isoformat()}, handle)
        except Exception as err:  # noqa: BLE001
            logging.warning(f"Could not persist state file: {err}")

    # --- write ---
    def _line(self, record: Dict[str, Any]) -> Optional[str]:
        tags = {"provider": self.provider}
        tags.update(record.get("tags") or {})
        tag_str = ",".join(
            f"{_escape_tag(k)}={_escape_tag(v)}"
            for k, v in tags.items()
            if v is not None and v != ""
        )
        field_parts = []
        for key, raw in (record.get("fields") or {}).items():
            token = _format_field(raw)
            if token is not None:
                field_parts.append(f"{_escape_tag(key)}={token}")
        if not field_parts:
            return None
        ts = _to_nanoseconds(record.get("time"))
        if ts is None:
            ts = int(time.time() * 1_000_000_000)
        measurement = _escape_measurement(record.get("measurement", self.measurement))
        head = f"{measurement},{tag_str}" if tag_str else measurement
        return f"{head} {','.join(field_parts)} {ts}"

    def add_samples(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            logging.info("No records to import.")
            return
        lines = []
        latest: Optional[_dt.datetime] = None
        for record in records:
            line = self._line(record)
            if line:
                lines.append(line)
            parsed = record.get("time")
            try:
                parsed_dt = (
                    parsed
                    if isinstance(parsed, _dt.datetime)
                    else _dt.datetime.fromisoformat(str(parsed))
                )
                if latest is None or parsed_dt > latest:
                    latest = parsed_dt
            except (ValueError, TypeError):
                pass
        if not lines:
            logging.info("No numeric records to write.")
            return
        logging.info(f"Writing {len(lines)} record(s) to VictoriaMetrics.")
        resp = requests.post(self.write_url, data="\n".join(lines).encode("utf-8"), timeout=self.timeout)
        resp.raise_for_status()
        logging.info(f"Successfully wrote {len(lines)} record(s).")
        if latest is not None:
            self._save_last_time(latest)
