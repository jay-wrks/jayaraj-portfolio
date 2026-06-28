from __future__ import annotations

import hashlib
import json
import math
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from firebase_admin import initialize_app
from firebase_functions import https_fn, options
from google.cloud import firestore

initialize_app()

COLLECTION_NAME = os.getenv("FIRESTORE_COLLECTION", "portfolio_analytics_events")
FIRESTORE_DATABASE = os.getenv("FIRESTORE_DATABASE", "myfirestoredb")
DEFAULT_LIMIT = 100
MAX_LIMIT = 8000
MAX_INGEST_BATCH = 450
MAX_FETCH = 7000
DEFAULT_HEATMAP_COLS = 24
DEFAULT_HEATMAP_ROWS = 14


def _db():
    return firestore.Client(database=FIRESTORE_DATABASE)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_string(value: Any, limit: int = 0) -> str:
    text = str(value or "").strip()
    if limit and len(text) > limit:
        return text[:limit]
    return text


def _coalesce(*values: Any) -> str:
    for value in values:
        text = _normalize_string(value)
        if text:
            return text
    return ""


def _clamp_01(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    return round(min(1.0, max(0.0, numeric)), 4)


def _parse_positive_int(value: str | None, fallback: int) -> int:
    try:
        parsed = int(str(value or "").strip())
        if parsed > 0:
            return parsed
    except (TypeError, ValueError):
        pass
    return fallback


def _average_int(total: int, count: int) -> int:
    if count <= 0:
        return 0
    return int(round(total / count))


def _average_ms(total: int, count: int) -> int:
    return _average_int(total, count)


def _json_response(payload: dict[str, Any], status: int = 200) -> https_fn.Response:
    return https_fn.Response(
        json.dumps(payload, default=_json_default),
        status=status,
        mimetype="application/json",
    )


def _json_default(value: Any):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def _parse_request_json(req: https_fn.Request) -> dict[str, Any]:
    data = req.get_json(silent=True)
    if isinstance(data, dict):
        return data
    return {}


def _parse_datetime(raw: str, end_of_day: bool = False) -> datetime | None:
    raw = _normalize_string(raw)
    if not raw:
        return None

    if len(raw) == 10:
        parsed = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return parsed + timedelta(days=1) if end_of_day else parsed

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed + timedelta(seconds=1) if end_of_day else parsed


def _to_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            return _parse_datetime(value, False)
        except Exception:
            return None
    return None


def _parse_filters(req: https_fn.Request) -> dict[str, Any]:
    args = req.args
    limit = min(_parse_positive_int(args.get("limit"), DEFAULT_LIMIT), MAX_LIMIT)
    return {
        "from_dt": _parse_datetime(args.get("from", ""), False),
        "to_dt": _parse_datetime(args.get("to", ""), True),
        "path": _normalize_string(args.get("path"), 512),
        "event_type": _normalize_string(args.get("eventType"), 64),
        "device_type": _normalize_string(args.get("deviceType"), 64),
        "timezone_value": _normalize_string(args.get("timezone"), 128),
        "command": _normalize_string(args.get("command"), 512).lower(),
        "limit": limit,
    }


def _request_path(req: https_fn.Request) -> str:
    path = _normalize_string(req.path or "/")
    if not path.startswith("/"):
        path = f"/{path}"
    return path


def _endpoint_name(req: https_fn.Request) -> str:
    action = _normalize_string(req.args.get("action"), 64).lower()
    if action:
        return action

    path = _request_path(req)
    parts = [part for part in path.split("/") if part]
    if not parts:
        return "root"

    if parts[0] == "api":
        parts = parts[1:]
    if not parts:
        return "root"

    candidate = parts[-1].lower()
    if candidate in {"ingest", "summary", "timeseries", "commands", "sessions", "heatmap", "events"}:
        return candidate
    return "root"


def _hash_ip(req: https_fn.Request) -> str:
    forwarded = _normalize_string(req.headers.get("X-Forwarded-For"))
    remote = forwarded.split(",")[0].strip() if forwarded else _normalize_string(req.remote_addr)
    if not remote:
        return ""
    salt = os.getenv("ANALYTICS_IP_SALT", "")
    digest = hashlib.sha256(f"{remote}:{salt}".encode("utf-8")).hexdigest()
    return digest


def _normalize_event(raw_event: dict[str, Any], received_at: datetime, ip_hash: str) -> dict[str, Any]:
    occurred_at_raw = _normalize_string(raw_event.get("occurredAt"))
    try:
      occurred_at = _parse_datetime(occurred_at_raw, False) if occurred_at_raw else received_at
    except Exception:
      occurred_at = received_at
    if occurred_at is None:
        occurred_at = received_at

    return {
        "siteName": _normalize_string(raw_event.get("siteName"), 128),
        "eventType": _normalize_string(raw_event.get("eventType"), 64),
        "eventId": _normalize_string(raw_event.get("eventId"), 128),
        "sessionId": _normalize_string(raw_event.get("sessionId"), 128),
        "visitorId": _normalize_string(raw_event.get("visitorId"), 128),
        "pageId": _normalize_string(raw_event.get("pageId"), 128),
        "occurredAt": occurred_at,
        "occurredDate": occurred_at.strftime("%Y-%m-%d"),
        "occurredHour": occurred_at.hour,
        "serverReceivedAt": received_at,
        "path": _normalize_string(raw_event.get("path"), 512),
        "href": _normalize_string(raw_event.get("href"), 512),
        "title": _normalize_string(raw_event.get("title"), 512),
        "referrer": _normalize_string(raw_event.get("referrer"), 512),
        "language": _normalize_string(raw_event.get("language"), 64),
        "timezone": _normalize_string(raw_event.get("timezone"), 128),
        "platform": _normalize_string(raw_event.get("platform"), 128),
        "userAgent": _normalize_string(raw_event.get("userAgent"), 512),
        "deviceType": _normalize_string(raw_event.get("deviceType"), 64),
        "viewportW": max(int(raw_event.get("viewportW") or 0), 0),
        "viewportH": max(int(raw_event.get("viewportH") or 0), 0),
        "screenW": max(int(raw_event.get("screenW") or 0), 0),
        "screenH": max(int(raw_event.get("screenH") or 0), 0),
        "pixelRatio": round(float(raw_event.get("pixelRatio") or 0), 2),
        "touchPoints": max(int(raw_event.get("touchPoints") or 0), 0),
        "hardwareConcurrency": max(int(raw_event.get("hardwareConcurrency") or 0), 0),
        "sessionDurationMs": max(int(raw_event.get("sessionDurationMs") or 0), 0),
        "visibleMs": max(int(raw_event.get("visibleMs") or 0), 0),
        "activeMs": max(int(raw_event.get("activeMs") or 0), 0),
        "commandCount": max(int(raw_event.get("commandCount") or 0), 0),
        "clickCount": max(int(raw_event.get("clickCount") or 0), 0),
        "scrollDepth": _clamp_01(raw_event.get("scrollDepth")),
        "maxScrollDepth": _clamp_01(raw_event.get("maxScrollDepth")),
        "command": _normalize_string(raw_event.get("command"), 512),
        "commandName": _normalize_string(raw_event.get("commandName"), 512),
        "commandFamily": _normalize_string(raw_event.get("commandFamily"), 128),
        "commandSource": _normalize_string(raw_event.get("commandSource"), 64),
        "status": _normalize_string(raw_event.get("status"), 64),
        "commandDurationMs": max(int(raw_event.get("commandDurationMs") or 0), 0),
        "typingDurationMs": max(int(raw_event.get("typingDurationMs") or 0), 0),
        "keyCount": max(int(raw_event.get("keyCount") or 0), 0),
        "backspaceCount": max(int(raw_event.get("backspaceCount") or 0), 0),
        "commandLength": max(int(raw_event.get("commandLength") or 0), 0),
        "typingSource": _normalize_string(raw_event.get("typingSource"), 64),
        "outputChars": max(int(raw_event.get("outputChars") or 0), 0),
        "outputLines": max(int(raw_event.get("outputLines") or 0), 0),
        "outputPreview": _normalize_string(raw_event.get("outputPreview"), 280),
        "section": _normalize_string(raw_event.get("section"), 256),
        "label": _normalize_string(raw_event.get("label"), 256),
        "xNorm": _clamp_01(raw_event.get("xNorm")),
        "yNorm": _clamp_01(raw_event.get("yNorm")),
        "targetTag": _normalize_string(raw_event.get("targetTag"), 64),
        "targetText": _normalize_string(raw_event.get("targetText"), 512),
        "linkTarget": _normalize_string(raw_event.get("linkTarget"), 512),
        "activeWindowMs": max(int(raw_event.get("activeWindowMs") or 0), 0),
        "ipHash": ip_hash,
        "meta": raw_event.get("meta") if isinstance(raw_event.get("meta"), dict) else None,
    }


def _load_events(filters: dict[str, Any], fetch_cap: int = MAX_FETCH) -> list[dict[str, Any]]:
    query = _db().collection(COLLECTION_NAME)
    if filters["from_dt"] is not None:
        query = query.where("occurredAt", ">=", filters["from_dt"])
    if filters["to_dt"] is not None:
        query = query.where("occurredAt", "<", filters["to_dt"])
    query = query.order_by("occurredAt", direction=firestore.Query.DESCENDING).limit(fetch_cap)
    docs = query.stream()

    events: list[dict[str, Any]] = []
    for doc in docs:
        event = doc.to_dict() or {}
        occurred_at = _to_utc_datetime(event.get("occurredAt"))
        if occurred_at is not None:
            event["occurredAt"] = occurred_at
        if _matches_filters(event, filters):
            events.append(event)
        if len(events) >= fetch_cap:
            break
    return events


def _matches_filters(event: dict[str, Any], filters: dict[str, Any]) -> bool:
    if filters["path"] and _normalize_string(event.get("path")) != filters["path"]:
        return False
    if filters["event_type"] and _normalize_string(event.get("eventType")) != filters["event_type"]:
        return False
    if filters["device_type"] and _normalize_string(event.get("deviceType")).lower() != filters["device_type"].lower():
        return False
    if filters["timezone_value"] and filters["timezone_value"].lower() not in _normalize_string(event.get("timezone")).lower():
        return False
    if filters["command"]:
        haystack = " ".join([
            _normalize_string(event.get("command")).lower(),
            _normalize_string(event.get("commandName")).lower(),
            _normalize_string(event.get("commandFamily")).lower(),
        ])
        if filters["command"] not in haystack:
            return False
    return True


def _top_counts(counts: dict[str, int], limit: int = 8) -> list[dict[str, Any]]:
    rows = [{"label": label, "count": count} for label, count in counts.items()]
    rows.sort(key=lambda item: (-item["count"], item["label"]))
    return rows[:limit]


def _event_row(event: dict[str, Any]) -> dict[str, Any]:
    occurred_at = event.get("occurredAt")
    return {
        "occurredAt": occurred_at,
        "eventType": event.get("eventType", ""),
        "sessionId": event.get("sessionId", ""),
        "command": event.get("command", ""),
        "commandName": event.get("commandName", ""),
        "status": event.get("status", ""),
        "label": event.get("label", ""),
        "section": event.get("section", ""),
        "path": event.get("path", ""),
        "deviceType": event.get("deviceType", ""),
        "timezone": event.get("timezone", ""),
        "scrollDepth": event.get("scrollDepth", 0),
        "outputPreview": event.get("outputPreview", ""),
        "typingDurationMs": event.get("typingDurationMs", 0),
        "commandDurationMs": event.get("commandDurationMs", 0),
    }


def _handle_root() -> https_fn.Response:
    return _json_response(
        {
            "ok": True,
            "service": "analytics_api",
            "endpoints": ["ingest", "summary", "timeseries", "commands", "sessions", "heatmap", "events"],
        }
    )


def _handle_ingest(req: https_fn.Request) -> https_fn.Response:
    body = _parse_request_json(req)
    events = body.get("events")
    if not isinstance(events, list) or not events:
        return _json_response({"ok": False, "error": "events array is required"}, 400)

    received_at = _now()
    ip_hash = _hash_ip(req)
    client = _db()
    pending: list[dict[str, Any]] = []
    written = 0

    for raw_event in events:
        if not isinstance(raw_event, dict):
            continue
        normalized = _normalize_event(raw_event, received_at, ip_hash)
        if not normalized.get("eventType"):
            continue
        pending.append(normalized)
        written += 1
        if len(pending) >= MAX_INGEST_BATCH:
            batch = client.batch()
            for event in pending:
                batch.set(client.collection(COLLECTION_NAME).document(), event)
            batch.commit()
            pending = []

    if pending:
        batch = client.batch()
        for event in pending:
            batch.set(client.collection(COLLECTION_NAME).document(), event)
        batch.commit()

    return _json_response({"ok": True, "written": written}, 202)


def _handle_summary(req: https_fn.Request) -> https_fn.Response:
    filters = _parse_filters(req)
    events = _load_events(filters, 6000)

    sessions: dict[str, dict[str, Any]] = {}
    path_counts: dict[str, int] = defaultdict(int)
    section_counts: dict[str, int] = defaultdict(int)
    device_counts: dict[str, int] = defaultdict(int)
    timezone_counts: dict[str, int] = defaultdict(int)

    total_page_views = 0
    total_section_views = 0
    total_commands = 0
    success_commands = 0
    total_command_ms = 0
    total_typing_ms = 0

    for event in events:
        session_id = _normalize_string(event.get("sessionId"))
        if session_id:
            aggregate = sessions.setdefault(
                session_id,
                {"durationMs": 0, "activeMs": 0, "visibleMs": 0, "deviceType": "", "timezone": ""},
            )
            aggregate["durationMs"] = max(aggregate["durationMs"], int(event.get("sessionDurationMs") or 0))
            aggregate["activeMs"] = max(aggregate["activeMs"], int(event.get("activeMs") or 0))
            aggregate["visibleMs"] = max(aggregate["visibleMs"], int(event.get("visibleMs") or 0))
            aggregate["deviceType"] = aggregate["deviceType"] or _normalize_string(event.get("deviceType"))
            aggregate["timezone"] = aggregate["timezone"] or _normalize_string(event.get("timezone"))

        event_type = _normalize_string(event.get("eventType"))
        if event_type == "page_view":
            total_page_views += 1
            path_counts[_coalesce(event.get("path"), "/")] += 1
        elif event_type == "section_view":
            total_section_views += 1
            section_counts[_coalesce(event.get("section"), event.get("label"), "unknown")] += 1
        elif event_type == "command_completed":
            total_commands += 1
            total_command_ms += int(event.get("commandDurationMs") or 0)
            total_typing_ms += int(event.get("typingDurationMs") or 0)
            if _normalize_string(event.get("status")).lower() == "success":
                success_commands += 1

    for aggregate in sessions.values():
        device_counts[_coalesce(aggregate["deviceType"], "unknown")] += 1
        timezone_counts[_coalesce(aggregate["timezone"], "unknown")] += 1

    total_sessions = len(sessions)
    total_session_ms = sum(int(item["durationMs"]) for item in sessions.values())
    total_active_ms = sum(int(item["activeMs"]) for item in sessions.values())
    success_rate = int(round((success_commands / total_commands) * 100)) if total_commands else 0

    return _json_response(
        {
            "ok": True,
            "summary": {
                "totalEvents": len(events),
                "totalSessions": total_sessions,
                "totalPageViews": total_page_views,
                "totalSectionViews": total_section_views,
                "totalCommands": total_commands,
                "avgSessionMs": _average_ms(total_session_ms, total_sessions),
                "avgActiveMs": _average_ms(total_active_ms, total_sessions),
                "avgCommandMs": _average_ms(total_command_ms, total_commands),
                "avgTypingMs": _average_ms(total_typing_ms, total_commands),
                "commandSuccessRate": success_rate,
                "topPaths": _top_counts(path_counts, 8),
                "topSections": _top_counts(section_counts, 8),
                "deviceBreakdown": _top_counts(device_counts, 8),
                "timezoneBreakdown": _top_counts(timezone_counts, 8),
            },
        }
    )


def _handle_timeseries(req: https_fn.Request) -> https_fn.Response:
    filters = _parse_filters(req)
    events = _load_events(filters, 6000)

    interval = "day"
    if filters["from_dt"] and filters["to_dt"] and (filters["to_dt"] - filters["from_dt"]) <= timedelta(hours=48):
        interval = "hour"

    points: dict[str, dict[str, Any]] = {}
    session_sets: dict[str, set[str]] = defaultdict(set)

    for event in events:
        occurred_at = event.get("occurredAt") or _now()
        if interval == "hour":
            bucket = occurred_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:00")
        else:
            bucket = occurred_at.astimezone(timezone.utc).strftime("%Y-%m-%d")

        point = points.setdefault(bucket, {"bucket": bucket, "events": 0, "sessions": 0, "commands": 0, "pageViews": 0})
        point["events"] += 1
        if event.get("eventType") == "page_view":
            point["pageViews"] += 1
        if event.get("eventType") == "command_completed":
            point["commands"] += 1
        session_id = _normalize_string(event.get("sessionId"))
        if session_id:
            session_sets[bucket].add(session_id)

    rows = sorted(points.values(), key=lambda item: item["bucket"])
    for row in rows:
        row["sessions"] = len(session_sets[row["bucket"]])

    return _json_response({"ok": True, "interval": interval, "points": rows})


def _handle_commands(req: https_fn.Request) -> https_fn.Response:
    filters = _parse_filters(req)
    filters["event_type"] = ""
    events = _load_events(filters, 6000)

    aggregates: dict[str, dict[str, int]] = {}
    recent: list[dict[str, Any]] = []

    for event in events:
        if event.get("eventType") != "command_completed":
            continue
        label = _coalesce(event.get("commandName"), event.get("commandFamily"), event.get("command"), "unknown")
        aggregate = aggregates.setdefault(
            label,
            {"count": 0, "sumDuration": 0, "sumTyping": 0, "sumOutput": 0, "successes": 0},
        )
        aggregate["count"] += 1
        aggregate["sumDuration"] += int(event.get("commandDurationMs") or 0)
        aggregate["sumTyping"] += int(event.get("typingDurationMs") or 0)
        aggregate["sumOutput"] += int(event.get("outputChars") or 0)
        if _normalize_string(event.get("status")).lower() == "success":
            aggregate["successes"] += 1
        if len(recent) < 20:
            recent.append(_event_row(event))

    rows: list[dict[str, Any]] = []
    for label, aggregate in aggregates.items():
        count = aggregate["count"]
        rows.append(
            {
                "commandName": label,
                "count": count,
                "avgDurationMs": _average_ms(aggregate["sumDuration"], count),
                "avgTypingMs": _average_ms(aggregate["sumTyping"], count),
                "avgOutputChars": _average_int(aggregate["sumOutput"], count),
                "successRate": int(round((aggregate["successes"] / count) * 100)) if count else 0,
            }
        )
    rows.sort(key=lambda item: (-item["count"], item["commandName"]))

    return _json_response({"ok": True, "topCommands": rows[:10], "recent": recent})


def _handle_sessions(req: https_fn.Request) -> https_fn.Response:
    filters = _parse_filters(req)
    filters["event_type"] = ""
    events = _load_events(filters, MAX_FETCH)

    sessions: dict[str, dict[str, Any]] = {}
    for event in events:
        session_id = _normalize_string(event.get("sessionId"))
        if not session_id:
            continue
        occurred_at = event.get("occurredAt") or _now()
        aggregate = sessions.setdefault(
            session_id,
            {
                "startedAt": occurred_at,
                "lastSeenAt": occurred_at,
                "durationMs": 0,
                "visibleMs": 0,
                "activeMs": 0,
                "deviceType": "",
                "path": "",
                "timezone": "",
                "views": 0,
                "commands": 0,
                "lastCommand": "",
            },
        )
        if occurred_at < aggregate["startedAt"]:
            aggregate["startedAt"] = occurred_at
        if occurred_at > aggregate["lastSeenAt"]:
            aggregate["lastSeenAt"] = occurred_at
        aggregate["durationMs"] = max(aggregate["durationMs"], int(event.get("sessionDurationMs") or 0))
        aggregate["visibleMs"] = max(aggregate["visibleMs"], int(event.get("visibleMs") or 0))
        aggregate["activeMs"] = max(aggregate["activeMs"], int(event.get("activeMs") or 0))
        aggregate["deviceType"] = aggregate["deviceType"] or _normalize_string(event.get("deviceType"))
        aggregate["path"] = aggregate["path"] or _normalize_string(event.get("path"))
        aggregate["timezone"] = aggregate["timezone"] or _normalize_string(event.get("timezone"))

        event_type = event.get("eventType")
        if event_type in {"page_view", "section_view"}:
            aggregate["views"] += 1
        elif event_type == "command_completed":
            aggregate["commands"] += 1
            aggregate["lastCommand"] = _coalesce(event.get("command"), event.get("commandName"), aggregate["lastCommand"])

    rows = [
        {
            "sessionId": session_id,
            "startedAt": aggregate["startedAt"],
            "lastSeenAt": aggregate["lastSeenAt"],
            "durationMs": aggregate["durationMs"],
            "visibleMs": aggregate["visibleMs"],
            "activeMs": aggregate["activeMs"],
            "deviceType": aggregate["deviceType"],
            "path": aggregate["path"],
            "timezone": aggregate["timezone"],
            "views": aggregate["views"],
            "commands": aggregate["commands"],
            "lastCommand": aggregate["lastCommand"],
        }
        for session_id, aggregate in sessions.items()
    ]
    rows.sort(key=lambda item: item["lastSeenAt"], reverse=True)
    return _json_response({"ok": True, "sessions": rows[: filters["limit"]]})


def _handle_heatmap(req: https_fn.Request) -> https_fn.Response:
    filters = _parse_filters(req)
    filters["event_type"] = ""
    events = _load_events(filters, MAX_FETCH)

    cols = _parse_positive_int(req.args.get("cols"), DEFAULT_HEATMAP_COLS)
    rows = _parse_positive_int(req.args.get("rows"), DEFAULT_HEATMAP_ROWS)
    grid: dict[tuple[int, int], int] = defaultdict(int)
    total_clicks = 0
    max_count = 0

    for event in events:
        if event.get("eventType") != "click":
            continue
        x_norm = _clamp_01(event.get("xNorm"))
        y_norm = _clamp_01(event.get("yNorm"))
        col = min(cols - 1, int(math.floor(x_norm * cols)))
        row = min(rows - 1, int(math.floor(y_norm * rows)))
        grid[(col, row)] += 1
        total_clicks += 1
        max_count = max(max_count, grid[(col, row)])

    cells = [
        {"col": col, "row": row, "count": count}
        for (col, row), count in sorted(grid.items(), key=lambda item: item[1], reverse=True)
    ]
    return _json_response(
        {
            "ok": True,
            "cols": cols,
            "rows": rows,
            "totalClicks": total_clicks,
            "maxCount": max_count,
            "cells": cells,
        }
    )


def _handle_events(req: https_fn.Request) -> https_fn.Response:
    filters = _parse_filters(req)
    events = _load_events(filters, min(filters["limit"] * 3, 900))
    rows = [_event_row(event) for event in events[: filters["limit"]]]
    return _json_response({"ok": True, "events": rows, "limit": filters["limit"]})


@https_fn.on_request(
    region="asia-south1",
    cors=options.CorsOptions(
        cors_origins="*",
        cors_methods=["get", "post", "options"],
    )
)
def analytics_api(req: https_fn.Request) -> https_fn.Response:
    endpoint = _endpoint_name(req)

    try:
        if endpoint == "root":
            return _handle_root()
        if endpoint == "ingest":
            if req.method.upper() != "POST":
                return _json_response({"ok": False, "error": "use POST for ingest"}, 405)
            return _handle_ingest(req)
        if endpoint == "summary":
            return _handle_summary(req)
        if endpoint == "timeseries":
            return _handle_timeseries(req)
        if endpoint == "commands":
            return _handle_commands(req)
        if endpoint == "sessions":
            return _handle_sessions(req)
        if endpoint == "heatmap":
            return _handle_heatmap(req)
        if endpoint == "events":
            return _handle_events(req)
        return _json_response({"ok": False, "error": f"unknown endpoint {endpoint!r}"}, 404)
    except Exception as exc:
        return _json_response({"ok": False, "error": str(exc)}, 500)
