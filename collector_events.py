"""CollectorEvent v1 construction and transport helpers.

This module deliberately has no dependency on the websocket or protobuf layers.
It is the stable boundary between the web collector and downstream consumers.
"""

from __future__ import annotations

import hashlib
import json
import sys
import threading
import time
from typing import Any, Callable, Mapping, Optional, TextIO


SCHEMA_VERSION = "douyin-web-collector-event-v1"


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def normalize_platform_time(value: Any, received_at: int) -> tuple[int, str]:
    """Return a Unix millisecond time without guessing an unknown unit.

    The currently observed ``Common.create_time`` is a 13 digit Unix
    millisecond value. Any other shape is treated as unverified and degraded to
    the local receive time, as required by the integration contract.
    """

    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return received_at, "fallback"

    if 1_000_000_000_000 <= timestamp <= 9_999_999_999_999:
        return timestamp, "platform"
    return received_at, "fallback"


def actor_fields(user: Any) -> tuple[dict, str]:
    nickname = getattr(user, "nick_name", None) or None
    # First-stage protobuf user IDs are not trusted. A future release may add a
    # validated resolver policy; a caller-controlled boolean is intentionally
    # not exposed because permission and semantic stability cannot be inferred.
    actor = {
        "sourceUserId": None,
        "nickname": nickname,
    }
    observed = getattr(user, "id", None)
    if type(observed) is int and 0 < observed <= 18446744073709551615 and observed != 111111:
        actor["observedSourceUserId"] = str(observed)
    sec_uid = getattr(user, "sec_uid", None)
    if isinstance(sec_uid, str) and 0 < len(sec_uid) <= 256 and all(c.isascii() and (c.isalnum() or c in "-_") for c in sec_uid):
        actor["observedSecUid"] = sec_uid
    # Only explicit nonzero observations: protobuf defaults do not establish
    # that a viewer is level zero or does not follow the anchor.
    profile = {}
    follow = getattr(user, 'follow_info', None)
    grade = getattr(user, 'pay_grade', None)
    club = getattr(getattr(user, 'fans_club', None), 'data', None)
    for key, value in (
        ('followStatusRaw', getattr(follow, 'follow_status', None)),
        ('payGradeLevel', getattr(grade, 'level', None)),
        ('fansClubLevel', getattr(club, 'level', None)),
        ('fansClubStatusRaw', getattr(club, 'user_fans_club_status', None)),
    ):
        if type(value) is int and 0 < value <= 100000:
            profile[key] = value
    club_anchor = getattr(club, 'anchor_id', None)
    if type(club_anchor) is int and 0 < club_anchor <= 18446744073709551615 and club_anchor != 111111:
        profile['fansClubAnchorId'] = str(club_anchor)
    if profile:
        actor['observedProfile'] = profile
    return actor, "unavailable"


def build_collector_event(
    *,
    kind: str,
    event_type: str,
    room_id: Any,
    web_rid: Any,
    payload: Mapping[str, Any],
    method: str,
    actor: Optional[Mapping[str, Any]] = None,
    platform_event_id: Any = None,
    platform_occurred_at: Any = None,
    received_at: Optional[int] = None,
    actor_id_quality: str = "unavailable",
) -> dict:
    """Build one CollectorEvent v1 using only contract-approved root fields."""

    if kind not in {"data", "lifecycle"}:
        raise ValueError(f"unsupported CollectorEvent kind: {kind}")
    if not event_type:
        raise ValueError("event_type is required")

    received = int(time.time() * 1000) if received_at is None else int(received_at)
    occurred, occurred_quality = normalize_platform_time(
        platform_occurred_at, received
    )
    normalized_actor = {
        "sourceUserId": None,
        "nickname": None,
    }
    if actor is not None:
        normalized_actor.update(
            {
                "sourceUserId": actor.get("sourceUserId"),
                "nickname": actor.get("nickname"),
            }
        )
        if actor.get("observedSourceUserId") is not None:
            normalized_actor["observedSourceUserId"] = actor["observedSourceUserId"]
        if actor.get("observedSecUid") is not None:
            normalized_actor["observedSecUid"] = actor["observedSecUid"]
        if actor.get("observedProfile"):
            normalized_actor["observedProfile"] = dict(actor["observedProfile"])

    normalized_room_id = str(room_id) if room_id not in (None, "") else None
    normalized_web_rid = str(web_rid)
    normalized_payload = dict(payload)

    if platform_event_id not in (None, 0, "", "0"):
        event_id = str(platform_event_id)
        event_id_quality = "platform"
    else:
        # If platform time was also unavailable, ``occurred`` is the local
        # receive time. The hash is deterministic for this one observation but
        # cannot identify a later retransmission of the same platform event.
        fallback_material = {
            "roomId": normalized_room_id,
            "webRid": normalized_web_rid,
            "type": event_type,
            "occurredAt": occurred,
            "actor": normalized_actor,
            "payload": normalized_payload,
            "method": method,
        }
        digest = hashlib.sha256(
            _canonical_json(fallback_material).encode("utf-8")
        ).hexdigest()
        event_id = f"fallback:{digest}"
        event_id_quality = "fallback"

    return {
        "schemaVersion": SCHEMA_VERSION,
        "kind": kind,
        "eventId": event_id,
        "roomId": normalized_room_id,
        "webRid": normalized_web_rid,
        "type": event_type,
        "occurredAt": occurred,
        "receivedAt": received,
        "actor": normalized_actor,
        "payload": normalized_payload,
        "source": {
            "name": "douyin-web",
            "protocol": "webcast-im",
            "method": method,
        },
        "quality": {
            "eventId": event_id_quality,
            "actorId": actor_id_quality,
            "occurredAt": occurred_quality,
        },
    }


class CallbackSink:
    """Deliver events to an in-process callback, one callback at a time."""

    def __init__(self, callback: Callable[[dict], None]):
        if not callable(callback):
            raise TypeError("callback must be callable")
        self._callback = callback
        self._lock = threading.RLock()

    def emit(self, event: dict) -> None:
        with self._lock:
            self._callback(event)


class NdjsonSink:
    """Write one complete JSON event per line and flush immediately."""

    def __init__(self, stream: Optional[TextIO] = None):
        self._stream = stream if stream is not None else sys.stdout
        self._lock = threading.RLock()

    def emit(self, event: dict) -> None:
        serialized = json.dumps(
            # ASCII-escaped JSON is valid UTF-8 JSON and remains writable when
            # an embedding host supplies a locale-bound stream such as cp1252.
            event, ensure_ascii=True, separators=(",", ":")
        ) + "\n"
        with self._lock:
            self._stream.write(serialized)
            self._stream.flush()
