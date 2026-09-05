"""Helpers for newline-delimited JSON control messages."""

from __future__ import annotations

import json
from typing import Any

from brit.upp.runner import exceptions


def encode_message(message: dict[str, Any]) -> bytes:
    """Serialize a protocol message as newline-delimited JSON."""
    return json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n"


def decode_message(frame: bytes) -> dict[str, Any]:
    """Parse a newline-delimited JSON protocol message."""
    if not frame.endswith(b"\n"):
        msg = "protocol frames must end with a newline"
        raise exceptions.ProtocolError(msg)

    try:
        decoded = json.loads(frame.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise exceptions.ProtocolError("invalid JSON frame") from exc

    if not isinstance(decoded, dict):
        msg = "protocol frames must decode to JSON objects"
        raise exceptions.ProtocolError(msg)

    return decoded
