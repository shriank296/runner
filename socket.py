"""Helpers for stream-based Unix domain socket message exchange."""

from __future__ import annotations

import socket

from brit.upp.runner import exceptions
from brit.upp.runner.messages import decode_message, encode_message

DEFAULT_MAX_FRAME_BYTES = 64 * 1024


class SocketMessageReader:
    """Buffered reader for newline-delimited JSON socket frames."""

    def __init__(
        self,
        connection: socket.socket,
        *,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    ) -> None:
        self.connection = connection
        self.max_frame_bytes = max_frame_bytes
        self._buffer = bytearray()

    def recv_message(self) -> dict[str, object]:
        """Read a single newline-delimited JSON message from the peer."""
        while True:
            newline_index = self._buffer.find(b"\n")
            if newline_index >= 0:
                frame = bytes(self._buffer[: newline_index + 1])
                del self._buffer[: newline_index + 1]
                return decode_message(frame)

            data = self.connection.recv(4096)
            if not data:
                raise exceptions.WorkerDisconnectedError("socket closed")

            self._buffer.extend(data)
            if len(self._buffer) > self.max_frame_bytes:
                msg = f"protocol frame exceeds {self.max_frame_bytes} bytes"
                raise exceptions.ProtocolError(msg)


def send_message(connection: socket.socket, message: dict[str, object]) -> None:
    """Send a newline-delimited JSON message to the peer."""
    connection.sendall(encode_message(message))


def recv_message(connection: socket.socket) -> dict[str, object]:
    """Read a single newline-delimited JSON message from the peer."""
    return SocketMessageReader(connection).recv_message()
