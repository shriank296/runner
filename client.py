"""Synchronous supervisor-side client for a single worker."""

from __future__ import annotations

import logging
import socket
import time
from pathlib import Path
from typing import Any

from brit.upp.runner import exceptions
from brit.upp.runner.protocol import ABI_VERSION, DEFAULT_TIMEOUT_SECONDS
from brit.upp.runner.shm import SharedMemoryRegion
from brit.upp.runner.socket import SocketMessageReader, send_message

log = logging.getLogger(__name__)


class WorkerClient:
    """Client that controls a worker through UDS and shared memory."""

    def __init__(
        self,
        *,
        socket_path: Path,
        request_region: SharedMemoryRegion,
        response_region: SharedMemoryRegion,
    ) -> None:
        self.socket_path = socket_path
        self.request_region = request_region
        self.response_region = response_region
        self._connection: socket.socket | None = None
        self._reader: SocketMessageReader | None = None

    def connect(self, *, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            connection: socket.socket | None = None
            try:
                connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                connection.settimeout(max(deadline - time.monotonic(), 0.001))
                connection.connect(str(self.socket_path))
            except FileNotFoundError:
                if connection is not None:
                    connection.close()
                log.debug(
                    "client_connect_retry socket_path=%s reason=FileNotFoundError",
                    self.socket_path,
                )
                time.sleep(0.05)
                continue
            except ConnectionRefusedError:
                if connection is not None:
                    connection.close()
                log.debug(
                    "client_connect_retry socket_path=%s reason=ConnectionRefusedError",
                    self.socket_path,
                )
                time.sleep(0.05)
                continue
            except TimeoutError:
                if connection is not None:
                    connection.close()
                log.debug(
                    "client_connect_retry socket_path=%s reason=TimeoutError",
                    self.socket_path,
                )
                time.sleep(0.05)
                continue
            except OSError:
                if connection is not None:
                    connection.close()
                log.debug(
                    "client_connect_retry socket_path=%s reason=OSError",
                    self.socket_path,
                )
                time.sleep(0.05)
                continue

            self._connection = connection
            self._reader = SocketMessageReader(connection)
            connection.settimeout(None)
            ready = self._reader.recv_message()
            self._validate_ready_message(ready)
            log.info("client_connected socket_path=%s", self.socket_path)
            return ready

        msg = f"timed out connecting to worker socket {self.socket_path}"
        raise TimeoutError(msg)

    def execute(
        self,
        payload: bytes,
        *,
        req_id: str,
        model: str,
        version: str,
        timeout_ms: int = 600000,
    ) -> bytes:
        _start = time.monotonic()
        connection = self._require_connection()
        log.info(
            "client_execute_start req_id=%s model=%s version=%s",
            req_id,
            model,
            version,
        )
        self.request_region.write_payload(payload)
        send_message(
            connection,
            {
                "cmd": "execute",
                "req_id": req_id,
                "model": model,
                "version": version,
                "timeout_ms": timeout_ms,
            },
        )
        try:
            response = self._recv_with_timeout(timeout_ms / 1000)
        except TimeoutError:
            duration_ms = (time.monotonic() - _start) * 1000
            log.warning(
                "worker_execute_timeout req_id=%s socket_path=%s timeout_ms=%s",
                req_id,
                self.socket_path,
                timeout_ms,
            )
            log.info(
                "client_execute_failed req_id=%s duration_ms=%.0f error=timeout",
                req_id,
                duration_ms,
            )
            raise
        duration_ms = (time.monotonic() - _start) * 1000
        if response.get("event") == "failed":
            error_msg = str(response.get("error", "worker execution failed"))
            log.info(
                "client_execute_failed req_id=%s duration_ms=%.0f error=%s",
                req_id,
                duration_ms,
                error_msg,
            )
            raise exceptions.WorkerExecutionError(
                error_msg,
                retryable=bool(response.get("retryable", False)),
            )
        if response.get("event") != "complete":
            msg = f"unexpected worker response: {response}"
            raise exceptions.ProtocolError(msg)
        result = self.response_region.read_payload()
        log.info(
            "client_execute_complete req_id=%s duration_ms=%.0f",
            req_id,
            duration_ms,
        )
        return result

    def shutdown(self, *, reason: str) -> None:
        connection = self._require_connection()
        log.debug("client_shutdown reason=%s", reason)
        send_message(connection, {"cmd": "shutdown", "reason": reason})

    def cancel(self, *, req_id: str) -> None:
        connection = self._require_connection()
        log.debug("client_cancel req_id=%s", req_id)
        send_message(connection, {"cmd": "cancel", "req_id": req_id})

    def close(self) -> None:
        log.debug("client_close socket_path=%s", self.socket_path)
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._reader = None
        self.request_region.close()
        self.response_region.close()

    def _require_connection(self) -> socket.socket:
        if self._connection is None:
            raise exceptions.ProtocolError("worker client is not connected")
        return self._connection

    def _recv_with_timeout(self, timeout_seconds: float) -> dict[str, Any]:
        connection = self._require_connection()
        if self._reader is None:
            raise exceptions.ProtocolError("worker client reader is not initialised")

        previous_timeout = connection.gettimeout()
        connection.settimeout(timeout_seconds)
        try:
            return self._reader.recv_message()
        except socket.timeout as exc:
            raise TimeoutError("timed out waiting for worker response") from exc
        finally:
            connection.settimeout(previous_timeout)

    def _validate_ready_message(self, message: dict[str, Any]) -> None:
        if message.get("event") != "ready":
            raise exceptions.ProtocolError(f"expected ready event, got: {message}")
        if int(message.get("abi_version", -1)) != ABI_VERSION:
            raise exceptions.ProtocolError(
                f"unsupported worker ABI version: {message.get('abi_version')}"
            )
        if not isinstance(message.get("model"), str) or not message["model"]:
            raise exceptions.ProtocolError("ready event missing model")
        if not isinstance(message.get("version"), str) or not message["version"]:
            raise exceptions.ProtocolError("ready event missing version")
        if (
            not isinstance(message.get("request_shm"), str)
            or not message["request_shm"]
        ):
            raise exceptions.ProtocolError("ready event missing request_shm")
        if (
            not isinstance(message.get("response_shm"), str)
            or not message["response_shm"]
        ):
            raise exceptions.ProtocolError("ready event missing response_shm")
