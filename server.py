"""Synchronous worker-side server for the local IPC protocol."""

from __future__ import annotations

import logging
import os
import queue
import socket
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, cast

from brit.upp.runner import exceptions
from brit.upp.runner.protocol import ABI_VERSION
from brit.upp.runner.shm import SharedMemoryRegion
from brit.upp.runner.socket import SocketMessageReader, send_message

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RequestContext:
    """Metadata supplied with each execute command."""

    req_id: str
    model: str
    version: str
    timeout_ms: int
    cancelled: threading.Event


WorkerHandler = Callable[[bytes, RequestContext], bytes]


class WorkerServer:
    """A single-threaded worker server bound to one Unix domain socket."""

    def __init__(
        self,
        *,
        socket_path: Path,
        request_region: SharedMemoryRegion,
        response_region: SharedMemoryRegion,
        model: str,
        version: str,
        handler: WorkerHandler,
    ) -> None:
        self.socket_path = socket_path
        self.request_region = request_region
        self.response_region = response_region
        self.model = model
        self.version = version
        self.handler = handler
        self._server_socket: socket.socket | None = None
        self._active_connection: socket.socket | None = None
        self._closed: threading.Event = threading.Event()
        self._serving = False
        self._resources_closed = False
        self._active_request_id: str | None = None
        self._active_cancel_event: threading.Event | None = None
        self._response_queue: queue.Queue[dict[str, object]] = queue.Queue()
        self._execution_thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def serve_forever(self) -> None:
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        if self.socket_path.exists():
            self.socket_path.unlink()

        self._serving = True
        log.info(
            "server_start socket_path=%s model=%s version=%s",
            self.socket_path,
            self.model,
            self.version,
        )
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server_socket:
                self._server_socket = server_socket
                server_socket.bind(str(self.socket_path))
                if self.socket_path.exists():
                    os.chmod(str(self.socket_path), 0o600)
                server_socket.listen(socket.SOMAXCONN)
                while not self._closed.is_set():
                    try:
                        connection, _ = server_socket.accept()
                    except OSError:
                        if self._closed.is_set():
                            break
                        raise

                    with connection:
                        self._active_connection = connection
                        log.info("worker_connected")
                        reader = SocketMessageReader(connection)
                        connection.settimeout(0.05)
                        send_message(
                            connection,
                            {
                                "event": "ready",
                                "model": self.model,
                                "version": self.version,
                                "abi_version": ABI_VERSION,
                                "request_shm": str(self.request_region.path),
                                "response_shm": str(self.response_region.path),
                            },
                        )
                        while not self._closed.is_set():
                            self._drain_response_queue(connection)
                            try:
                                message = reader.recv_message()
                            except exceptions.WorkerDisconnectedError:
                                log.info("worker_disconnected")
                                break
                            except socket.timeout:
                                continue
                            except OSError:
                                if self._closed.is_set():
                                    break
                                raise
                            try:
                                command = message.get("cmd")
                                if command == "execute":
                                    self._start_execute(message)
                                elif command == "cancel":
                                    self._handle_cancel(message)
                                elif command == "shutdown":
                                    log.debug(
                                        "server_shutdown socket_path=%s reason=%s",
                                        self.socket_path,
                                        message.get("reason"),
                                    )
                                    self._closed.set()
                                    break
                                elif command == "ping":
                                    send_message(connection, {"event": "pong"})
                                else:
                                    msg = f"unknown command: {command}"
                                    raise exceptions.ProtocolError(msg)
                            except exceptions.ProtocolError as exc:
                                log.warning(
                                    "worker_protocol_error req_id=%s error=%s",
                                    str(message.get("req_id", "")),
                                    str(exc),
                                )
                                try:
                                    send_message(
                                        connection,
                                        {
                                            "event": "failed",
                                            "req_id": str(message.get("req_id", "")),
                                            "error": str(exc),
                                            "retryable": False,
                                        },
                                    )
                                except OSError:
                                    break

                        self._active_connection = None
        finally:
            self._serving = False
            self._close_resources()

    def close(self) -> None:
        log.debug("server_close socket_path=%s", self.socket_path)
        self._closed.set()
        if self._server_socket is not None:
            self._server_socket.close()
            self._server_socket = None
        if self._active_connection is not None:
            self._active_connection.close()
            self._active_connection = None
        if not self._serving:
            self._close_resources()

    def shutdown(self, *, timeout: float = 30.0) -> None:
        log.debug(
            "server_shutdown socket_path=%s timeout=%.1f", self.socket_path, timeout
        )
        self._closed.set()
        with self._lock:
            if self._active_cancel_event is not None:
                self._active_cancel_event.set()
        if self._server_socket is not None:
            self._server_socket.close()
            self._server_socket = None
        if self._active_connection is not None:
            self._active_connection.close()
            self._active_connection = None
        exec_thread = self._execution_thread
        if exec_thread is not None and exec_thread.is_alive():
            exec_thread.join(timeout=timeout)
        self._close_resources()

    def _start_execute(self, message: dict[str, object]) -> None:
        self._validate_execute_message(message)
        cancel_event = threading.Event()
        timeout_ms = cast(int, message["timeout_ms"])
        context = RequestContext(
            req_id=str(message["req_id"]),
            model=str(message["model"]),
            version=str(message["version"]),
            timeout_ms=timeout_ms,
            cancelled=cancel_event,
        )

        with self._lock:
            if self._execution_thread is not None and self._execution_thread.is_alive():
                raise exceptions.ProtocolError("worker already has an active request")
            self._active_request_id = context.req_id
            self._active_cancel_event = cancel_event
            self._execution_thread = threading.Thread(
                target=self._execute_in_thread,
                args=(context,),
                daemon=True,
            )
            self._execution_thread.start()

        log.info(
            "worker_request_start req_id=%s model=%s",
            context.req_id,
            context.model,
        )

    def _validate_execute_message(self, message: dict[str, object]) -> None:
        required_string_fields = ("req_id", "model", "version")
        for field in required_string_fields:
            value = message.get(field)
            if not isinstance(value, str) or not value:
                raise exceptions.ProtocolError(f"execute command missing {field}")

        timeout_ms = message.get("timeout_ms")
        if not isinstance(timeout_ms, int):
            raise exceptions.ProtocolError("execute command missing timeout_ms")

    def _handle_cancel(self, message: dict[str, object]) -> None:
        req_id = message.get("req_id")
        if not isinstance(req_id, str) or not req_id:
            raise exceptions.ProtocolError("cancel command missing req_id")
        with self._lock:
            if (
                self._active_request_id == req_id
                and self._active_cancel_event is not None
            ):
                self._active_cancel_event.set()
                log.info("worker_cancelled req_id=%s", req_id)

    def _execute_in_thread(self, context: RequestContext) -> None:
        try:
            payload = self.request_region.read_payload()
            log.debug(
                "server_execute_payload_read req_id=%s payload_size=%d",
                context.req_id,
                len(payload),
            )
            response = self.handler(payload, context)
            self.response_region.write_payload(response)
        except Exception as exc:  # pragma: no cover - broad to report worker failure
            log.info(
                "worker_request_failed req_id=%s error=%s",
                context.req_id,
                str(exc),
            )
            self._response_queue.put(
                {
                    "event": "failed",
                    "req_id": context.req_id,
                    "error": str(exc),
                    "retryable": False,
                }
            )
            return

        log.info("worker_request_complete req_id=%s", context.req_id)
        self._response_queue.put({"event": "complete", "req_id": context.req_id})

    def _drain_response_queue(self, connection: socket.socket) -> None:
        while True:
            try:
                message = self._response_queue.get_nowait()
            except queue.Empty:
                return

            try:
                send_message(connection, message)
            except OSError:
                return
            finally:
                with self._lock:
                    self._active_request_id = None
                    self._active_cancel_event = None
                    self._execution_thread = None

    def _close_resources(self) -> None:
        if self._resources_closed:
            return
        self._resources_closed = True
        log.debug("server_close_resources socket_path=%s", self.socket_path)
        if self.socket_path.exists():
            self.socket_path.unlink()
        self.request_region.destroy()
        self.response_region.destroy()
