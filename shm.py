"""File-backed shared memory regions using POSIX-compatible mmap."""

from __future__ import annotations

import logging
import mmap
import os
import struct
import uuid
from pathlib import Path
from typing import Protocol

from brit.upp.runner import exceptions
from brit.upp.runner.protocol import (
    DATA_SIZE_OFFSET,
    FLAGS_OFFSET,
    HEADER_SIZE,
    MAGIC_BYTES,
    MAGIC_OFFSET,
)

log = logging.getLogger(__name__)
_UINT64 = struct.Struct("<Q")
_UINT32 = struct.Struct("<I")


class _FileHandle(Protocol):
    def close(self) -> None: ...  # pragma: no cover

    def fileno(self) -> int: ...  # pragma: no cover


class SharedMemoryRegion:
    """A fixed-capacity payload region backed by a file and ``mmap``."""

    def __init__(
        self,
        path: Path,
        file_handle: _FileHandle | None,
        mapping: mmap.mmap,
        *,
        owns_lease: bool,
    ):
        self.path = path
        self._file_handle = file_handle
        self._mapping = mapping
        self._owns_lease = owns_lease
        self._closed = False
        self._destroyed = False

    @property
    def capacity(self) -> int:
        return len(self._mapping) - HEADER_SIZE

    @classmethod
    def create(cls, path: Path, *, capacity: int) -> "SharedMemoryRegion":
        path.parent.mkdir(parents=True, exist_ok=True)
        lease_path = cls.lease_path(path)
        if path.exists() or lease_path.exists():
            raise exceptions.SharedMemoryError(
                f"shared memory region already exists: {path}"
            )

        size = HEADER_SIZE + capacity
        with path.open("w+b") as handle:
            handle.truncate(size)
        os.chmod(str(path), 0o600)
        file_handle = path.open("r+b")
        mapping = mmap.mmap(file_handle.fileno(), size)
        lease_path.write_text(str(uuid.uuid4()), encoding="utf-8")
        os.chmod(str(lease_path), 0o600)
        region = cls(
            path=path, file_handle=file_handle, mapping=mapping, owns_lease=True
        )
        region._write_size(0)
        region._write_flags(0)
        region._write_magic()
        region._mapping.flush()
        log.info("shm_create path=%s capacity=%d", path, capacity)
        return region

    @classmethod
    def open(cls, path: Path) -> "SharedMemoryRegion":
        lease_path = cls.lease_path(path)
        if not lease_path.exists():
            raise exceptions.SharedMemoryError(
                f"shared memory lease marker is missing for {path}"
            )
        file_handle = path.open("r+b")
        mapping = mmap.mmap(file_handle.fileno(), 0)
        region = cls(
            path=path, file_handle=file_handle, mapping=mapping, owns_lease=False
        )
        region._validate_header()
        log.info("shm_open path=%s", path)
        return region

    @staticmethod
    def lease_path(path: Path) -> Path:
        return path.with_name(f"{path.name}.lease")

    def write_payload(self, payload: bytes) -> None:
        if len(payload) > self.capacity:
            msg = f"payload size {len(payload)} exceeds capacity {self.capacity}"
            raise exceptions.SharedMemoryBoundsError(msg)

        self._mapping.seek(HEADER_SIZE)
        self._mapping.write(payload)
        self._write_size(len(payload))
        self._mapping.flush()
        log.debug("shm_write path=%s size=%d", self.path, len(payload))

    def read_payload(self) -> bytes:
        self._validate_header()
        size = self._read_size()
        if size > self.capacity:
            msg = f"payload size header {size} exceeds capacity {self.capacity}"
            raise exceptions.SharedMemoryError(msg)
        self._mapping.seek(HEADER_SIZE)
        data = self._mapping.read(size)
        log.debug("shm_read path=%s size=%d", self.path, len(data))
        return data

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._mapping.close()
        log.debug("shm_close path=%s", self.path)
        if self._file_handle is not None:
            self._file_handle.close()
            self._file_handle = None

    def destroy(self) -> None:
        self.close()
        if self._destroyed:
            return
        self._destroyed = True
        log.debug("shm_destroy path=%s owns_lease=%s", self.path, self._owns_lease)
        if self._owns_lease:
            lease_path = self.lease_path(self.path)
            if lease_path.exists():
                lease_path.unlink()
            if self.path.exists():
                self.path.unlink()

    def _read_size(self) -> int:
        return _UINT64.unpack_from(self._mapping, DATA_SIZE_OFFSET)[0]

    def _write_size(self, size: int) -> None:
        _UINT64.pack_into(self._mapping, DATA_SIZE_OFFSET, size)

    def _write_flags(self, flags: int) -> None:
        _UINT32.pack_into(self._mapping, FLAGS_OFFSET, flags)

    def _write_magic(self) -> None:
        self._mapping[MAGIC_OFFSET : MAGIC_OFFSET + len(MAGIC_BYTES)] = MAGIC_BYTES

    def _validate_header(self) -> None:
        magic = bytes(self._mapping[MAGIC_OFFSET : MAGIC_OFFSET + len(MAGIC_BYTES)])
        if magic != MAGIC_BYTES:
            raise exceptions.SharedMemoryError("shared memory header magic is invalid")
