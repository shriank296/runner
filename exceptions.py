"""Exception hierarchy for the local IPC runner protocol."""


class RunnerError(Exception):
    """Base error for the runner package."""


class ProtocolError(RunnerError):
    """Raised when a control message cannot be parsed or validated."""


class SharedMemoryError(RunnerError):
    """Raised for shared memory region failures."""


class SharedMemoryBoundsError(SharedMemoryError):
    """Raised when a payload exceeds the configured segment capacity."""


class WorkerDisconnectedError(RunnerError):
    """Raised when a worker disconnects unexpectedly."""


class WorkerExecutionError(RunnerError):
    """Raised when a worker reports a failed execution."""

    def __init__(self, error: str, *, retryable: bool) -> None:
        super().__init__(error)
        self.error = error
        self.retryable = retryable
