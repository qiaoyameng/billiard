try:
    from multiprocessing import (
        ProcessError,
        BufferTooShort,
        TimeoutError,
        AuthenticationError,
    )
except ImportError:
    class ProcessError(Exception):             # noqa
        pass

    class BufferTooShort(ProcessError):        # noqa
        pass

    class TimeoutError(ProcessError):          # noqa
        pass

    class AuthenticationError(ProcessError):   # noqa
        pass


class TimeLimitExceeded(Exception):
    """The time limit has been exceeded and the job has been terminated."""

    def __str__(self):
        return "TimeLimitExceeded%s" % (self.args, )


class SoftTimeLimitExceeded(Exception):
    """The soft time limit has been exceeded. This exception is raised
    to give the task a chance to clean up."""

    def __str__(self):
        return "SoftTimeLimitExceeded%s" % (self.args, )


class WorkerLostError(Exception):
    """The worker processing a job has exited prematurely."""


class PoolNotReadyError(Exception):
    """The pool has not completed worker startup."""


class PoolClosedError(Exception):
    """The pool was closed while an operation was waiting."""


class WorkerInitializationError(Exception):
    """A worker failed to run its initializer."""

    def __init__(self, diagnostics=None, pid=None, index=None, attempt=None):
        self.diagnostics = diagnostics or {}
        self.pid = pid
        self.index = index
        self.attempt = attempt
        message = (
            self.diagnostics.get('message')
            if isinstance(self.diagnostics, dict)
            else str(self.diagnostics)
        ) or 'worker initialization failed'
        super().__init__(message)


class Terminated(Exception):
    """The worker processing a job has been terminated by user request."""


class RestartFreqExceeded(Exception):
    """Restarts too fast."""


class CoroStop(Exception):
    """Coroutine exit, as opposed to StopIteration which may
    mean it should be restarted."""
    pass
