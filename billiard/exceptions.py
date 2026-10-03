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


class PoolNotReady(Exception):
    """The pool has not finished initializing and rejects task submission."""


class WorkerInitError(Exception):
    """One or more worker processes failed to run their initializer.

    The ``diagnostics`` attribute holds a list of ``(pid, info)`` tuples
    describing each failure, where ``info`` is a picklable
    :class:`billiard.einfo.ExceptionInfo` or a plain string.
    """

    def __init__(self, msg, diagnostics=()):
        super().__init__(msg)
        self.diagnostics = list(diagnostics)


class Terminated(Exception):
    """The worker processing a job has been terminated by user request."""


class RestartFreqExceeded(Exception):
    """Restarts too fast."""


class CoroStop(Exception):
    """Coroutine exit, as opposed to StopIteration which may
    mean it should be restarted."""
    pass
