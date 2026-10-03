import threading


class ProgressEvent:
    __slots__ = ('job', 'pid', 'seq', 'payload', 'size')

    def __init__(self, job, pid, seq, payload, size):
        self.job = job
        self.pid = pid
        self.seq = seq
        self.payload = payload
        self.size = size

    def __repr__(self):
        return (
            f'<ProgressEvent job={self.job!r} pid={self.pid!r} '
            f'seq={self.seq!r} size={self.size!r}>'
        )


class ProgressGap:
    __slots__ = ('start_seq', 'end_seq', 'reason')

    def __init__(self, start_seq, end_seq, reason):
        self.start_seq = start_seq
        self.end_seq = end_seq
        self.reason = reason

    def __repr__(self):
        return (
            f'<ProgressGap start={self.start_seq!r} '
            f'end={self.end_seq!r} reason={self.reason!r}>'
        )


_state = threading.local()


def report_progress(payload):
    reporter = getattr(_state, 'reporter', None)
    if reporter is None:
        return
    reporter.report_progress(payload)
