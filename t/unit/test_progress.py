"""Tests for opt-in long-task progress reporting in billiard pools.

Progress events use a dedicated ``PROGRESS`` message type, carry job
identity and a per-job sequence number, are bounded on both the worker
and the parent side, and never block the critical ACK/READY path.  When
a consumer pauses and falls behind, pending events are coalesced or
dropped and the missing range is reported as a ``ProgressGap``.
"""
import threading
import time

import billiard.pool as pool_module
from billiard import get_context
from billiard.pool import (
    PROGRESS_COALESCE,
    PROGRESS_DROP_NEWEST,
    ProgressGap,
    _ProgressBuffer,
    report_progress,
)


def _fork_pool(processes=1, **kwargs):
    # spawn/forkserver cannot create unix sockets in the restricted
    # sandbox; fork exercises the same worker/workloop code paths.
    return pool_module.Pool(processes, context=get_context('fork'),
                            **kwargs)


def report_task(n, delay=0.0, _progress=None):
    for i in range(n):
        if _progress is not None:
            _progress(i)
        if delay:
            time.sleep(delay)
    return n * 10


def module_report_task(n):
    for i in range(n):
        report_progress(i)
    return n


def task_without_progress_optin():
    try:
        report_progress(1)
    except RuntimeError:
        return 'runtimeerror'
    return 'noerror'


def exploding_progress_task(_progress=None):
    _progress(1)
    _progress(2)
    return 'ok'


def _drain(buffer, sink):
    thread = threading.Thread(target=lambda: buffer.drain(sink.append))
    thread.start()
    thread.join(2)
    assert not thread.is_alive()
    return sink


class test_worker_reporter_buffer:

    def test_coalesce_reports_single_gap_with_sequence_holes(self):
        delivered = []
        buf = _ProgressBuffer(
            'job', delivered.append, max_events=3,
            max_bytes=10 ** 9, policy=PROGRESS_COALESCE)
        for seq in range(10):
            buf.add(seq, False, seq)
        buf.close()
        delivered = _drain(buf, [])

        # 3 newest events survive plus exactly one coalesced gap marker.
        gaps = [x for x in delivered if isinstance(x, ProgressGap)]
        assert len(gaps) == 1
        assert gaps[0].first_seq == 0
        assert gaps[0].last_seq == 6
        assert gaps[0].count == 7
        assert gaps[0].where == 'parent'
        assert [x for x in delivered
                if not isinstance(x, ProgressGap)] == [7, 8, 9]
        expected = 0
        for item in delivered:
            if isinstance(item, ProgressGap):
                expected = item.last_seq + 1
            else:
                assert item == expected
                expected += 1
        assert expected == 10

    def test_byte_limit_triggers_coalescing(self):
        buf = _ProgressBuffer(
            'job', None, max_events=1000, max_bytes=300,
            policy=PROGRESS_COALESCE)
        for seq in range(20):
            buf.add(seq, False, 'x' * 100)
        buf.close()
        delivered = _drain(buf, [])
        gaps = [x for x in delivered if isinstance(x, ProgressGap)]
        assert gaps
        assert gaps[0].where == 'parent'

    def test_drop_newest_does_not_enqueue_overflowing_event(self):
        buf = _ProgressBuffer(
            'job', None, max_events=2,
            max_bytes=10 ** 9, policy=PROGRESS_DROP_NEWEST)
        for seq in range(10):
            buf.add(seq, False, seq)
        # Newest events are rejected, so the oldest two stay buffered.
        assert [s for s, g, p, z in buf._events] == [0, 1]
        buf.close()
        delivered = _drain(buf, [])
        assert [x for x in delivered if not isinstance(x, ProgressGap)] == \
            [0, 1]
        gap = next(x for x in delivered if isinstance(x, ProgressGap))
        assert gap.first_seq == 2
        assert gap.last_seq == 9

    def test_gap_event_is_not_reinterpreted_as_a_hole(self):
        buf = _ProgressBuffer(
            'job', None, max_events=10, max_bytes=10 ** 9)
        gap = ProgressGap('job', 0, 4, 5, 'worker')
        buf.add(0, True, gap)
        buf.add(5, False, 5)
        buf.close()
        delivered = _drain(buf, [])
        assert delivered == [gap, 5]


class test_pool_progress:

    def test_progress_delivered_before_result(self):
        events = []
        pool = _fork_pool()
        try:
            result = pool.apply_async(
                report_task, (5,), progress_callback=events.append)
            assert result.get(15) == 50
        finally:
            pool.close()
            pool.join()
            pool.terminate()
        assert events == [0, 1, 2, 3, 4]

    def test_module_level_report_progress(self):
        events = []
        pool = _fork_pool()
        try:
            result = pool.apply_async(
                module_report_task, (4,), progress_callback=events.append)
            assert result.get(15) == 4
        finally:
            pool.close()
            pool.join()
            pool.terminate()
        assert events == [0, 1, 2, 3]

    def test_report_progress_without_optin_raises_runtime_error(self):
        pool = _fork_pool()
        try:
            assert pool.apply_async(
                task_without_progress_optin).get(15) == 'runtimeerror'
        finally:
            pool.close()
            pool.join()
            pool.terminate()

    def test_progress_is_zero_cost_without_callback(self):
        pool = _fork_pool()
        try:
            result = pool.apply_async(report_task, (2,))
            assert result.get(15) == 20
            assert result.progress is None
            assert not pool._progress_dispatcher._was_started
        finally:
            pool.close()
            pool.join()
            pool.terminate()

    def test_paused_consumer_then_resume_reports_gap_and_result(self):
        gate = pool_module.threading.Event()
        received = []

        def callback(payload):
            gate.wait()
            received.append(payload)

        pool = _fork_pool()
        try:
            result = pool.apply_async(
                report_task, (10,),
                progress_callback=callback,
                progress_max_events=3,
                progress_max_bytes=10 ** 9)
            handle = result.progress
            handle.pause()

            # The final result must arrive promptly even though the
            # progress consumer is completely stalled.
            assert result.get(20) == 100
            assert received == []

            handle.resume()
            gate.set()

            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if received and handle.gaps():
                    break
                time.sleep(0.05)

            gaps = handle.gaps()
            assert gaps, 'expected a gap description after resume'
            assert all(isinstance(g, ProgressGap) for g in gaps)
            expected = 0
            for item in received:
                if isinstance(item, ProgressGap):
                    assert item.first_seq == expected
                    expected = item.last_seq + 1
                else:
                    assert item == expected
                    expected += 1
            assert expected == 10
        finally:
            pool.close()
            pool.join()
            pool.terminate()

    def test_callback_exception_does_not_affect_result(self):
        def bad_callback(payload):
            raise ValueError('boom in progress callback')

        pool = _fork_pool()
        try:
            result = pool.apply_async(
                exploding_progress_task, progress_callback=bad_callback)
            assert result.get(15) == 'ok'
            followup = pool.apply_async(report_task, (2,))
            assert followup.get(15) == 20
        finally:
            pool.close()
            pool.join()
            pool.terminate()

    def test_late_progress_after_completion_is_discarded(self):
        events = []
        pool = _fork_pool()
        try:
            result = pool.apply_async(
                report_task, (3,), progress_callback=events.append)
            assert result.get(15) == 30
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline and len(events) < 3:
                time.sleep(0.02)
            assert events == [0, 1, 2]
            assert result._job not in pool._cache
        finally:
            pool.close()
            pool.join()
            pool.terminate()


class _FakeOutQueue:
    def __init__(self):
        self.messages = []

    def put(self, message):
        self.messages.append(message)


class test_worker_progress_reporter:

    def _drain_worker(self, reporter):
        reporter.close()
        reporter._feeder.join(2)
        assert not reporter._feeder.is_alive()
        return [args for state, args in reporter.outq.messages
                if state == pool_module.PROGRESS]

    def test_small_volume_flushes_all_events(self):
        reporter = pool_module._WorkerProgressReporter(
            7, _FakeOutQueue(), max_events=10, max_bytes=10 ** 9)
        for i in range(4):
            reporter.report(i)
        messages = self._drain_worker(reporter)
        assert [(job, seq, is_gap, payload)
                for job, seq, is_gap, payload in messages] == [
                    (7, 0, False, 0), (7, 1, False, 1),
                    (7, 2, False, 2), (7, 3, False, 3)]

    def test_overflow_is_coalesced_into_one_worker_gap(self):
        reporter = pool_module._WorkerProgressReporter(
            9, _FakeOutQueue(), max_events=3, max_bytes=10 ** 9)
        for i in range(10):
            reporter.report(i)
        messages = self._drain_worker(reporter)
        gaps = [args for args in messages if args[2]]
        payloads = [args[3] for args in messages if not args[2]]
        assert payloads == [7, 8, 9]
        assert len(gaps) == 1
        gap = gaps[0][3]
        assert isinstance(gap, ProgressGap)
        assert gap.where == 'worker'
        assert gap.first_seq == 0
        assert gap.last_seq == 6
        assert gap.count == 7
        assert all(job == 9 for job, *_ in messages)
