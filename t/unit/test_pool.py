import os
import signal
import threading
import time

import pytest

import billiard.pool
from billiard import ProgressEvent, ProgressGap, report_progress
from billiard import get_context
from billiard.exceptions import TimeLimitExceeded


def func(x):
    if x == 2:
        raise ValueError
    return x


def get_on_ready_count():
    import inspect
    worker = inspect.stack()[1].frame.f_locals['self']
    return worker.on_ready_counter.value

def simple_task(x):
    return x * 2


def progress_task(count):
    for seq in range(1, count + 1):
        report_progress({'seq': seq})
    return 'done'


def bad_progress_task():
    report_progress({'seq': 1})
    return 'real-result'


def progress_bytes_task():
    report_progress({'value': 'small'})
    report_progress({'value': 'x' * 128})
    report_progress({'value': 'tiny2'})
    return 'byte-result'


class UnpicklableProgress:
    def __reduce__(self):
        raise TypeError('cannot send progress')


def progress_unpicklable_task():
    report_progress({'value': UnpicklableProgress()})
    report_progress({'value': 'after-bad'})
    return 'unpicklable-result'

def raise_base_exception():
    raise BaseException("base exception test")


def raise_system_exit():
    raise SystemExit(1)

class test_pool:
    def test_memory_error_from_callback_propagates(self):
        def callback(value):
            raise MemoryError(value)

        result = billiard.pool.ApplyResult({}, callback)

        with pytest.raises(MemoryError, match='out of memory'):
            result._set(None, (True, 'out of memory'))

    def test_raises(self):
        pool = billiard.pool.Pool()
        assert pool.did_start_ok() is True
        pool.close()
        pool.terminate()

    def test_timeout_handler_iterates_with_cache(self):
        # Given a pool
        pool = billiard.pool.Pool()
        # If I have a cache containing async results
        cache = {n: pool.apply_async(n) for n in range(4)}
        # And a TimeoutHandler with that cache
        timeout_handler = pool.TimeoutHandler(pool._pool, cache, 0, 0)
        # If I call to handle the timeouts I expect no exception
        next(timeout_handler.handle_timeouts())

    def test_exception_traceback_present(self):
        pool = billiard.pool.Pool(1)
        results = [pool.apply_async(func, (i,)) for i in range(3)]

        time.sleep(1)
        pool.close()
        pool.join()
        pool.terminate()

        for i, res in enumerate(results):
            if i == 2:
                with pytest.raises(ValueError):
                    res.get()

    def test_base_exception_propagates(self):
        pool = billiard.pool.Pool(1)
        result = pool.apply_async(raise_base_exception)

        with pytest.raises(BaseException, match="base exception test"):
            result.get(timeout=10)

        pool.close()
        pool.join()
        pool.terminate()

    def test_system_exit_from_task_replaces_worker(self):
        pool = billiard.pool.Pool(1)
        try:
            pid_before = pool.apply_async(os.getpid).get(timeout=10)
            result = pool.apply_async(raise_system_exit)
            with pytest.raises(SystemExit):
                result.get(timeout=10)
            pid_after = pool.apply_async(os.getpid).get(timeout=10)
            assert pid_after != pid_before
        finally:
            pool.terminate()
            pool.join()

    def test_hard_timeout_does_not_stall_pool(self):
        pool = billiard.pool.Pool(1, timeout=1)
        try:
            pid_before = pool.apply_async(os.getpid).get(timeout=10)
            result = pool.apply_async(time.sleep, (30,))
            with pytest.raises(Exception) as excinfo:
                result.get(timeout=10)
            exc = getattr(excinfo.value, 'exc', excinfo.value)
            assert isinstance(exc, TimeLimitExceeded)
            # Only submit again once the killed worker has been replaced;
            # otherwise the job may still be picked up by the dying worker.
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if pid_before not in [w.pid for w in pool._pool]:
                    break
                time.sleep(0.1)
            assert pool.apply_async(simple_task, (21,)).get(timeout=10) == 42
        except BaseException:
            # A stalled pool cannot be terminated either (terminate() blocks
            # on the same inqueue lock), so kill the workers outright and
            # skip the terminate-at-exit finalizer.
            for worker in pool._pool:
                try:
                    os.kill(worker.pid, signal.SIGKILL)
                except OSError:
                    pass
            pool._terminate.cancel()
            raise
        pool.terminate()
        pool.join()

    def test_on_ready_counter_is_synchronized(self):
        for ctx in ('spawn', 'fork', 'forkserver'):
            pool = billiard.pool.Pool(processes=1, context=get_context(ctx))
            pool.apply_async(func, (1,)).get(1)
            on_ready_counter = pool.apply_async(get_on_ready_count, ).get(1)
            assert on_ready_counter == 1
            pool.close()
            pool.join()
            pool.terminate()

    def test_graceful_shutdown_delivers_results(self):
        """Test that queued results are delivered during pool shutdown.
        
        Specifically, this test verifies that when _terminate_pool() is called,
        the ResultHandler.finish_at_shutdown() continues processing results
        that workers have placed in the outqueue.
        """

        # Create pool with threads=False so that the result handler thread does
        # not start and the task results are allowed to build up in the queue.
        pool = billiard.pool.Pool(processes=2, threads=False)

        # Submit tasks so that results are queued but not processed.
        results = [pool.apply_async(simple_task, (i,)) for i in range(8)]

        # Allow a small amount of time for tasks to complete.
        time.sleep(0.5)

        # Close and join the pool to ensure workers stop.
        pool.close()
        pool.join()

        # Call the _terminate_pool() class method to trigger the finish_at_shutdown()
        # function that will process results in the queue. Normally _terminate_pool()
        # is called by a Finalize object when the Pool object is destroyed. We cannot
        # call pool.terminate() here because it will call the Finalize object, which
        # won't do anything until the Pool object is destroyed at the end of this test.
        # We can simulate the shutdown behaviour by calling _terminate_pool() directly.
        billiard.pool.Pool._terminate_pool(
            pool._taskqueue,
            pool._inqueue,
            pool._outqueue,
            pool._pool,
            pool._worker_handler,
            pool._task_handler,
            pool._result_handler,
            pool._cache,
            pool._timeout_handler,
            pool._help_stuff_finish_args()
        )

        # Cancel the Finalize object to prevent _terminate_pool() from being called
        # a second time when the Pool object is destroyed.
        pool._terminate.cancel()

        # Verify that all results were delivered by finish_at_shutdown() and can be
        # retrieved.
        for i, result in enumerate(results):
            assert result.get() == i * 2

    def test_progress_backpressure_reports_gap_and_keeps_result(self):
        pool = billiard.pool.Pool(processes=1)
        gate = threading.Event()
        started = threading.Event()
        received = []

        def on_progress(event):
            if not received:
                started.set()
                gate.wait()
            received.append(event)

        try:
            result = pool.apply_async(
                progress_task,
                (8,),
                progress_callback=on_progress,
                progress_max_pending=3,
            )
            assert result.get(10) == 'done'

            assert started.wait(5)
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                time.sleep(0.01)
            assert received == []

            gate.set()
            result.wait_progress(5)

            assert len(received) == 4
            assert [event.seq for event in received[:3]] == [1, 2, 3]
            gap = received[3]
            assert isinstance(gap, ProgressGap)
            assert gap.start_seq == 4
            assert gap.end_seq == 8
            assert gap.reason == 'progress_buffer_full'
        finally:
            gate.set()
            pool.terminate()
            pool.join()

    def test_progress_callback_exception_keeps_result(self):
        pool = billiard.pool.Pool(processes=1)

        def bad_callback(event):
            raise RuntimeError('progress callback failure')

        try:
            result = pool.apply_async(
                bad_progress_task,
                progress_callback=bad_callback,
            )
            assert result.get(10) == 'real-result'
        finally:
            pool.terminate()
            pool.join()

    def test_progress_oversize_payload_reports_gap(self):
        pool = billiard.pool.Pool(processes=1)
        received = []

        try:
            result = pool.apply_async(
                progress_bytes_task,
                progress_callback=received.append,
                progress_max_bytes=1024,
                progress_max_payload_bytes=64,
            )
            assert result.get(10) == 'byte-result'
            result.wait_progress(5)

            assert received[0].payload == {'value': 'small'}
            assert isinstance(received[1], ProgressGap)
            assert received[1].reason == 'progress_too_large'
            assert received[1].start_seq == 2
            assert received[1].end_seq == 2
        finally:
            pool.terminate()
            pool.join()

    def test_unpicklable_progress_reports_gap(self):
        pool = billiard.pool.Pool(processes=1)
        received = []

        try:
            result = pool.apply_async(
                progress_unpicklable_task,
                progress_callback=received.append,
            )
            assert result.get(10) == 'unpicklable-result'
            result.wait_progress(5)

            assert isinstance(received[0], ProgressGap)
            assert (received[0].start_seq, received[0].end_seq) == (1, 1)
            assert received[0].reason == 'progress_payload_error'
            assert received[1].payload == {'value': 'after-bad'}
            assert received[1].seq == 2
        finally:
            pool.terminate()
            pool.join()

    def test_progress_total_byte_limit_reports_gap(self):
        received = []
        result = billiard.pool.ApplyResult(
            {},
            None,
            progress_callback=received.append,
            progress_max_pending=10,
            progress_max_bytes=30,
        )
        result._offer_progress(ProgressEvent(result._job, 1, 1, 'a', 20))
        result._offer_progress(ProgressEvent(result._job, 1, 2, 'b', 20))
        result._set(None, (True, 'result'))
        result.wait_progress(5)

        assert received[0].seq == 1
        assert isinstance(received[1], ProgressGap)
        assert (received[1].start_seq, received[1].end_seq) == (2, 2)
        assert received[1].reason == 'progress_buffer_full'

    def test_progress_limits_must_be_positive(self):
        with pytest.raises(ValueError, match='max_pending'):
            billiard.pool.ApplyResult(
                {}, None, progress_max_pending=0,
            )
        with pytest.raises(ValueError, match='max_bytes'):
            billiard.pool.ApplyResult(
                {}, None, progress_max_bytes=0,
            )
        with pytest.raises(ValueError, match='max_progress_bytes'):
            billiard.pool.ApplyResult(
                {}, None, progress_max_payload_bytes=0,
            )

    def test_rejects_fewer_than_one_process(self):
        for processes in (0, -1):
            with pytest.raises(ValueError, match="at least 1"):
                billiard.pool.Pool(processes=processes)

    def test_rejects_non_positive_maxtasksperchild(self):
        # Worker only asserted this, so it failed with a bare AssertionError,
        # and under python -O the pool kept replacing workers that exited
        # before running a task.
        for maxtasksperchild in (0, -1, 1.5):
            with pytest.raises(ValueError, match="maxtasksperchild"):
                billiard.pool.Pool(processes=1,
                                   maxtasksperchild=maxtasksperchild)

    def test_rejects_bool_for_numeric_pool_args(self):
        """bool subclasses int; processes=True must not silently spawn 1 worker."""
        for kwargs in (
            {"processes": True},
            {"processes": False},
            {"maxtasksperchild": True},
            {"timeout": True},
            {"soft_timeout": True},
            {"lost_worker_timeout": True},
            {"max_memory_per_child": True},
            {"max_restarts": True},
            {"max_restart_freq": True},
        ):
            with pytest.raises(TypeError, match="bool"):
                billiard.pool.Pool(**kwargs)
