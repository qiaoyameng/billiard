import os
import signal
import threading
import time

import pytest

import billiard.pool
from billiard import get_context
from billiard.exceptions import Cancelled, TimeLimitExceeded
from queue import Empty


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

def raise_base_exception():
    raise BaseException("base exception test")


def raise_system_exit():
    raise SystemExit(1)

# -- helpers for the cancel() tests -------------------------------------

# Per-worker fixture registry, installed by _lab_init in each worker.
_LAB = {}


def _lab_init(started, release, executed):
    _LAB['started'] = started
    _LAB['release'] = release
    _LAB['executed'] = executed


def _lab_blocking_task(name):
    # Signal that the function started, wait for the parent's release
    # event, then record that it executed.
    _LAB['started'].set()
    _LAB['release'].wait(30)
    _LAB['executed'].put(name)
    return name


def _lab_quick_task(name):
    # Record execution immediately: used to prove a task did (not) run.
    _LAB['executed'].put(name)
    return name


def _lab_sleep_task(name):
    # Signal start, then keep the worker busy without holding any
    # shared locks, so the worker can be killed safely mid-task.
    _LAB['started'].set()
    time.sleep(30)
    return name


def _lab_pool(ctx, synack=False, **kwargs):
    started, release, executed = ctx.Event(), ctx.Event(), ctx.Queue()
    pool = billiard.pool.Pool(
        processes=1, synack=synack, context=ctx,
        initializer=_lab_init, initargs=(started, release, executed),
        **kwargs)
    return pool, started, release, executed


@pytest.mark.skipif(not hasattr(os, 'fork'), reason='requires fork')
class test_cancel:
    """Tests for the public ApplyResult.cancel()/MapResult.cancel() API.

    Inter-process events are used to pin tasks at exact lifecycle
    boundaries (dispatched, received, started) instead of relying on
    fixed sleeps.
    """

    def setup_method(self):
        self.ctx = get_context('fork')

    def test_cancel_waiting_task_never_executes(self):
        # cancel wins the race against a queued (dispatched but not yet
        # received) task: the function is guaranteed to never run.
        pool, started, release, executed = _lab_pool(self.ctx, synack=True)
        try:
            blocker = pool.apply_async(_lab_blocking_task, ('blocker',))
            assert started.wait(10)
            victim = pool.apply_async(_lab_quick_task, ('victim',))
            assert victim.cancel() is True
            assert victim.cancel() is True  # idempotent
            assert victim.state == billiard.pool.ApplyResult.CANCELLED
            release.set()
            # the worker must receive (and discard) the victim before it
            # can run the sentinel, so a completed sentinel proves the
            # victim was skipped.
            sentinel = pool.apply_async(_lab_quick_task, ('sentinel',))
            assert sentinel.get(10) == 'sentinel'
            with pytest.raises(Cancelled):
                victim.get(10)
            assert victim.ready()
            assert not victim.successful()
            assert blocker.get(10) == 'blocker'
            assert executed.get(10) == 'blocker'
            assert executed.get(10) == 'sentinel'
            with pytest.raises(Empty):
                executed.get_nowait()
        finally:
            release.set()
            pool.terminate()
            pool.join()

    def test_cancel_racing_ack_at_receive_boundary(self):
        # The accept callback pins the job at the receive boundary: the
        # worker has received the task but is still waiting for the
        # go-ahead.  Cancelling there must NACK the worker.
        pool, started, release, executed = _lab_pool(self.ctx, synack=True)
        acked = threading.Event()
        goahead = threading.Event()

        def on_accept(pid, time_accepted):
            acked.set()
            goahead.wait(30)

        try:
            victim = pool.apply_async(
                _lab_quick_task, ('victim',), accept_callback=on_accept)
            assert acked.wait(10)
            assert victim.state == billiard.pool.ApplyResult.ACKED
            assert victim.cancel() is True
            goahead.set()
            with pytest.raises(Cancelled):
                victim.get(10)
            # the pool is still usable and the victim never ran.
            sentinel = pool.apply_async(_lab_quick_task, ('sentinel',))
            assert sentinel.get(10) == 'sentinel'
            assert executed.get(10) == 'sentinel'
            with pytest.raises(Empty):
                executed.get_nowait()
        finally:
            goahead.set()
            pool.terminate()
            pool.join()

    def test_cancel_after_function_started_fails(self):
        # cancel loses the race once the function started: it returns
        # False and the task runs to completion (worker not killed).
        pool, started, release, executed = _lab_pool(self.ctx)
        try:
            result = pool.apply_async(_lab_blocking_task, ('running',))
            assert started.wait(10)
            assert result.state == billiard.pool.ApplyResult.RUNNING
            assert result.cancel() is False
            release.set()
            assert result.get(10) == 'running'
            assert result.state == billiard.pool.ApplyResult.READY
            assert result.successful()
            # the worker was not killed by the failed cancel.
            assert pool.apply_async(os.getpid).get(10) == \
                result.worker_pids()[0]
        finally:
            release.set()
            pool.terminate()
            pool.join()

    def test_cancel_dispatched_task_without_synack_fails(self):
        # Without synack a task already handed to the work queue cannot
        # be recalled: cancel() returns False and the task executes.
        pool, started, release, executed = _lab_pool(self.ctx)
        try:
            blocker = pool.apply_async(_lab_blocking_task, ('blocker',))
            assert started.wait(10)
            victim = pool.apply_async(_lab_quick_task, ('victim',))
            # wait until the task was written to the work queue.
            deadline = time.monotonic() + 10
            while victim.state == billiard.pool.ApplyResult.PENDING:
                assert time.monotonic() < deadline
                time.sleep(0.01)
            assert victim.state == billiard.pool.ApplyResult.DISPATCHED
            assert victim.cancel() is False
            release.set()
            assert victim.get(10) == 'victim'
            assert blocker.get(10) == 'blocker'
        finally:
            release.set()
            pool.terminate()
            pool.join()

    def test_cancel_finished_task_fails(self):
        pool = billiard.pool.Pool(1)
        try:
            result = pool.apply_async(simple_task, (21,))
            assert result.get(10) == 42
            assert result.cancel() is False
        finally:
            pool.terminate()
            pool.join()

    def test_cancel_then_worker_exit_keeps_cancelled_state(self):
        # cancel racing with worker exit: waiters see exactly one
        # terminal state (Cancelled, not WorkerLostError).
        pool, started, release, executed = _lab_pool(self.ctx, synack=True)
        try:
            blocker = pool.apply_async(_lab_sleep_task, ('blocker',))
            assert started.wait(10)
            victim = pool.apply_async(_lab_quick_task, ('victim',))
            assert victim.cancel() is True
            # kill the worker while the cancelled task is still queued.
            pool.terminate_job(pool._pool[0].pid, signal.SIGKILL)
            with pytest.raises(Cancelled):
                victim.get(10)
            assert victim.state == billiard.pool.ApplyResult.CANCELLED
        finally:
            pool.terminate()
            pool.join()

    def test_dispatch_gate_drops_cancelled_tasks(self):
        # unit-level test of the dispatch gate: a task cancelled before
        # dispatch is dropped instead of being written to the inqueue.
        pool = billiard.pool.Pool(1)
        try:
            sent = []
            real_put = pool._quick_put
            pool._quick_put = sent.append
            try:
                cancelled = billiard.pool.ApplyResult(pool._cache, None)
                assert cancelled.state == billiard.pool.ApplyResult.PENDING
                assert cancelled.cancel() is True
                pool._put_task((billiard.pool.TASK,
                                (cancelled._job, None, func, (1,), {})))
                assert sent == []
                assert cancelled._job not in pool._cache

                normal = billiard.pool.ApplyResult(pool._cache, None)
                pool._put_task((billiard.pool.TASK,
                                (normal._job, None, func, (1,), {})))
                assert len(sent) == 1
                assert normal._dispatched
                # dispatched without synack: cannot be recalled.
                assert normal.cancel() is False
            finally:
                pool._quick_put = real_put
        finally:
            pool.terminate()
            pool.join()

    def test_cancel_map_async_before_start(self):
        # map cancellation granularity is the chunk: cancelling before
        # any chunk started guarantees no chunk (and no item) executes.
        pool, started, release, executed = _lab_pool(self.ctx, synack=True)
        try:
            blocker = pool.apply_async(_lab_blocking_task, ('blocker',))
            assert started.wait(10)
            result = pool.map_async(_lab_quick_task, range(4), chunksize=1)
            assert result.cancel() is True
            assert result.state == billiard.pool.ApplyResult.CANCELLED
            release.set()
            with pytest.raises(Cancelled):
                result.get(10)
            sentinel = pool.apply_async(_lab_quick_task, ('sentinel',))
            assert sentinel.get(10) == 'sentinel'
            assert blocker.get(10) == 'blocker'
            assert executed.get(10) == 'blocker'
            assert executed.get(10) == 'sentinel'
            with pytest.raises(Empty):
                executed.get_nowait()
        finally:
            release.set()
            pool.terminate()
            pool.join()

    def test_cancel_map_async_after_chunk_started_fails(self):
        # once any chunk of a map started, the whole map can no longer
        # be cancelled and runs to completion.
        pool, started, release, executed = _lab_pool(self.ctx, synack=True)
        try:
            result = pool.map_async(_lab_blocking_task, range(2),
                                    chunksize=1)
            assert started.wait(10)  # first chunk is running
            assert result.cancel() is False
            release.set()
            assert result.get(10) == [0, 1]
        finally:
            release.set()
            pool.terminate()
            pool.join()
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
