import os
import signal
import time

import pytest

import billiard.pool
from billiard import get_context
from billiard.exceptions import TaskCancelledError, TimeLimitExceeded


class BoundaryWorker(billiard.pool.Worker):
    boundary_start = None
    boundary_release = None
    boundary_phase = 0

    def on_syn_ack(self, job):
        self.boundary_phase += 1
        if self.boundary_phase == 1 and self.boundary_start is not None:
            self.boundary_start.set()
            self.boundary_release.wait(10)
        return True


class BoundaryPool(billiard.pool.Pool):
    Worker = BoundaryWorker

    def __init__(self, *args, boundary_start=None, boundary_release=None,
                 **kwargs):
        BoundaryWorker.boundary_start = boundary_start
        BoundaryWorker.boundary_release = boundary_release
        BoundaryWorker.boundary_phase = 0
        self._boundary_start = boundary_start
        self._boundary_release = boundary_release
        super().__init__(*args, **kwargs)


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


def event_simple_task(args):
    key, x = args
    TASK_EVENTS[key].set()
    return x * 2


def marked_simple_task(x):
    marker = os.environ.get('BILLIARD_CANCEL_MARKER')
    if marker:
        with open(marker, 'w'):
            pass
    return x * 2


TASK_EVENTS = {}

def raise_base_exception():
    raise BaseException("base exception test")


def raise_system_exit():
    raise SystemExit(1)


def wait_named_event(key):
    TASK_EVENTS[key].wait(10)


def named_blocking_task(key):
    TASK_EVENTS[key + ':entered'].set()
    TASK_EVENTS[key + ':release'].wait(10)
    return 'finished'


def named_blocking_tuple(key):
    (key,) = key
    TASK_EVENTS[key + ':entered'].set()
    TASK_EVENTS[key + ':release'].wait(10)
    return 'finished'


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

    def test_cancel_pending_task_guarantees_function_does_not_run(
            self, tmp_path, monkeypatch):
        ctx = get_context()
        barrier = ctx.Event()
        TASK_EVENTS['pending-barrier'] = barrier
        marker = tmp_path / 'ran'
        monkeypatch.setenv('BILLIARD_CANCEL_MARKER', str(marker))
        pool = billiard.pool.Pool(1, synack=True, context=ctx)
        try:
            running = pool.apply_async(wait_named_event, ('pending-barrier',))
            cancelled = pool.apply_async(marked_simple_task, (400,))

            assert cancelled.status() == billiard.pool.TASK_PENDING
            assert cancelled.cancel() is True
            assert cancelled.status() == billiard.pool.TASK_CANCELLED
            barrier.set()
            assert running.get(10) is None
            with pytest.raises(TaskCancelledError):
                cancelled.get(10)
            assert not marker.exists()
        finally:
            pool.terminate()
            pool.join()

    def test_started_task_is_not_cancelled_and_is_not_killed(self):
        ctx = get_context()
        entered = ctx.Event()
        release = ctx.Event()
        TASK_EVENTS['started:entered'] = entered
        TASK_EVENTS['started:release'] = release
        pool = billiard.pool.Pool(1, synack=True, context=ctx)
        try:
            result = pool.apply_async(named_blocking_task, ('started',))
            assert entered.wait(10)
            assert result.status() == billiard.pool.TASK_STARTED
            pid_before = pool._pool[0].pid
            assert result.cancel() is False
            assert pool._pool[0].pid == pid_before
            release.set()
            assert result.get(10) == 'finished'
        finally:
            pool.terminate()
            pool.join()

    @pytest.mark.parametrize('allow_start', [False, True])
    def test_cancel_race_at_start_boundary(self, allow_start):
        worker_start = get_context().Event()
        worker_release = get_context().Event()
        function_started = get_context().Event()
        TASK_EVENTS['race-started'] = function_started
        pool = BoundaryPool(
            1, synack=True,
            boundary_start=worker_start,
            boundary_release=worker_release,
        )
        try:
            result = pool.apply_async(
                event_simple_task, (('race-started', 7),))
            assert worker_start.wait(10)
            assert result.status() == billiard.pool.TASK_RECEIVED

            if allow_start:
                worker_release.set()
                function_started.wait(10)
                assert result.cancel() is False
                assert result.get(10) == 14
            else:
                assert result.cancel() is True
                worker_release.set()
                assert result.status() == billiard.pool.TASK_CANCELLED
                with pytest.raises(TaskCancelledError):
                    result.get(10)
                assert not function_started.is_set()
        finally:
            worker_release.set()
            pool.terminate()
            pool.join()

    def test_cancel_is_idempotent_after_terminal_result(self):
        pool = billiard.pool.Pool(1, synack=True)
        try:
            result = pool.apply_async(simple_task, (21,))
            assert result.get(10) == 42
            assert result.status() == billiard.pool.TASK_FINISHED
            assert result.cancel() is False
            assert result.get(10) == 42
        finally:
            pool.terminate()
            pool.join()

    def test_cancel_requires_synack(self):
        pool = billiard.pool.Pool(1)
        try:
            result = pool.apply_async(simple_task, (1,))
            with pytest.raises(RuntimeError, match='synack=True'):
                result.cancel()
        finally:
            pool.terminate()
            pool.join()

    def test_late_result_does_not_override_cancelled_terminal_state(self):
        result = billiard.pool.ApplyResult(
            {}, None, control_writer=True, send_ack=lambda *a: None,
        )
        result._cancelled = True
        result._state = billiard.pool.TASK_CANCELLED
        result._set_cancelled_locked()

        result._set(None, (True, 'late result'))

        with pytest.raises(TaskCancelledError):
            result.get()

    def test_map_cancel_uses_whole_chunks_only(self):
        ctx = get_context()
        entered = ctx.Event()
        release = ctx.Event()
        TASK_EVENTS['map:entered'] = entered
        TASK_EVENTS['map:release'] = release
        pool = billiard.pool.Pool(1, synack=True, context=ctx)
        try:
            running = pool.map_async(
                named_blocking_tuple, [('map',)], chunksize=1,
            )
            assert entered.wait(10)
            pending = pool.map_async(simple_task, range(8), chunksize=4)
            assert running.chunk_status(0) == billiard.pool.TASK_STARTED
            assert pending.chunk_status(0) == billiard.pool.TASK_PENDING
            assert pending.chunk_status(1) == billiard.pool.TASK_PENDING
            assert running.cancel(0) is False
            assert pending.cancel(0) is True
            assert pending.cancel(1) is True
            assert pending.chunk_status(1) == billiard.pool.TASK_CANCELLED
            assert pending.cancel() is False
            release.set()
            assert running.get(10) == ['finished']
            with pytest.raises(TaskCancelledError):
                pending.get(10)
        finally:
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
