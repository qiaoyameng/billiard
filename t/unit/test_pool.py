import os
import signal
import threading
import time

import pytest

import billiard.pool
from billiard import get_context
from billiard.exceptions import (
    PoolClosedError,
    PoolNotReadyError,
    TimeLimitExceeded,
    WorkerInitializationError,
)


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


def wait_for_startup_event(event):
    event.wait(30)


def fail_initialization():
    raise RuntimeError('initializer failed')

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

    def test_waits_for_all_workers_to_become_ready(self):
        pool = billiard.pool.Pool(2)
        try:
            assert pool.wait_ready(10) is True
            assert pool.is_ready() is True
            assert len(pool._ready_workers) == 2
        finally:
            pool.terminate()
            pool.join()

    def test_rejects_tasks_until_pool_is_ready(self):
        ctx = get_context('fork')
        event = ctx.Event()
        pool = billiard.pool.Pool(
            1, wait_for_startup_event, (event,),
            reject_tasks_when_not_ready=True,
            context=ctx,
        )
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and pool.is_ready():
                time.sleep(0.01)
            with pytest.raises(PoolNotReadyError):
                pool.apply_async(simple_task, (1,), reject_if_not_ready=True)

            event.set()
            assert pool.wait_ready(10) is True
            assert pool.apply_async(simple_task, (1,)).get(10) == 2
        finally:
            event.set()
            pool.terminate()
            pool.join()

    def test_partial_ready_does_not_open_barrier(self):
        ctx = get_context('fork')
        state = ctx.Value('i', 0)
        release_first = ctx.Event()
        release_second = ctx.Event()

        def wait_for_first_worker():
            with state.get_lock():
                state.value += 1
                should_release_first = state.value == 1
            if should_release_first:
                event = release_first
            else:
                event = release_second
            event.wait(30)

        pool = billiard.pool.Pool(
            2, wait_for_first_worker,
            context=ctx,
        )
        try:
            release_first.set()
            deadline = time.monotonic() + 10
            while (time.monotonic() < deadline
                   and state.value < 1):
                time.sleep(0.01)
            deadline = time.monotonic() + 2
            while (time.monotonic() < deadline
                   and len(pool._ready_workers) < 1):
                time.sleep(0.01)
            assert len(pool._ready_workers) == 1
            assert pool.is_ready() is False
            assert pool.wait_ready(0.1) is False

            release_second.set()
            assert pool.wait_ready(10) is True
            assert pool.is_ready() is True
        finally:
            release_first.set()
            release_second.set()
            pool.terminate()
            pool.join()

    def test_initializer_failure_returns_diagnostics_and_reaps_pool(self):
        pool = billiard.pool.Pool(
            1, fail_initialization,
            max_initializer_restarts=0,
        )
        try:
            with pytest.raises(WorkerInitializationError) as exc_info:
                pool.wait_ready(10)

            error = exc_info.value
            assert error.diagnostics['type'] == 'RuntimeError'
            assert error.diagnostics['message'] == 'initializer failed'
            assert 'exception_info' in error.diagnostics
            assert pool._state == billiard.pool.TERMINATE
        finally:
            pool.join()
            assert all(not worker.is_alive() for worker in pool._pool)

    def test_initializer_restart_budget_is_limited(self):
        pool = billiard.pool.Pool(
            1, fail_initialization,
            max_initializer_restarts=1,
            max_restart_freq=100,
        )
        try:
            with pytest.raises(WorkerInitializationError):
                pool.wait_ready(10)

            assert pool._initializer_restarts == 2
            assert pool._state == billiard.pool.TERMINATE
        finally:
            pool.join()
            assert all(not worker.is_alive() for worker in pool._pool)

    def test_one_blocked_initializer_and_one_failed_initializer(self):
        ctx = get_context('fork')
        startup_count = ctx.Value('i', 0)
        startup_lock = ctx.Lock()
        second_worker_started = ctx.Event()
        block_second_worker = ctx.Event()

        def initializer():
            with startup_lock:
                startup_count.value += 1
                should_block = startup_count.value == 2
            if should_block:
                second_worker_started.set()
                block_second_worker.wait(30)
            else:
                second_worker_started.wait(10)
                raise RuntimeError('initializer failed')

        pool = billiard.pool.Pool(
            2, initializer,
            max_initializer_restarts=0,
            max_restart_freq=100,
            context=ctx,
        )
        try:
            with pytest.raises(WorkerInitializationError) as exc_info:
                pool.wait_ready(10)

            assert exc_info.value.diagnostics['message'] == (
                'initializer failed'
            )
            pool.join()
            assert all(not worker.is_alive() for worker in pool._pool)
            assert any(worker.exitcode < 0 for worker in pool._pool)
        finally:
            pool.terminate()

    def test_ready_wait_returns_on_close(self):
        ctx = get_context('fork')
        event = ctx.Event()
        pool = billiard.pool.Pool(
            1, wait_for_startup_event, (event,),
            context=ctx,
        )
        try:
            errors = []

            def wait_for_pool():
                try:
                    pool.wait_ready(10)
                except BaseException as exc:
                    errors.append(exc)

            waiter = threading.Thread(target=wait_for_pool)
            waiter.start()
            time.sleep(0.2)
            pool.close()
            waiter.join(5)
            assert not waiter.is_alive()
            assert errors and isinstance(errors[0], PoolClosedError)
        finally:
            pool.join()

    def test_ready_timeout_terminates_pool(self):
        ctx = get_context('fork')
        event = ctx.Event()
        pool = billiard.pool.Pool(
            1, wait_for_startup_event, (event,),
            ready_timeout=0.1,
            context=ctx,
        )
        try:
            with pytest.raises(Exception):
                pool.wait_ready(10)
            assert pool._state == billiard.pool.TERMINATE
        finally:
            pool.join()
            assert all(not worker.is_alive() for worker in pool._pool)

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
