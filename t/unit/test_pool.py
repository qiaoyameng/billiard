import os
import signal
import time

import pytest

import billiard.pool
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


def event_controlled_long_task(started_file, release_file):
    """Report the worker pid, then block until released by the parent."""
    with open(started_file, 'w') as f:
        f.write(str(os.getpid()))
    deadline = time.monotonic() + 60
    while not os.path.exists(release_file):
        if time.monotonic() > deadline:
            raise RuntimeError('timed out waiting for release file')
        time.sleep(0.05)
    return os.getpid()


def recording_short_task(record_file, x):
    """Record every execution so duplicates can be detected."""
    with open(record_file, 'a') as f:
        f.write('%d\n' % x)
    return x * 2


def file_gate_initializer(fail_file):
    if os.path.exists(fail_file):
        raise RuntimeError('initializer failed as requested')


def wait_for_recycle(pool, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not pool.recycle_in_progress and all(
                w._capacity_credited for w in pool._pool):
            return
        time.sleep(0.1)
    raise AssertionError('rolling recycle did not converge')


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

    def test_rolling_recycle(self, tmp_path):
        """Two workers, one busy with an event-controlled long task.

        While a rolling recycle runs, short tasks keep being submitted.
        Verifies that no task is executed twice, the long task is not
        interrupted, the pool never exceeds its configured size, and all
        workers end up on the final generation.
        """
        started_file = tmp_path / 'long_started'
        release_file = tmp_path / 'long_release'
        record_file = tmp_path / 'short_record'
        pool = billiard.pool.Pool(2)
        try:
            gen0_pids = {w.pid for w in pool._pool}
            long_result = pool.apply_async(
                event_controlled_long_task,
                (str(started_file), str(release_file)),
            )
            deadline = time.monotonic() + 10
            while not started_file.exists():
                assert time.monotonic() < deadline, 'long task never started'
                time.sleep(0.05)
            long_pid = int(started_file.read_text())
            assert long_pid in gen0_pids

            pool.rolling_recycle(limit=1)
            assert pool.recycle_in_progress
            # a consecutive request merges into the recycle in progress
            # instead of starting an unbounded second wave of replacements
            pool.rolling_recycle(limit=1)
            assert pool.generation == 2

            short_results = [
                pool.apply_async(
                    recording_short_task, (str(record_file), i))
                for i in range(20)
            ]
            # replacements are bounded: never more processes than configured
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and pool.recycle_in_progress:
                assert len(pool._pool) <= 2
                time.sleep(0.05)

            # let the long task finish on its original, undrained worker
            release_file.touch()
            assert long_result.get(timeout=30) == long_pid
            assert sorted(r.get(timeout=30) for r in short_results) == [
                i * 2 for i in range(20)
            ]

            wait_for_recycle(pool)
            # every short task was executed exactly once
            executed = [int(x) for x in record_file.read_text().split()]
            assert sorted(executed) == list(range(20))
            # final generation: both original workers were replaced
            assert {w._generation for w in pool._pool} == {pool.generation}
            assert not {w.pid for w in pool._pool} & gen0_pids
            assert pool._recycle_pending_capacity == 0
        finally:
            pool.terminate()
            pool.join()

    def test_rolling_recycle_with_maxtasksperchild(self):
        """Natural maxtasksperchild recycling joins the new generation
        instead of fighting the rolling recycle."""
        pool = billiard.pool.Pool(2, maxtasksperchild=2)
        try:
            pool.rolling_recycle(limit=1)
            results = [pool.apply_async(simple_task, (i,)) for i in range(12)]
            assert [r.get(timeout=30) for r in results] == [
                i * 2 for i in range(12)
            ]
            wait_for_recycle(pool)
            assert {w._generation for w in pool._pool} == {1}
        finally:
            pool.terminate()
            pool.join()

    def test_rolling_recycle_retries_after_initializer_failure(
            self, tmp_path):
        """Replacements that fail to initialize are retried and never
        count towards the pool capacity until they succeed."""
        fail_file = tmp_path / 'fail_init'
        started_pids = []
        pool = billiard.pool.Pool(
            1, initializer=file_gate_initializer,
            initargs=(str(fail_file),),
            on_process_up=lambda w: started_pids.append(w.pid))
        try:
            assert pool.apply_async(simple_task, (1,)).get(timeout=10) == 2
            fail_file.touch()
            pool.rolling_recycle(limit=1)
            # replacements are attempted but keep failing their
            # initializer: the recycle cannot complete and no capacity
            # is credited to the new generation
            deadline = time.monotonic() + 15
            while len(started_pids) < 3:
                assert time.monotonic() < deadline, \
                    'failed replacements were not retried'
                time.sleep(0.1)
            assert pool.recycle_in_progress
            assert not any(
                w._generation == 1 and w._capacity_credited
                for w in pool._pool
            )
            # once the initializer can succeed the recycle converges
            fail_file.unlink()
            wait_for_recycle(pool)
            assert {w._generation for w in pool._pool} == {1}
            assert pool.apply_async(simple_task, (2,)).get(timeout=10) == 4
        finally:
            pool.terminate()
            pool.join()

    def test_rolling_recycle_validates_arguments(self):
        pool = billiard.pool.Pool(1)
        try:
            with pytest.raises(TypeError, match="bool"):
                pool.rolling_recycle(limit=True)
            with pytest.raises(ValueError, match="positive int"):
                pool.rolling_recycle(limit=0)
            assert not pool.recycle_in_progress
        finally:
            pool.terminate()
            pool.join()
        with pytest.raises(RuntimeError):
            pool.rolling_recycle()
