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

def raise_base_exception():
    raise BaseException("base exception test")


def raise_system_exit():
    raise SystemExit(1)


_blocking_event = None


def init_blocking_worker(event):
    global _blocking_event
    _blocking_event = event


def block_first_task(x):
    # The first item waits on an event, all later items finish at once.
    if x == 0 and _blocking_event is not None:
        _blocking_event.wait(30)
    return x


class InfiniteCounter:
    """Infinite iterable recording how many times next() was called."""

    def __init__(self):
        self.next_count = 0

    def __iter__(self):
        return self

    def __next__(self):
        value = self.next_count
        self.next_count += 1
        return value

    next = __next__


def wait_for(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


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


class test_imap_max_pending:

    def test_ordered_window_bounds_prefetch(self):
        event = get_context().Event()
        pool = billiard.pool.Pool(
            2, initializer=init_blocking_worker, initargs=(event,))
        try:
            source = InfiniteCounter()
            it = pool.imap(block_first_task, source, max_pending=4)
            # The first item blocks in a worker; the window fills with
            # three more items and the producer stops pulling the
            # infinite source.
            assert wait_for(lambda: source.next_count >= 4)
            time.sleep(0.5)
            assert source.next_count == 4
            # Submitted-but-unconsumed items and results buffered in the
            # parent are both bounded by the window.
            assert it._pending <= 4
            assert len(it._items) + len(it._unsorted) <= 4
            event.set()
            assert [next(it) for _ in range(4)] == [0, 1, 2, 3]
            it.close()
            time.sleep(0.5)
            # Closing the result iterator stopped the producer: no more
            # than the consumed items plus one window were pulled.
            assert source.next_count <= 4 + 4
        finally:
            event.set()
            pool.terminate()
            pool.join()

    def test_unordered_window_and_early_close(self):
        pool = billiard.pool.Pool(2)
        try:
            source = InfiniteCounter()
            it = pool.imap_unordered(simple_task, source, max_pending=3)
            assert wait_for(lambda: source.next_count >= 3)
            time.sleep(0.5)
            assert source.next_count == 3
            assert it._pending <= 3
            assert len(it._items) <= 3
            results = [next(it) for _ in range(2)]
            assert sorted(results) == [0, 2]
            it.close()
            time.sleep(0.5)
            assert source.next_count <= 2 + 3
        finally:
            pool.terminate()
            pool.join()

    def test_chunksize_budget_is_counted_in_items(self):
        pool = billiard.pool.Pool(2)
        try:
            source = InfiniteCounter()
            # max_pending=5 with chunksize=4 allows ceil(5/4)=2 chunks
            # in flight, i.e. exactly 8 items are pulled before the
            # producer blocks.
            it = pool.imap(simple_task, source, chunksize=4, max_pending=5)
            assert wait_for(lambda: source.next_count >= 8)
            time.sleep(0.5)
            assert source.next_count == 8
            assert [next(it) for _ in range(4)] == [0, 2, 4, 6]
            it.close()
            time.sleep(0.5)
            # Consuming one chunk released one chunk (4 items) of budget.
            assert source.next_count <= 8 + 4
        finally:
            pool.terminate()
            pool.join()

    def test_finite_source_completes(self):
        pool = billiard.pool.Pool(2)
        try:
            expected = [i * 2 for i in range(10)]
            assert list(pool.imap(
                simple_task, range(10), max_pending=3)) == expected
            assert sorted(pool.imap_unordered(
                simple_task, range(10), max_pending=3)) == expected
            assert list(pool.imap(
                simple_task, range(10), chunksize=3,
                max_pending=4)) == expected
        finally:
            pool.terminate()
            pool.join()

    def test_worker_error_releases_producer(self):
        pool = billiard.pool.Pool(2)
        try:
            source = InfiniteCounter()
            # func raises ValueError for x == 2.
            it = pool.imap(func, source, max_pending=2)
            assert next(it) == 0
            assert next(it) == 1
            with pytest.raises(Exception):
                next(it)
            # The worker error must release the throttled producer so it
            # cannot block on the window forever.
            assert wait_for(lambda: it._failed)
            time.sleep(0.5)
            next_count = source.next_count
            time.sleep(0.5)
            assert source.next_count == next_count
        finally:
            pool.terminate()
            pool.join()

    def test_source_error_does_not_stall_producer(self):
        class FailingSource:
            def __init__(self):
                self.next_count = 0

            def __iter__(self):
                return self

            def __next__(self):
                if self.next_count == 3:
                    raise RuntimeError('source failed')
                self.next_count += 1
                return self.next_count - 1

            next = __next__

        pool = billiard.pool.Pool(2)
        try:
            it = pool.imap(simple_task, FailingSource(), max_pending=2)
            assert [next(it) for _ in range(3)] == [0, 2, 4]
            with pytest.raises(StopIteration):
                next(it)
        finally:
            pool.terminate()
            pool.join()

    def test_max_pending_must_be_positive(self):
        pool = billiard.pool.Pool(1)
        try:
            with pytest.raises(ValueError, match='max_pending'):
                pool.imap(simple_task, range(3), max_pending=0)
            with pytest.raises(ValueError, match='max_pending'):
                pool.imap_unordered(simple_task, range(3), max_pending=-1)
        finally:
            pool.terminate()
            pool.join()
