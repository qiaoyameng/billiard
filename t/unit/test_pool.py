import os
import signal
import threading
import time

import pytest

import billiard.pool
from billiard.pool import IMapIterator, IMapUnorderedIterator, mapstar
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


def boom(x):
    raise ValueError(x)


def raising_source():
    yield 0
    raise RuntimeError('source failed')

def raise_base_exception():
    raise BaseException("base exception test")


def raise_system_exit():
    raise SystemExit(1)


class test_pool:
    def test_imap_max_pending_counts_buffered_results(self):
        result = IMapIterator({}, max_pending=2)
        result._reserve(2)
        result._set_task_size(0, 1)
        result._set_task_size(1, 1)
        result._set_length(3)
        result._set(0, (True, 0))
        result._set(1, (True, 1))

        assert result.pending() == 2
        assert result.next(timeout=1) == 0
        assert result.pending() == 1

        def reserve_more():
            result._reserve(2)

        thread = threading.Thread(target=reserve_more)
        thread.start()
        thread.join(timeout=0.1)
        assert thread.is_alive()
        assert result.pending() == 1
        assert result.next(timeout=1) == 1
        thread.join(timeout=0.1)
        assert not thread.is_alive()
        assert result.pending() == 2
        result.close()

    @pytest.mark.parametrize(
        'iterator_cls', (IMapIterator, IMapUnorderedIterator)
    )
    def test_imap_max_pending_chunks_are_counted_by_items(
        self, iterator_cls,
    ):
        result = iterator_cls({}, max_pending=2)
        result._reserve(2)
        result._set_task_size(0, 2)
        result._set_length(1)
        result._set(0, (True, [0, 1]))

        assert result.pending() == 2

        def reserve_next_chunk():
            result._reserve(2)

        thread = threading.Thread(target=reserve_next_chunk)
        thread.start()
        thread.join(timeout=0.1)
        assert thread.is_alive()

        flatten = result._flatten_chunks()
        assert next(flatten) == 0
        assert result.pending() == 1
        assert next(flatten) == 1
        thread.join(timeout=1)
        assert result.pending() == 2
        result.close()

    def test_imap_tasks_do_not_pull_past_item_budget(self):
        pulled = []

        def source():
            for value in range(10):
                pulled.append(value)
                yield value

        pool = billiard.pool.Pool(1)
        result = IMapIterator(pool._cache, max_pending=3)
        try:
            tasks = pool._imap_tasks(
                result, mapstar, simple_task, source(), 2, 3,
            )
            task = next(tasks)
            assert pulled == [0, 1]
            assert result.pending() == 2

            result._set(0, (True, [0, 2]))
            assert result.next(timeout=1) == [0, 2]

            task = next(tasks)
            assert pulled == [0, 1, 2, 3]
            assert result.pending() == 2
        finally:
            result.close()
            pool.terminate()
            pool.join()

    def test_imap_early_close_stops_pulling_source(self):
        result = IMapIterator({}, max_pending=2)
        result._reserve(2)
        result.close()
        with pytest.raises(billiard.pool._IMapClosed):
            result._reserve(1)

    @pytest.mark.parametrize('chunksize', (1, 2))
    def test_imap_source_exception_does_not_block_producer(self, chunksize):
        pool = billiard.pool.Pool(2)
        try:
            iterator = pool.imap(
                simple_task, raising_source(),
                chunksize=chunksize, max_pending=2,
            )
            assert next(iterator) == 0
            with pytest.raises(Exception, match='source failed'):
                next(iterator)
        finally:
            pool.terminate()
            pool.join()

    @pytest.mark.parametrize('unordered', (False, True))
    def test_imap_worker_exception_does_not_block_producer(self, unordered):
        def source():
            yield 0
            yield 1

        pool = billiard.pool.Pool(2)
        try:
            method = pool.imap_unordered if unordered else pool.imap
            iterator = method(boom, source(), max_pending=2)
            with pytest.raises(Exception, match='ValueError'):
                next(iterator)
        finally:
            pool.terminate()
            pool.join()

    @pytest.mark.parametrize('max_pending', (0, -1, 1.5, True))
    def test_imap_rejects_invalid_max_pending(self, max_pending):
        pool = billiard.pool.Pool(1)
        try:
            with pytest.raises((TypeError, ValueError), match='max_pending'):
                pool.imap(simple_task, range(3), max_pending=max_pending)
        finally:
            pool.terminate()
            pool.join()

    def test_imap_iterator_releases_budget_when_result_is_consumed(self):
        result = IMapIterator({}, max_pending=1)
        result._reserve(1)
        result._set_task_size(0, 1)
        result._set_length(1)
        result._set(0, (True, 'value'))
        assert result.pending() == 1
        assert result.next(timeout=1) == 'value'
        assert result.pending() == 0
        result.close()

    def test_imap_unordered_iterator_releases_budget_when_consumed(self):
        result = IMapUnorderedIterator({}, max_pending=1)
        result._reserve(1)
        result._set_task_size(0, 1)
        result._set_length(1)
        result._set(0, (True, 'value'))
        assert result.pending() == 1
        assert result.next(timeout=1) == 'value'
        assert result.pending() == 0
        result.close()

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
