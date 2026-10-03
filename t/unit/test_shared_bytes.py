import os
import pickle
import signal
import sys
import time

import pytest

import billiard.pool
from billiard.exceptions import WorkerLostError
from billiard.shared_bytes import (
    SharedBytes,
    SharedBytesRef,
    SharedBytesRegistry,
    SharedBytesReleased,
    SharedBytesView,
    share_bytes,
)

PAYLOAD = bytes(range(256)) * 4096  # 1 MiB of deterministic data


def read_block(index, block):
    # runs in the worker: block is a read-only SharedBytesView
    return os.getpid(), len(block), bytes(block[:16]), PAYLOAD == bytes(block)


def suicide(index, block):
    os.kill(os.getpid(), signal.SIGKILL)


def uses_lambda():
    return None


class RecordingQueue:
    """Wraps the pool task queue to record serialized payload sizes."""

    def __init__(self, queue):
        self._queue = queue
        self.sizes = []

    def put(self, item):
        try:
            self.sizes.append(len(pickle.dumps(item)))
        except Exception:
            pass
        return self._queue.put(item)

    def __getattr__(self, name):
        return getattr(self._queue, name)


@pytest.fixture
def registry():
    return SharedBytesRegistry()


class test_registry:
    def test_register_rejects_non_bytes_like(self, registry):
        with pytest.raises(TypeError):
            registry.register(['not', 'bytes'])
        with pytest.raises(TypeError):
            registry.register(object())

    def test_register_bytes_like(self, registry):
        for payload in (b'abc', bytearray(b'abc'), memoryview(b'abc')):
            block = registry.register(payload)
            assert block.tobytes() == b'abc'
            block.close()
        assert registry.blocks == set()

    def test_handle_is_not_picklable(self, registry):
        block = registry.register(b'abc')
        with pytest.raises(TypeError):
            pickle.dumps(block)
        block.close()

    def test_view_is_not_picklable(self, registry):
        block = registry.register(b'abc')
        view = SharedBytesView(block.name, block.size, block.token)
        assert bytes(view) == b'abc'
        with pytest.raises(TypeError):
            pickle.dumps(view)
        view.close()
        block.close()

    def test_view_is_read_only(self, registry):
        block = registry.register(b'abc')
        view = SharedBytesView(block.name, block.size, block.token)
        with pytest.raises(TypeError):
            view[0] = 1
        view.close()
        block.close()

    def test_released_handle_cannot_be_acquired(self, registry):
        block = registry.register(b'abc')
        block.close()
        assert block.released
        with pytest.raises(SharedBytesReleased):
            registry.acquire(block)
        assert registry.blocks == set()

    def test_user_release_waits_for_inflight(self, registry):
        block = registry.register(b'abc')
        name = block.name
        ref = registry.acquire(block)
        block.close()
        # in-flight task keeps the segment alive
        assert registry.blocks == {name}
        assert registry.inflight == 1
        registry.release(ref)
        assert registry.blocks == set()
        assert registry.inflight == 0

    def test_stale_ref_cannot_free_new_generation(self, registry):
        block = registry.register(b'abc')
        ref = registry.acquire(block)
        stale = SharedBytesRef(block.name, block.size, '0' * 32)
        registry.release(stale)  # ignored: wrong generation token
        assert registry.inflight == 1
        registry.release(ref)
        registry.release(ref)  # duplicate return is harmless
        assert registry.inflight == 0
        block.close()
        assert registry.blocks == set()

    def test_default_registry_share_bytes(self):
        block = share_bytes(b'xyz')
        assert isinstance(block, SharedBytes)
        block.close()


@pytest.mark.skipif(sys.platform == 'win32', reason='uses POSIX signals')
class test_pool_shared_bytes:
    def test_two_workers_share_block_and_payload_stays_small(
            self, registry):
        pool = billiard.pool.Pool(processes=2, shared_registry=registry)
        recorder = RecordingQueue(pool._taskqueue)
        pool._taskqueue = recorder
        block = pool.shared_bytes(PAYLOAD)
        try:
            results = [
                pool.apply_async(read_block, args=(i,), shared=block)
                for i in range(10)
            ]
            values = [res.get(timeout=30) for res in results]
        finally:
            pool.close()
            pool.join()
        # two different workers read the same block
        assert len({pid for pid, _, _, _ in values}) == 2
        for _, size, head, identical in values:
            assert size == len(PAYLOAD)
            assert head == PAYLOAD[:16]
            assert identical
        # the 1 MiB payload never went through the task pipe
        assert recorder.sizes
        assert max(recorder.sizes) < 4096
        # all task references returned; user release reclaims the segment
        assert registry.inflight == 0
        name = block.name
        block.close()
        assert registry.blocks == set()
        with pytest.raises(FileNotFoundError):
            from multiprocessing.shared_memory import SharedMemory
            SharedMemory(name=name)

    def test_user_release_keeps_inflight_tasks_alive(self, registry):
        pool = billiard.pool.Pool(processes=2, shared_registry=registry)
        block = pool.shared_bytes(PAYLOAD)
        try:
            results = [
                pool.apply_async(read_block, args=(i,), shared=block)
                for i in range(8)
            ]
            # release the user handle while tasks are in flight
            block.close()
            assert block.released
            values = [res.get(timeout=30) for res in results]
        finally:
            pool.close()
            pool.join()
        assert all(identical for _, _, _, identical in values)
        # segment reclaimed once the last in-flight task settled
        assert registry.blocks == set()
        assert registry.inflight == 0

    def test_worker_loss_returns_refs_and_remaining_tasks_finish(
            self, registry):
        pool = billiard.pool.Pool(processes=2, shared_registry=registry)
        block = pool.shared_bytes(PAYLOAD)
        name = block.name
        try:
            doomed = pool.apply_async(
                suicide, args=(0,), shared=block, lost_worker_timeout=1.0,
            )
            survivors = [
                pool.apply_async(read_block, args=(i,), shared=block)
                for i in range(6)
            ]
            with pytest.raises(Exception) as exc_info:
                doomed.get(timeout=30)
            # parent-side worker-loss errors arrive wrapped in
            # ExceptionWithTraceback carrying the WorkerLostError.
            assert isinstance(
                getattr(exc_info.value, 'exc', exc_info.value),
                WorkerLostError,
            )
            values = [res.get(timeout=30) for res in survivors]
            assert all(identical for _, _, _, identical in values)
        finally:
            pool.close()
            pool.join()
        # the lost worker's task returned its reference too
        assert registry.inflight == 0
        block.close()
        assert registry.blocks == set()
        with pytest.raises(FileNotFoundError):
            from multiprocessing.shared_memory import SharedMemory
            SharedMemory(name=name)

    def test_serialization_failure_returns_refs(self, registry):
        pool = billiard.pool.Pool(processes=1, shared_registry=registry)
        block = pool.shared_bytes(b'x' * 1024)
        try:
            # lambdas cannot be pickled: the task handler fails to
            # serialize the task and settles the job with the error.
            res = pool.apply_async(lambda: None, shared=block)
            with pytest.raises(Exception):
                res.get(timeout=30)
            assert registry.inflight == 0
        finally:
            pool.terminate()
            pool.join()
        block.close()
        assert registry.blocks == set()

    def test_serialization_failure_returns_refs_no_threads(self, registry):
        pool = billiard.pool.Pool(
            processes=1, threads=False, shared_registry=registry,
        )
        block = pool.shared_bytes(b'x' * 1024)
        try:
            with pytest.raises(Exception):
                # _quick_put pickles eagerly: the error is raised by
                # apply_async itself and the task is never sent.
                pool.apply_async(lambda: None, shared=block)
            assert registry.inflight == 0
        finally:
            pool.terminate()
        block.close()
        assert registry.blocks == set()

    def test_terminate_returns_refs_of_pending_jobs(self, registry):
        pool = billiard.pool.Pool(processes=1, shared_registry=registry)
        block = pool.shared_bytes(PAYLOAD)
        res = pool.apply_async(time.sleep, args=(30,), shared=block)
        pool.terminate()
        pool.join()
        assert registry.inflight == 0
        block.close()
        assert registry.blocks == set()
