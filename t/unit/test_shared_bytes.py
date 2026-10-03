import hashlib
import os
import pickle
import signal
import time

import pytest

import billiard
import billiard.connection
import billiard.pool
from billiard.exceptions import WorkerLostError
from billiard.sharedbytes import (
    SharedBytesHandle,
    SharedBytesRegistry,
    SharedBytesReleased,
    shared_bytes_supported,
)

pytestmark = pytest.mark.skipif(
    not shared_bytes_supported(),
    reason='multiprocessing.shared_memory not available',
)

PAYLOAD = os.urandom(1 << 21)  # 2 MiB
PAYLOAD_SHA256 = hashlib.sha256(PAYLOAD).hexdigest()

_worker_gate = None
_worker_started = None


def init_worker(gate, started):
    global _worker_gate, _worker_started
    _worker_gate, _worker_started = gate, started


def gated_digest(view):
    # Block until the parent releases the gate, then read the block.
    _worker_started.set()
    _worker_gate.wait(30)
    return os.getpid(), hashlib.sha256(view.view()).hexdigest(), len(view)


def blocking_read(view):
    _worker_started.set()
    _worker_gate.wait(30)
    return len(view)


def sleeping_read(view):
    # Blocks without holding any shared lock, so the worker can be
    # killed mid-task without corrupting shared synchronization state.
    _worker_started.set()
    time.sleep(30)
    return len(view)


def identity(x):
    return x


def wait_for(cond, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    return False


def make_events():
    return billiard.Event(), billiard.Event()


class recording_send:
    """Record the pickled size of every TASK payload sent to workers."""

    def __init__(self, monkeypatch):
        self.sizes = []
        original = billiard.connection.Connection.send

        def spy(conn, obj, _orig=original):
            if (isinstance(obj, tuple) and len(obj) == 2 and
                    obj[0] == billiard.pool.TASK):
                self.sizes.append(len(pickle.dumps(obj)))
            return _orig(conn, obj)

        # Must be installed before the pool binds its quick-put method.
        monkeypatch.setattr(billiard.connection.Connection, 'send', spy)


class test_shared_bytes_unit:
    def test_handle_has_unforgeable_generational_identity(self):
        registry = SharedBytesRegistry()
        block = registry.create(b'x' * 64)
        handle = block.handle
        assert handle.identity == (block.name, block.generation)
        assert len(handle.token) == 16
        assert registry.verify(handle)

        forged = SharedBytesHandle(
            handle.name, handle.generation, b'0' * 16, handle.size)
        assert not registry.verify(forged)
        assert forged != handle

        other = registry.create(b'y' * 64)
        assert other.generation != block.generation
        assert other.handle != handle

        # Handles survive pickling (this is how they reach workers).
        assert pickle.loads(pickle.dumps(handle)) == handle

        block.close()
        assert not registry.verify(handle)  # stale handle
        other.close()

    def test_block_rejects_non_bytes_like(self):
        registry = SharedBytesRegistry()
        with pytest.raises(TypeError):
            registry.create([1, 2, 3])
        with pytest.raises(TypeError):
            registry.create('text')

    def test_block_accepts_any_bytes_like(self):
        registry = SharedBytesRegistry()
        block = registry.create(bytearray(b'abc'))
        with block.handle.attach() as view:
            assert view.tobytes() == b'abc'
        block.close()
        block = registry.create(memoryview(b'def'))
        with block.handle.attach() as view:
            assert view.tobytes() == b'def'
        block.close()

    def test_block_is_not_picklable(self):
        registry = SharedBytesRegistry()
        block = registry.create(b'abc')
        with pytest.raises(TypeError, match='shared'):
            pickle.dumps(block)
        block.close()

    def test_view_is_read_only(self):
        registry = SharedBytesRegistry()
        block = registry.create(b'abc')
        view = block.handle.attach()
        assert view.view().readonly
        with pytest.raises(TypeError):
            view.view()[0] = 1
        view.close()
        with pytest.raises(SharedBytesReleased):
            view.view()
        block.close()

    def test_handle_of_released_block_raises(self):
        registry = SharedBytesRegistry()
        block = registry.create(b'abc')
        block.close()
        assert block.released
        with pytest.raises(SharedBytesReleased):
            block.handle

    def test_acquire_rejects_foreign_and_plain_objects(self):
        registry = SharedBytesRegistry()
        foreign = SharedBytesRegistry().create(b'abc')
        with pytest.raises(ValueError):
            registry.acquire(1, (foreign,))
        with pytest.raises(TypeError):
            registry.acquire(1, (b'abc',))
        foreign.close()

    def test_release_job_is_idempotent(self):
        registry = SharedBytesRegistry()
        block = registry.create(b'abc')
        registry.acquire(1, (block,))
        registry.acquire(2, (block,))
        assert block.refcount == 2
        registry.release_job(1)
        registry.release_job(1)
        assert block.refcount == 1
        registry.release_job(2)
        assert block.refcount == 0
        block.close()

    def test_cancelled_job_returns_references(self):
        released = []
        result = billiard.pool.ApplyResult(
            {}, None, send_ack=lambda *args: None)
        result._shared_release = lambda: released.append(result._job)
        result._cancel()
        result._ack(None, 0.0, 1234, None)
        assert released == [result._job]
        # A later terminal state must not release twice.
        result._set(None, (True, None))
        assert released == [result._job]

    def test_discard_returns_references(self):
        released = []
        result = billiard.pool.ApplyResult({}, None)
        result._shared_release = lambda: released.append(result._job)
        result.discard()
        result.discard()
        assert released == [result._job]


class test_shared_bytes_pool:
    def test_two_workers_read_same_block_with_small_pipe_payload(
            self, monkeypatch):
        recorder = recording_send(monkeypatch)
        gate, started = make_events()
        pool = billiard.pool.Pool(
            2, initializer=init_worker, initargs=(gate, started))
        try:
            block = pool.shared_bytes(PAYLOAD)
            accepted = []
            results = [
                pool.apply_async(
                    gated_digest, (), shared=(block,),
                    accept_callback=lambda pid, ts: accepted.append(pid))
                for _ in range(2)
            ]
            # Both workers pick up one task each and block on the gate.
            assert wait_for(lambda: len(accepted) == 2)
            assert len(set(accepted)) == 2
            gate.set()  # let both workers read the block
            out = [res.get(timeout=30) for res in results]
            assert {pid for pid, _, _ in out} == set(accepted)
            assert all(sha == PAYLOAD_SHA256 for _, sha, _ in out)
            assert all(size == len(PAYLOAD) for _, _, size in out)

            # Control: the same payload as a regular argument is copied
            # through the pipe in full.
            control = pool.apply_async(identity, (PAYLOAD,))
            assert control.get(timeout=30) == PAYLOAD

            assert len(recorder.sizes) == 3
            shared_sizes, control_size = recorder.sizes[:2], recorder.sizes[2]
            assert all(size < 16 * 1024 for size in shared_sizes)
            assert control_size > len(PAYLOAD)

            block.close()
            assert block.released
            assert len(pool._shared_registry) == 0
        finally:
            pool.terminate()
            pool.join()

    def test_user_close_keeps_inflight_tasks_alive(self):
        gate, started = make_events()
        pool = billiard.pool.Pool(
            1, initializer=init_worker, initargs=(gate, started))
        try:
            block = pool.shared_bytes(PAYLOAD)
            result = pool.apply_async(blocking_read, (), shared=(block,))
            assert started.wait(10)
            assert wait_for(lambda: block.refcount == 1)
            block.close()  # release the user handle mid-flight
            assert not block.released  # task reference keeps it mapped
            gate.set()
            assert result.get(timeout=30) == len(PAYLOAD)
            assert wait_for(lambda: block.released, timeout=5)
            assert len(pool._shared_registry) == 0
        finally:
            pool.terminate()
            pool.join()

    def test_worker_killed_releases_refs_and_remaining_tasks_complete(
            self, monkeypatch):
        recorder = recording_send(monkeypatch)
        gate, started = make_events()
        pool = billiard.pool.Pool(
            2, lost_worker_timeout=0.5,
            initializer=init_worker, initargs=(gate, started))
        try:
            block = pool.shared_bytes(PAYLOAD)
            accepted = []
            lost = pool.apply_async(
                sleeping_read, (), shared=(block,),
                accept_callback=lambda pid, ts: accepted.append(pid))
            assert wait_for(lambda: len(accepted) == 1)
            assert started.wait(10)

            # The surviving worker picks up the second task and blocks
            # on the gate while reading the same block.
            survivor = pool.apply_async(gated_digest, (), shared=(block,))
            assert wait_for(lambda: block.refcount == 2)

            # Kill the worker running the first task mid-read.
            os.kill(accepted[0], signal.SIGKILL)
            with pytest.raises(Exception) as excinfo:
                lost.get(timeout=30)
            exc = getattr(excinfo.value, 'exc', excinfo.value)
            assert isinstance(exc, WorkerLostError)

            # The remaining task completes and reads the correct data.
            gate.set()
            pid, sha, size = survivor.get(timeout=30)
            assert pid != accepted[0]
            assert sha == PAYLOAD_SHA256
            assert size == len(PAYLOAD)

            # All task references returned; only small handles travelled
            # through the pipe.
            assert wait_for(lambda: block.refcount == 0, timeout=5)
            assert recorder.sizes and all(
                size < 16 * 1024 for size in recorder.sizes)

            # Final reclamation of the shared resource.
            block.close()
            assert block.released
            assert len(pool._shared_registry) == 0
            if os.name == 'posix':
                assert not os.path.exists(os.path.join('/dev/shm',
                                                       block.name))
        finally:
            pool.terminate()
            pool.join()

    def test_serialization_failure_returns_references(self):
        pool = billiard.pool.Pool(1)
        try:
            block = pool.shared_bytes(b'abc')
            result = pool.apply_async(
                identity, (lambda: None,), shared=(block,))
            with pytest.raises(Exception):
                result.get(timeout=30)
            assert wait_for(lambda: block.refcount == 0, timeout=5)
            block.close()
            assert block.released
        finally:
            pool.terminate()
            pool.join()

    def test_pool_terminate_releases_everything(self):
        gate, started = make_events()
        pool = billiard.pool.Pool(
            1, initializer=init_worker, initargs=(gate, started))
        block = pool.shared_bytes(PAYLOAD)
        name = block.name
        pool.apply_async(blocking_read, (), shared=(block,))
        assert started.wait(10)
        assert wait_for(lambda: block.refcount == 1)
        pool.terminate()
        pool.join()
        assert block.released
        assert len(pool._shared_registry) == 0
        if os.name == 'posix':
            assert not os.path.exists(os.path.join('/dev/shm', name))
