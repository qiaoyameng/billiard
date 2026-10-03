"""Explicit shared read-only byte blocks for pool tasks.

This module lets the parent process create a block of shared memory once
and pass it to many pool tasks without copying the payload through the
task pipe every time.

Only payloads explicitly registered with a :class:`SharedBytesRegistry`
(via :meth:`billiard.pool.Pool.shared_bytes` or :func:`share_bytes`) are
shared.  Nothing is scanned or rewritten automatically: ``SharedBytes``
handles are *not* picklable and must be passed to tasks through the
explicit ``shared=`` parameter of :meth:`billiard.pool.Pool.apply_async`.

Every block carries an unforgeable generational identity (a random
token).  The parent registry hands out a tiny reference (name, size,
token) per in-flight task and takes it back when the task settles --
on success, failure, cancellation, serialization failure, worker loss
and pool termination.  Releasing the user handle while tasks are still
in flight only marks the block for reclamation; the underlying segment
is freed once the last in-flight reference is returned.

Platform support
----------------
Implemented on top of :mod:`multiprocessing.shared_memory`
(Python >= 3.8).  Workers always attach to the segment *by name*, so
this works with the ``fork``, ``forkserver`` and ``spawn`` start
methods on POSIX, and with ``spawn`` on Windows -- it never relies on
a forked child happening to inherit the parent's address space.
Requires Python >= 3.8 and a platform with
``multiprocessing.shared_memory`` (all POSIX systems and Windows).
"""
from __future__ import annotations

import threading
import uuid

try:
    from multiprocessing import shared_memory
except ImportError:  # pragma: no cover -- Python < 3.8
    shared_memory = None

__all__ = (
    'SharedBytes',
    'SharedBytesRef',
    'SharedBytesRegistry',
    'SharedBytesUnsupported',
    'SharedBytesReleased',
    'SharedBytesView',
    'share_bytes',
    'default_registry',
)


class SharedBytesUnsupported(RuntimeError):
    """Raised when shared byte blocks are not available on this platform."""


class SharedBytesReleased(ValueError):
    """Raised when using a handle whose block has already been released."""


def _require_shared_memory():
    if shared_memory is None:  # pragma: no cover
        raise SharedBytesUnsupported(
            'shared byte blocks require multiprocessing.shared_memory '
            '(Python >= 3.8)',
        )


def _attach(name):
    """Attach to an existing segment by name, without resource_tracker
    bookkeeping in the (short-lived, non-owning) worker process."""
    _require_shared_memory()
    try:
        # Python >= 3.13: opt out of resource_tracker directly.
        return shared_memory.SharedMemory(name=name, create=False, track=False)
    except TypeError:
        shm = shared_memory.SharedMemory(name=name, create=False)
        try:
            from multiprocessing import resource_tracker
            resource_tracker.unregister(shm._name, 'shared_memory')
        except Exception:
            pass
        return shm


class SharedBytesView:
    """Restricted, read-only view of a shared block, as seen by workers.

    Supports ``len()``, indexing/slicing, ``bytes(view)`` and the
    read-only :attr:`view` memoryview.  It cannot free or unlink the
    parent's segment, and it cannot be pickled again.
    """

    __slots__ = ('_shm', '_view', 'name', 'size', 'token')

    def __init__(self, name, size, token):
        shm = _attach(name)
        if shm.size < size:
            shm.close()
            raise SharedBytesReleased(
                f'shared block {name!r} is smaller than expected',
            )
        self._shm = shm
        self._view = shm.buf[:size].toreadonly()
        self.name = name
        self.size = size
        self.token = token

    @property
    def view(self):
        """Read-only memoryview of the block."""
        return self._view

    def tobytes(self):
        return self._view.tobytes()

    def close(self):
        """Detach this view.  Never affects the parent's segment."""
        view, self._view = self._view, None
        shm, self._shm = self._shm, None
        if view is not None:
            view.release()
        if shm is not None:
            shm.close()

    def __del__(self):  # pragma: no cover -- best effort finalizer
        try:
            self.close()
        except Exception:
            pass

    def __bytes__(self):
        return self.tobytes()

    def __len__(self):
        return self.size

    def __getitem__(self, key):
        return self._view[key]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __reduce__(self):
        raise TypeError(
            'SharedBytesView cannot be pickled; pass SharedBytes handles '
            'to tasks via the explicit shared= parameter',
        )

    def __repr__(self):
        return f'<SharedBytesView: {self.name} size={self.size}>'


class SharedBytesRef:
    """Tiny wire reference to a shared block: (name, size, token).

    Pickling a reference yields a :class:`SharedBytesView` on the
    receiving side; the payload is only a few dozen bytes regardless of
    the block size.
    """

    __slots__ = ('name', 'size', 'token')

    def __init__(self, name, size, token):
        self.name = name
        self.size = size
        self.token = token

    def __reduce__(self):
        return SharedBytesView, (self.name, self.size, self.token)

    def __repr__(self):
        return f'<SharedBytesRef: {self.name} size={self.size}>'


class SharedBytes:
    """Parent-side handle owning a registered shared byte block.

    Created by :meth:`SharedBytesRegistry.register`.  Call :meth:`close`
    (or use it as a context manager) to release the handle; the segment
    itself is reclaimed once every in-flight task reference has also
    been returned.  Handles are deliberately not picklable.
    """

    __slots__ = ('_registry', '_shm', 'name', 'size', 'token', '_released')

    def __init__(self, registry, shm, size, token):
        self._registry = registry
        self._shm = shm
        self.name = shm.name
        self.size = size
        self.token = token
        self._released = False

    @property
    def released(self):
        return self._released

    @property
    def view(self):
        """Read-only memoryview of the block (parent side)."""
        if self._released:
            raise SharedBytesReleased(
                f'shared block {self.name!r} has been released',
            )
        return self._shm.buf[:self.size].toreadonly()

    def tobytes(self):
        return self.view.tobytes()

    def close(self):
        if not self._released:
            self._released = True
            self._registry._user_release(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):  # pragma: no cover -- best effort finalizer
        try:
            self.close()
        except Exception:
            pass

    def __reduce__(self):
        raise TypeError(
            'SharedBytes handles cannot be pickled; pass them to tasks '
            'via the explicit shared= parameter of Pool.apply_async',
        )

    def __repr__(self):
        state = 'released' if self._released else 'active'
        return f'<SharedBytes: {self.name} size={self.size} {state}>'


class _Block:
    __slots__ = ('shm', 'size', 'token', 'user_released', 'inflight')

    def __init__(self, shm, size, token):
        self.shm = shm
        self.size = size
        self.token = token
        self.user_released = False
        self.inflight = 0


class SharedBytesRegistry:
    """Parent-process registry owning all shared byte blocks.

    Tracks one user reference (the :class:`SharedBytes` handle) plus one
    reference per in-flight task.  A block's segment is closed and
    unlinked only when the user released the handle *and* the last
    in-flight task reference was returned.  References carry the block's
    generational token, so stale or duplicate returns can never free a
    newer block that happens to reuse a name.
    """

    def __init__(self):
        self._mutex = threading.Lock()
        self._blocks = {}

    def register(self, data):
        """Register a bytes-like payload and return its handle.

        Only explicit bytes-like payloads (``bytes``, ``bytearray``,
        contiguous ``memoryview``) are accepted.
        """
        _require_shared_memory()
        if isinstance(data, (bytes, bytearray)):
            payload = bytes(data)
        elif isinstance(data, memoryview):
            if not data.contiguous:
                raise TypeError(
                    'shared byte blocks require a contiguous memoryview',
                )
            payload = data.tobytes()
        else:
            raise TypeError(
                'shared byte blocks only accept bytes-like payloads '
                f'(bytes, bytearray, memoryview), got {type(data).__name__}',
            )
        size = len(payload)
        shm = shared_memory.SharedMemory(create=True, size=max(size, 1))
        try:
            shm.buf[:size] = payload
            token = uuid.uuid4().hex
            with self._mutex:
                self._blocks[shm.name] = _Block(shm, size, token)
            return SharedBytes(self, shm, size, token)
        except Exception:
            shm.close()
            shm.unlink()
            raise

    def acquire(self, handle):
        """Take an in-flight task reference to ``handle``."""
        if not isinstance(handle, SharedBytes):
            raise TypeError(
                'shared= expects SharedBytes handles registered with '
                f'this pool, got {type(handle).__name__}',
            )
        with self._mutex:
            block = self._blocks.get(handle.name)
            if block is None or block.token != handle.token:
                raise SharedBytesReleased(
                    f'shared block {handle.name!r} has been released',
                )
            block.inflight += 1
            return SharedBytesRef(handle.name, handle.size, handle.token)

    def release(self, ref):
        """Return an in-flight task reference.

        Ignores stale references whose token does not match the block's
        current generation.
        """
        with self._mutex:
            block = self._blocks.get(ref.name)
            if block is None or block.token != ref.token:
                return  # stale generation: already reclaimed
            if block.inflight <= 0:
                return  # duplicate return of the same reference
            block.inflight -= 1
            self._maybe_free(ref.name, block)

    def release_refs(self, refs):
        for ref in refs or ():
            self.release(ref)

    def _user_release(self, handle):
        with self._mutex:
            block = self._blocks.get(handle.name)
            if block is None or block.token != handle.token:
                return
            block.user_released = True
            self._maybe_free(handle.name, block)

    def _maybe_free(self, name, block):
        # called with self._mutex held
        if block.user_released and block.inflight <= 0:
            del self._blocks[name]
            try:
                block.shm.close()
            except BufferError:
                # the user still holds an exported view of the segment;
                # the mapping is reclaimed when that view is released.
                pass
            block.shm.unlink()

    @property
    def blocks(self):
        """Snapshot of live block names (for introspection/tests)."""
        with self._mutex:
            return set(self._blocks)

    @property
    def inflight(self):
        """Total number of in-flight task references."""
        with self._mutex:
            return sum(b.inflight for b in self._blocks.values())


_default_registry = None
_default_registry_lock = threading.Lock()


def default_registry():
    """Process-wide default registry used by pools by default."""
    global _default_registry
    with _default_registry_lock:
        if _default_registry is None:
            _default_registry = SharedBytesRegistry()
        return _default_registry


def share_bytes(data):
    """Register ``data`` with the default registry; see
    :meth:`SharedBytesRegistry.register`."""
    return default_registry().register(data)
