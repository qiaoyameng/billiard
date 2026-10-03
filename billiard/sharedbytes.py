"""Explicit shared read-only byte blocks for pool tasks.

This module lets the parent process of a :class:`billiard.pool.Pool`
share a single large read-only bytes-like payload with many worker
processes without copying it through the task pipe for every task.

The parent creates and owns a :class:`SharedBytes` block::

    pool = billiard.Pool(4)
    block = pool.shared_bytes(large_payload)
    results = [pool.apply_async(consume, (i,), shared=(block,))
               for i in range(100)]
    block.close()  # release the user handle; in-flight tasks still finish

Each task submitted with ``shared=(block,)`` receives the block as a
:class:`SharedBytesView` appended to its positional arguments, and only a
small :class:`SharedBytesHandle` (name + generational identity) travels
through the task pipe.

Only payloads explicitly registered with :meth:`Pool.shared_bytes` and
passed via the ``shared`` parameter are transferred this way.  The pool
never scans or rewrites arbitrary object graphs: ``SharedBytes``
instances are not picklable and cannot be smuggled through regular
task arguments.

Platform support
----------------
Shared blocks are implemented on top of
:mod:`multiprocessing.shared_memory` and require Python >= 3.8.

- POSIX (Linux, macOS, *BSD): supported with the ``fork``, ``spawn``
  and ``forkserver`` start methods.
- Windows: supported with the ``spawn`` start method (the only one
  available).  The segment is reclaimed by the OS once the last handle
  to it is closed.
- Platforms without :mod:`multiprocessing.shared_memory` raise
  :exc:`NotImplementedError` when a block is created.

Workers always attach to the segment *by name*; correctness never
depends on a child process happening to inherit the parent's address
space, so ``fork`` is not required.
"""

import itertools
import secrets
import threading

__all__ = (
    'SharedBytes',
    'SharedBytesError',
    'SharedBytesHandle',
    'SharedBytesRegistry',
    'SharedBytesReleased',
    'SharedBytesView',
    'shared_bytes_supported',
)

try:
    from multiprocessing.shared_memory import SharedMemory
except ImportError:  # pragma: no cover - Python < 3.8
    SharedMemory = None

try:
    from multiprocessing import resource_tracker
except ImportError:  # pragma: no cover
    resource_tracker = None


_generation_counter = itertools.count()


class SharedBytesError(Exception):
    """Base class for shared-bytes errors."""


class SharedBytesReleased(SharedBytesError):
    """The shared block has been released and can no longer be used."""


def shared_bytes_supported():
    """Return True if shared byte blocks are usable on this platform."""
    return SharedMemory is not None


def _untrack(shm):
    # Detach the segment from this process's resource tracker.  The
    # registry manages the segment lifetime explicitly: otherwise the
    # tracker's atexit cleanup would unlink the segment when an
    # attaching worker exits, even while the parent and other workers
    # still use it.
    if resource_tracker is None:  # pragma: no cover
        return
    try:
        resource_tracker.unregister(shm._name, 'shared_memory')
    except (AttributeError, KeyError, ValueError):
        pass


def _create_segment(size):
    try:
        return SharedMemory(create=True, size=size, track=False)
    except TypeError:  # Python < 3.13 has no ``track`` argument.
        shm = SharedMemory(create=True, size=size)
        _untrack(shm)
        return shm


def _attach_segment(name):
    try:
        return SharedMemory(name=name, create=False, track=False)
    except TypeError:  # Python < 3.13 has no ``track`` argument.
        shm = SharedMemory(name=name, create=False)
        _untrack(shm)
        return shm


def _unlink_segment(shm):
    # Unlink without SharedMemory.unlink(): that method also unregisters
    # the segment from the resource tracker on Python < 3.13, but the
    # segment was already untracked at creation/attach time, which would
    # make the tracker choke on the duplicate UNREGISTER.  On Windows
    # there is no unlink: the segment is reclaimed once the last handle
    # to it is closed.
    name = getattr(shm, '_name', None)
    if not name:
        return
    try:
        import _posixshmem
    except ImportError:
        return
    try:
        _posixshmem.shm_unlink(name)
    except FileNotFoundError:
        pass


class SharedBytesHandle:
    """Restricted, picklable reference to a :class:`SharedBytes` block.

    The handle carries an unforgeable generational identity:
    ``(name, generation, token)``.  ``generation`` distinguishes blocks
    that reuse a recycled segment name, and ``token`` is a random
    128-bit secret so handles cannot be guessed or forged.

    A handle can only attach a read-only view in the receiving process;
    it cannot release or unlink the parent-owned segment.
    """

    __slots__ = ('name', 'generation', 'token', 'size')

    def __init__(self, name, generation, token, size):
        self.name = name
        self.generation = generation
        self.token = token
        self.size = size

    @property
    def identity(self):
        return (self.name, self.generation)

    def __eq__(self, other):
        if not isinstance(other, SharedBytesHandle):
            return NotImplemented
        return (
            self.identity == other.identity and
            self.token == other.token and
            self.size == other.size
        )

    def __hash__(self):
        return hash((self.identity, self.token))

    def __repr__(self):
        return (
            f'<{type(self).__name__}: {self.name} '
            f'gen={self.generation} size={self.size}>'
        )

    def __reduce__(self):
        return (
            type(self),
            (self.name, self.generation, self.token, self.size),
        )

    def attach(self):
        """Attach a read-only view of the block in this process."""
        return SharedBytesView(self)


class SharedBytesView:
    """Read-only view of a shared block, attached in the local process.

    Supports the buffer protocol through :meth:`view`.  ``close()``
    only detaches the local mapping; the segment itself is owned and
    released by the parent process.
    """

    __slots__ = ('handle', '_shm', '_view')

    def __init__(self, handle, _shm=None):
        self._shm = None
        self._view = None
        shm = _shm if _shm is not None else _attach_segment(handle.name)
        if shm.size < handle.size:
            shm.close()
            raise SharedBytesError(
                f'segment {handle.name!r} is smaller than the handle '
                f'declares ({shm.size} < {handle.size}): stale handle?'
            )
        self.handle = handle
        self._shm = shm
        self._view = shm.buf[:handle.size].toreadonly()

    def view(self):
        """Return a read-only memoryview of the shared bytes."""
        if self._view is None:
            raise SharedBytesReleased('view is closed')
        return self._view

    def tobytes(self):
        """Return a copy of the shared bytes."""
        return self.view().tobytes()

    def __len__(self):
        return self.handle.size

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        view, self._view = self._view, None
        shm, self._shm = self._shm, None
        if view is not None:
            view.release()
        if shm is not None:
            shm.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class SharedBytes:
    """Parent-owned shared read-only byte block.

    Created by :meth:`billiard.pool.Pool.shared_bytes`.  The block is
    reference counted: the user holds one reference (released with
    :meth:`close`) and every in-flight task submitted with
    ``shared=(block,)`` holds another.  The underlying segment is only
    unlinked once the user reference *and* all in-flight task
    references have been returned, so releasing the user handle never
    breaks tasks that are still running.
    """

    def __init__(self, data, _registry=None):
        if SharedMemory is None:
            raise NotImplementedError(
                'shared byte blocks require multiprocessing.shared_memory '
                '(Python >= 3.8); not available on this platform'
            )
        try:
            payload = memoryview(data)
        except TypeError:
            raise TypeError(
                f'shared_bytes() requires a bytes-like object, '
                f'not {type(data).__name__!r}'
            ) from None
        if payload.ndim != 1:
            raise TypeError('shared_bytes() requires a 1-d bytes-like object')
        payload = payload.cast('B') if payload.format != 'B' else payload
        self.size = payload.nbytes
        self._shm = _create_segment(max(self.size, 1))
        if self.size:
            self._shm.buf[:self.size] = payload
        self.name = self._shm.name
        self.generation = next(_generation_counter)
        self.token = secrets.token_bytes(16)
        self._registry = _registry
        self._lock = threading.Lock()
        self._refcount = 0
        self._user_released = False
        self._released = False

    @property
    def identity(self):
        """Generational identity ``(name, generation)`` of this block."""
        return (self.name, self.generation)

    @property
    def handle(self):
        """Restricted picklable handle for this block."""
        with self._lock:
            if self._released:
                raise SharedBytesReleased(
                    f'shared block {self.name!r} has been released'
                )
            return SharedBytesHandle(
                self.name, self.generation, self.token, self.size,
            )

    @property
    def released(self):
        """True once the underlying segment has been unlinked."""
        return self._released

    @property
    def refcount(self):
        """Number of in-flight task references (parent process only)."""
        return self._refcount

    def close(self):
        """Release the user reference to this block.

        The segment is unlinked as soon as this reference and all
        in-flight task references are gone.
        """
        with self._lock:
            if self._released or self._user_released:
                return
            self._user_released = True
            finalize = self._refcount == 0
        if finalize:
            self._finalize()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def _acquire(self):
        with self._lock:
            if self._released:
                raise SharedBytesReleased(
                    f'shared block {self.name!r} has been released'
                )
            self._refcount += 1

    def _release_ref(self):
        with self._lock:
            if self._refcount > 0:
                self._refcount -= 1
            finalize = (
                self._refcount == 0 and
                self._user_released and
                not self._released
            )
        if finalize:
            self._finalize()

    def _finalize(self):
        with self._lock:
            if self._released:
                return
            self._released = True
        try:
            self._shm.close()
        finally:
            _unlink_segment(self._shm)
        registry, self._registry = self._registry, None
        if registry is not None:
            registry._forget(self)

    def __reduce__(self):
        raise TypeError(
            'SharedBytes blocks cannot be pickled as task arguments; '
            "pass them explicitly via the 'shared' parameter of "
            'Pool.apply_async() instead'
        )

    def __repr__(self):
        return (
            f'<{type(self).__name__}: {self.name} gen={self.generation} '
            f'size={self.size} refs={self._refcount}>'
        )


class SharedBytesRegistry:
    """Tracks shared blocks and in-flight task references.

    Lives in the parent process, owned by the pool.  References are
    returned when a task finishes, is cancelled, fails to serialize, or
    its worker is lost; :meth:`close_all` releases everything when the
    pool is torn down.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._blocks = {}    # (name, generation) -> SharedBytes
        self._job_refs = {}  # job id -> [SharedBytes, ...]

    def create(self, data):
        """Create and register a new :class:`SharedBytes` block."""
        block = SharedBytes(data, _registry=self)
        with self._lock:
            self._blocks[block.identity] = block
        return block

    def acquire(self, job, blocks):
        """Take a task reference on each block; return their handles."""
        blocks = tuple(blocks)
        for block in blocks:
            if not isinstance(block, SharedBytes):
                raise TypeError(
                    f"'shared' only accepts registered SharedBytes blocks, "
                    f'not {type(block).__name__!r}'
                )
        with self._lock:
            for block in blocks:
                if self._blocks.get(block.identity) is not block:
                    raise ValueError(
                        'shared block was not registered with this pool'
                    )
                block._acquire()
            self._job_refs.setdefault(job, []).extend(blocks)
        return tuple(block.handle for block in blocks)

    def release_job(self, job):
        """Return all shared-block references held by a task."""
        with self._lock:
            blocks = self._job_refs.pop(job, ())
        for block in blocks:
            block._release_ref()

    def verify(self, handle):
        """True if the handle identifies a live registered block."""
        with self._lock:
            block = self._blocks.get(handle.identity)
        return (
            block is not None and
            not block.released and
            secrets.compare_digest(block.token, handle.token)
        )

    def _forget(self, block):
        with self._lock:
            self._blocks.pop(block.identity, None)

    def close_all(self):
        """Release every remaining task reference and unlink all blocks."""
        with self._lock:
            refs = list(self._job_refs.values())
            self._job_refs.clear()
            blocks = list(self._blocks.values())
        for job_blocks in refs:
            for block in job_blocks:
                block._release_ref()
        for block in blocks:
            block._finalize()

    def __len__(self):
        return len(self._blocks)
