import asyncio
import atexit
import ctypes
import enum
import os

import liburing

libc = ctypes.CDLL(None)


class RequestType(str, enum.Enum):
    OPEN = 'OPEN'
    CLOSE = 'CLOSE'
    READ = 'READ'
    WRITE = 'WRITE'


class Uring:
    def __init__(self, loop=None, queue_size=32):
        self.event_fd = libc.eventfd(0, os.O_NONBLOCK | os.O_CLOEXEC)
        self.ring = liburing.io_uring()
        self.cqes = liburing.io_uring_cqes(1)
        self.store = {}
        self.loop = loop
        self.queue_size = queue_size
        self._setup_done = False

    def setup(self):
        liburing.io_uring_queue_init(self.queue_size, self.ring)
        liburing.io_uring_register_eventfd(self.ring, self.event_fd)

        if self.loop is None:
            self.loop = asyncio.get_event_loop()
        self.loop.add_reader(self.event_fd, self.eventfd_callback)

        atexit.register(self.cleanup)
        self._setup_done = True

    def eventfd_callback(self):
        libc.eventfd_read(self.event_fd, os.O_NONBLOCK | os.O_CLOEXEC)
        while True:
            try:
                liburing.io_uring_peek_cqe(self.ring, self.cqes)
            except BlockingIOError:
                break
            cqe = self.cqes[0]
            result = liburing.trap_error(cqe.res)
            request_type, future, *args = self.store[cqe.user_data]

            if request_type is RequestType.OPEN:
                future.set_result(result)
            elif request_type is RequestType.CLOSE:
                future.set_result(None)
            elif request_type is RequestType.READ:
                future.set_result(args[0])
            elif request_type is RequestType.WRITE:
                future.set_result(None)
            else:
                raise RuntimeError

            del self.store[cqe.user_data]
            liburing.io_uring_cqe_seen(self.ring, cqe)

    def _get_sqe(self):
        if not self._setup_done:
            self.setup()

        return liburing.io_uring_get_sqe(self.ring)

    def _submit(self, request_type, sqe, *args):
        future = asyncio.Future()
        sqe.user_data = id(future)
        self.store[sqe.user_data] = request_type, future, *args
        liburing.io_uring_submit(self.ring)
        return future

    def submit_open_entry(self, fpath, flags, mode, dir_fd):
        sqe = self._get_sqe()
        path = os.path.abspath(fpath).encode()
        liburing.io_uring_prep_openat(sqe, dir_fd, path, flags, mode)
        return self._submit(RequestType.OPEN, sqe, path)

    def submit_close_entry(self, fd):
        sqe = self._get_sqe()
        liburing.io_uring_prep_close(sqe, fd)
        return self._submit(RequestType.CLOSE, sqe)

    def submit_read_entry(self, fd, size, offset):
        sqe = self._get_sqe()
        array = bytearray(size)
        iovecs = liburing.iovec(array)
        liburing.io_uring_prep_readv(sqe, fd, iovecs, len(iovecs), offset)
        return self._submit(RequestType.READ, sqe, array)

    def submit_write_entry(self, fd, data, offset):
        sqe = self._get_sqe()
        iovecs = liburing.iovec(data)
        liburing.io_uring_prep_writev(sqe, fd, iovecs, len(iovecs), offset)
        return self._submit(RequestType.WRITE, sqe)

    def cleanup(self):
        liburing.io_uring_unregister_eventfd(self.ring)
        liburing.io_uring_queue_exit(self.ring)
        self.loop.remove_reader(self.event_fd)
        self._setup_done = False


_DEFAULT_URING = Uring()


class File:
    def __init__(self, fpath, uring=_DEFAULT_URING):
        self.fpath = fpath
        self._fd = None
        self._offset = 0
        self._uring = uring

    def open(self, flags=os.O_RDONLY, mode=0o660, dir_fd=-1):
        assert self._fd is None

        async def _open_wrapper():
            self._fd = await self._uring.submit_open_entry(self.fpath, flags, mode, dir_fd)
            return self

        async def _close_wrapper():
            await self.close()

        class _MaybeContextManager:
            def __await__(self):
                return _open_wrapper().__await__()

            async def __aenter__(self):
                return await _open_wrapper()

            async def __aexit__(self, *_):
                await _close_wrapper()

        return _MaybeContextManager()

    async def close(self):
        assert self._fd is not None
        await self._uring.submit_close_entry(self._fd)
        self._offset = 0
        self._fd = None

    async def read(self, size=None):
        assert self._fd is not None
        fsize = os.stat(self.fpath).st_size
        if size is None:
            size = fsize

        size = min(size, fsize - self._offset)
        if size <= 0:
            return b''

        data = await self._uring.submit_read_entry(self._fd, size, self._offset)
        self._offset += len(data)
        return bytes(data)

    async def readline(self):
        line = bytearray()
        while (chunk := await self.read(32)):
            if (nl := chunk.find(b'\n')) != -1:
                line += chunk[:nl]
                self._offset += nl + 1 - len(chunk)
                break
            else:
                line += chunk
        return bytes(line)

    async def __aiter__(self):
        while (line := await self.readline()):
            yield line

    async def write(self, data):
        await self._uring.submit_write_entry(self._fd, data, self._offset)
        self._offset += len(data)

    def seek(self, offset):
        self._offset = offset

    def fileno(self):
        return self._fd


def open(fpath, flags=os.O_RDONLY, mode=0o660, dir_fd=-1):
    return File(fpath).open(flags, mode, dir_fd)
