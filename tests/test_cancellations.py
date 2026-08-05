import typing

import anyio
import hpack
import hyperframe
import pytest

import httpcore


class SlowWriteStream(httpcore.AsyncNetworkStream):
    """
    A stream that we can use to test cancellations during
    the request writing.
    """

    async def write(
        self, buffer: bytes, timeout: typing.Optional[float] = None
    ) -> None:
        await anyio.sleep(999)

    async def aclose(self) -> None:
        await anyio.sleep(0)


class HandshakeThenSlowWriteStream(httpcore.AsyncNetworkStream):
    """
    A stream that we can use to test cancellations during
    the HTTP/2 request writing, after allowing the initial
    handshake to complete.
    """

    def __init__(self) -> None:
        self._handshake_complete = False

    async def write(
        self, buffer: bytes, timeout: typing.Optional[float] = None
    ) -> None:
        if not self._handshake_complete:
            self._handshake_complete = True
        else:
            await anyio.sleep(999)

    async def aclose(self) -> None:
        await anyio.sleep(0)


class SlowReadStream(httpcore.AsyncNetworkStream):
    """
    A stream that we can use to test cancellations during
    the response reading.
    """

    def __init__(self, buffer: typing.List[bytes]):
        self._buffer = buffer

    async def write(self, buffer, timeout=None):
        pass

    async def read(
        self, max_bytes: int, timeout: typing.Optional[float] = None
    ) -> bytes:
        if not self._buffer:
            await anyio.sleep(999)
        return self._buffer.pop(0)

    async def aclose(self):
        await anyio.sleep(0)


class SlowWriteBackend(httpcore.AsyncNetworkBackend):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        return SlowWriteStream()


class SlowReadBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, buffer: typing.List[bytes]):
        self._buffer = buffer

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        return SlowReadStream(self._buffer)


@pytest.mark.anyio
async def test_connection_pool_timeout_during_request():
    """
    An async timeout when writing an HTTP/1.1 response on the connection pool
    should leave the pool in a consistent state.

    In this case, that means the connection will become closed, and no
    longer remain in the pool.
    """
    network_backend = SlowWriteBackend()
    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        with anyio.move_on_after(0.01):
            await pool.request("GET", "http://example.com")
        assert not pool.connections


@pytest.mark.anyio
async def test_connection_pool_timeout_during_response():
    """
    An async timeout when reading an HTTP/1.1 response on the connection pool
    should leave the pool in a consistent state.

    In this case, that means the connection will become closed, and no
    longer remain in the pool.
    """
    network_backend = SlowReadBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 1000\r\n",
            b"\r\n",
            b"Hello, world!...",
        ]
    )
    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        with anyio.move_on_after(0.01):
            await pool.request("GET", "http://example.com")
        assert not pool.connections


@pytest.mark.anyio
async def test_h11_timeout_during_request():
    """
    An async timeout on an HTTP/1.1 during the request writing
    should leave the connection in a neatly closed state.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = SlowWriteStream()
    async with httpcore.AsyncHTTP11Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")
        assert conn.is_closed()


@pytest.mark.anyio
async def test_h11_timeout_during_response():
    """
    An async timeout on an HTTP/1.1 during the response reading
    should leave the connection in a neatly closed state.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = SlowReadStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 1000\r\n",
            b"\r\n",
            b"Hello, world!...",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")
        assert conn.is_closed()


@pytest.mark.anyio
async def test_h2_timeout_during_handshake():
    """
    An async timeout on an HTTP/2 during the initial handshake
    should leave the connection in a neatly closed state.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = SlowWriteStream()
    async with httpcore.AsyncHTTP2Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")
        assert conn.is_closed()


@pytest.mark.anyio
async def test_h2_timeout_during_request():
    """
    An async timeout during an HTTP/2 request write must make the
    connection unusable.

    `h2`'s `data_to_send()` drains frames out of the HTTP/2 state before
    the network write runs; a cancellation delivered in the write window
    discards those frames (HPACK dynamic-table updates included), leaving
    the connection's HTTP/2 state out of sync with the peer — even when
    zero bytes reached the wire. Reusing the connection would silently
    lose requests, so it must no longer be offered to the pool, and
    later writes on it must fail loudly rather than hang.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = HandshakeThenSlowWriteStream()
    async with httpcore.AsyncHTTP2Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")

        assert not conn.is_available()

        with pytest.raises(httpcore.WriteError):
            await conn.request("GET", "http://example.com")


@pytest.mark.anyio
async def test_h2_cancellation_fails_concurrent_streams_fast():
    """
    A cancellation during one stream's write poisons the multiplexed
    connection for every other stream on it. Requests already waiting
    on the connection's write lock must fail promptly and loudly
    (rather than writing into the desynchronized connection, or
    hanging).
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = HandshakeThenSlowWriteStream()
    async with httpcore.AsyncHTTP2Connection(origin, stream) as conn:
        other_error: typing.Optional[Exception] = None

        async def other_request() -> None:
            nonlocal other_error
            # Give the first request time to take the h2 write lock.
            await anyio.sleep(0.05)
            try:
                with anyio.fail_after(2):
                    await conn.request("GET", "http://example.com")
            except Exception as exc:
                other_error = exc

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(other_request)
            with anyio.move_on_after(0.1):
                await conn.request("GET", "http://example.com")

        assert isinstance(other_error, httpcore.WriteError)
        assert not conn.is_available()


class CancelledWriteThenWorkingBackend(httpcore.AsyncNetworkBackend):
    """
    The first connection stalls during the request write (so that the
    request gets cancelled mid-write); subsequent connections serve a
    valid HTTP/2 response.
    """

    def __init__(self) -> None:
        self.connect_count = 0

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        self.connect_count += 1
        if self.connect_count == 1:
            return HandshakeThenSlowWriteStream()
        return httpcore.AsyncMockStream(
            [
                hyperframe.frame.SettingsFrame().serialize(),
                hyperframe.frame.HeadersFrame(
                    stream_id=1,
                    data=hpack.Encoder().encode(
                        [
                            (b":status", b"200"),
                            (b"content-type", b"plain/text"),
                        ]
                    ),
                    flags=["END_HEADERS"],
                ).serialize(),
                hyperframe.frame.DataFrame(
                    stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
                ).serialize(),
            ]
        )


@pytest.mark.anyio
async def test_h2_pool_does_not_reuse_connection_after_cancelled_write():
    """
    After a request on a shared HTTP/2 connection is cancelled mid-write,
    the pool must not reuse that connection: the next request must be
    served on a fresh connection.
    """
    network_backend = CancelledWriteThenWorkingBackend()
    async with httpcore.AsyncConnectionPool(
        http1=False, http2=True, network_backend=network_backend
    ) as pool:
        with anyio.move_on_after(0.01):
            await pool.request("GET", "http://example.com")

        with anyio.fail_after(2):
            response = await pool.request("GET", "http://example.com")

        assert response.status == 200
        assert network_backend.connect_count == 2


@pytest.mark.anyio
async def test_h2_timeout_during_response():
    """
    An async timeout on an HTTP/2 during the response reading
    should leave the connection in a neatly idle state.

    The connection is not closed because it is multiplexed,
    and a timeout on one request does not require the entire
    connection be closed.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = SlowReadStream(
        [
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=1, data=b"Hello, world!...", flags=[]
            ).serialize(),
        ]
    )
    async with httpcore.AsyncHTTP2Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")

        assert not conn.is_closed()
        assert conn.is_idle()
        # A cancellation during the response *read* loses no outgoing
        # frames, so the multiplexed connection remains safe to reuse.
        assert conn.is_available()
