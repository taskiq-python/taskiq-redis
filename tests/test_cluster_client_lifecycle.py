import asyncio
import uuid
from collections.abc import Callable, Coroutine
from typing import Any, Literal

import pytest
from redis.asyncio import Redis, RedisCluster
from redis.asyncio.connection import Connection
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from taskiq import TaskiqResult
from taskiq.depends.progress_tracker import TaskProgress

from taskiq_redis import RedisAsyncClusterResultBackend
from taskiq_redis.redis_backend import PROGRESS_KEY_SUFFIX


class ResponseGate:
    """Relay real Redis traffic, holding one command's response on this client."""

    def __init__(self, target: tuple[str, int], request: bytes) -> None:
        self.target = target
        self.request = request
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.server: asyncio.Server | None = None
        self.port = 0
        self.handlers: set[asyncio.Task[Any]] = set()

    async def start(self) -> None:
        """Listen on a private ephemeral loopback port."""
        self.server = await asyncio.start_server(self.relay, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    def remap(self, address: tuple[str, int]) -> tuple[str, int]:
        """Route only the selected cluster node through this relay."""
        return ("127.0.0.1", self.port) if address == self.target else address

    async def relay(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Forward bytes unchanged; never fabricate a Redis response."""
        handler = asyncio.current_task()
        assert handler is not None
        self.handlers.add(handler)
        upstream_writer: asyncio.StreamWriter | None = None
        tasks: list[asyncio.Task[None]] = []
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                *self.target,
            )
            hold = False

            async def requests() -> None:
                nonlocal hold
                recent = b""
                while data := await reader.read(65536):
                    recent = (recent + data)[-65536:]
                    if self.request in recent:
                        hold = True
                        recent = b""
                    assert upstream_writer is not None
                    upstream_writer.write(data)
                    await upstream_writer.drain()

            async def responses() -> None:
                nonlocal hold
                while data := await upstream_reader.read(65536):
                    if hold:
                        self.entered.set()
                        await self.release.wait()
                        hold = False
                    writer.write(data)
                    await writer.drain()

            tasks = [asyncio.create_task(requests()), asyncio.create_task(responses())]
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            writer.close()
            if upstream_writer is not None:
                upstream_writer.close()
                await upstream_writer.wait_closed()
            await writer.wait_closed()
            self.handlers.discard(handler)

    async def close(self) -> None:
        """Close the listener and all test-owned relay connections."""
        self.release.set()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        handlers = list(self.handlers)
        for handler in handlers:
            handler.cancel()
        await asyncio.gather(*handlers, return_exceptions=True)


async def assert_cluster_shutdown(
    backend: RedisAsyncClusterResultBackend[int],
    target: tuple[str, int],
    client_name: str,
) -> None:
    """Check live server connections disappear when the backend shuts down."""
    async with Redis(host=target[0], port=target[1]) as observer:
        assert any(
            client["name"] == client_name for client in await observer.client_list()
        )
        await backend.shutdown()

        async def wait_for_disconnect() -> None:
            while any(
                client["name"] == client_name for client in await observer.client_list()
            ):
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_disconnect(), 5)


async def start_response_gate(
    redis_cluster_url: str,
    read_key: str,
    read_kind: str,
) -> tuple[ResponseGate, tuple[str, int]]:
    """Find the actual node and start a private relay for the selected read."""
    command = {"ready": "EXISTS", "consume": "GETDEL"}.get(read_kind, "GET")
    request = b"".join(Connection().pack_command(command, read_key))
    async with RedisCluster.from_url(redis_cluster_url) as locator:
        node = locator.get_node_from_key(read_key)
        assert node is not None
        target = (node.host, int(node.port))
    gate = ResponseGate(target, request)
    await gate.start()
    return gate, target


@pytest.mark.anyio
@pytest.mark.parametrize("write_kind", ["result", "progress"])
@pytest.mark.parametrize("read_kind", ["result", "consume", "progress", "ready"])
async def test_cluster_write_preserves_pending_read(
    redis_cluster_url: str,
    write_kind: Literal["result", "progress"],
    read_kind: Literal["result", "consume", "progress", "ready"],
) -> None:
    token = uuid.uuid4().hex
    read_id = f"{{{token}}}:read"
    write_id = f"{{{token}}}:write"
    read_key = read_id + PROGRESS_KEY_SUFFIX if read_kind == "progress" else read_id
    gate, target = await start_response_gate(redis_cluster_url, read_key, read_kind)
    client_name = f"taskiq-cluster-{token}"
    backend: RedisAsyncClusterResultBackend[int] = RedisAsyncClusterResultBackend(
        redis_cluster_url,
        address_remap=gate.remap,
        client_name=client_name,
        retry=Retry(NoBackoff(), 0),
        keep_results=read_kind != "consume",
    )
    result = TaskiqResult[int](is_err=False, return_value=42, execution_time=0)
    progress = TaskProgress[int](state="running", meta=7)
    pending: asyncio.Task[Any] | None = None
    stopped = False
    keys = (
        read_id,
        write_id,
        read_id + PROGRESS_KEY_SUFFIX,
        write_id + PROGRESS_KEY_SUFFIX,
    )
    try:
        await backend.redis.initialize()
        await backend.set_result(read_id, result)
        await backend.set_progress(read_id, progress)
        routed_node = backend.redis.get_node_from_key(read_id)
        assert routed_node is not None and int(routed_node.port) == gate.port
        reads: dict[str, Callable[[str], Coroutine[Any, Any, Any]]] = {
            "result": backend.get_result,
            "consume": backend.get_result,
            "progress": backend.get_progress,
            "ready": backend.is_result_ready,
        }
        pending = asyncio.create_task(reads[read_kind](read_id))
        # The real server has returned the response, but the relay has not
        # delivered it to this client's socket. Other clients are unaffected.
        await asyncio.wait_for(gate.entered.wait(), 5)
        if write_kind == "result":
            await backend.set_result(write_id, result)
        else:
            await backend.set_progress(write_id, progress)
        gate.release.set()
        expected = {
            "result": result,
            "consume": result,
            "progress": progress,
            "ready": True,
        }
        assert await asyncio.wait_for(pending, 5) == expected[read_kind]

        if write_kind == "result":
            assert await backend.get_result(write_id) == result
        else:
            assert await backend.get_progress(write_id) == progress
        await backend.set_result(write_id, result)
        assert await backend.get_result(write_id) == result
        await backend.redis.delete(*keys)
        await assert_cluster_shutdown(backend, target, client_name)
        stopped = True
    finally:
        gate.release.set()
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        try:
            if not stopped:
                await backend.redis.delete(*keys)
                await backend.shutdown()
        finally:
            await gate.close()
