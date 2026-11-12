import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from redis.asyncio import RedisCluster
from taskiq import ScheduledTask
from taskiq.cli.scheduler.run import get_schedules

from taskiq_redis import RedisClusterScheduleSource


@pytest.mark.anyio
@pytest.mark.parametrize("via_scheduler", [False, True])
@pytest.mark.parametrize("keep_live", [False, True])
async def test_cluster_schedule_removed_after_scan(
    redis_cluster_url: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    via_scheduler: bool,
    keep_live: bool,
) -> None:
    prefix = uuid.uuid4().hex
    reader = RedisClusterScheduleSource(redis_cluster_url, prefix=prefix)
    writer = RedisClusterScheduleSource(redis_cluster_url, prefix=prefix)
    gone = ScheduledTask(
        schedule_id="{0}:gone",
        task_name="gone",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    live = gone.model_copy(update={"schedule_id": "{1}:live", "task_name": "live"})
    scanned = asyncio.Event()
    deleted = asyncio.Event()
    original_scan = RedisCluster.scan_iter
    removed_key = f"{prefix}:{gone.schedule_id}".encode()
    pending: asyncio.Task[list[ScheduledTask]] | None = None

    async def scan_with_barrier(
        client: RedisCluster,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncIterator[bytes]:
        async for key in original_scan(client, *args, **kwargs):
            if client is reader.redis and key == removed_key:
                # The key really came from Redis. Hold it before the source's
                # GET so a separate client's actual DELETE happens in between.
                scanned.set()
                await deleted.wait()
            yield key

    try:
        await writer.add_schedule(gone)
        if keep_live:
            await writer.add_schedule(live)
        monkeypatch.setattr(RedisCluster, "scan_iter", scan_with_barrier)
        pending = asyncio.create_task(
            get_schedules(reader) if via_scheduler else reader.get_schedules(),
        )
        await asyncio.wait_for(scanned.wait(), 5)
        await writer.delete_schedule(gone.schedule_id)
        assert not await writer.redis.exists(removed_key)
        deleted.set()
        expected = [live] if keep_live else []
        assert await asyncio.wait_for(pending, 5) == expected
        assert not any(record.levelno >= logging.ERROR for record in caplog.records)

        assert await reader.get_schedules() == expected
        await writer.add_schedule(gone)
        restored = await reader.get_schedules()
        assert {task.schedule_id: task for task in restored} == {
            task.schedule_id: task for task in [gone, *expected]
        }
    finally:
        deleted.set()
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await writer.delete_schedule(gone.schedule_id)
        await writer.delete_schedule(live.schedule_id)
        await reader.shutdown()
        await writer.shutdown()


@pytest.mark.anyio
async def test_cluster_schedule_invalid_payload_is_not_ignored(
    redis_cluster_url: str,
) -> None:
    prefix = uuid.uuid4().hex
    source = RedisClusterScheduleSource(redis_cluster_url, prefix=prefix)
    try:
        await source.redis.set(f"{prefix}:invalid", b"")
        with pytest.raises(EOFError):
            await source.get_schedules()
    finally:
        await source.delete_schedule("invalid")
        await source.shutdown()
