"""按 Frozen UMO 与稳定 Source Event 批次隔离语义分析。"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
import inspect
import time

from .identity import UmoIdentity
from .memory_events import MemoryEventLedger, SourceEvent


@dataclass
class _ScopeBuffer:
    identity: UmoIdentity
    events: list[SourceEvent] = field(default_factory=list)
    first_seen: float = 0.0
    attempts: int = 0


@dataclass
class _QueuedBatch:
    events: list[SourceEvent]
    attempts: int = 0
    failure_pending: bool = False
    failure_sink_attempted: bool = False
    failure_error: str = ""


class AnalysisScheduler:
    """按完整 UMO 背压；pending_count 是已接纳且尚未完成/转存的事件数。"""

    def __init__(
        self,
        analyzer,
        *,
        batch_size: int = 3,
        flush_interval_seconds: float = 120,
        max_concurrency: int = 2,
        max_queue_size: int = 20,
        max_retries: int = 3,
        failure_sink=None,
        close_timeout_seconds: float = 10,
    ) -> None:
        self.analyzer = analyzer
        self.batch_size = max(1, int(batch_size))
        self.flush_interval_seconds = max(1.0, float(flush_interval_seconds))
        self.max_queue_size = max(1, int(max_queue_size))
        self.max_retries = max(1, int(max_retries))
        self.failure_sink = failure_sink
        self.close_timeout_seconds = max(0.1, float(close_timeout_seconds))
        self._buffers: dict[tuple[str, str], _ScopeBuffer] = {}
        self._queues: dict[tuple[str, str], deque[_QueuedBatch]] = {}
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._analysis_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._timers: dict[tuple[str, str], asyncio.Task] = {}
        self._inflight: dict[
            tuple[str, str], tuple[UmoIdentity, list[SourceEvent], int]
        ] = {}
        self._active_flush_tasks: set[asyncio.Task] = set()
        self._active_flush_scopes: dict[asyncio.Task, tuple[str, str]] = {}
        self._semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))
        self._capacity_conditions: dict[tuple[str, str], asyncio.Condition] = {}
        self._capacity_waiters: dict[tuple[str, str], deque[object]] = {}
        self._accepted_counts: dict[tuple[str, str], int] = {}
        self._closed = False
        self._close_error: BaseException | None = None

    @staticmethod
    def _key(identity: UmoIdentity) -> tuple[str, str]:
        return identity.scope_key, identity.umo_id

    def _capacity_condition(self, key: tuple[str, str]) -> asyncio.Condition:
        return self._capacity_conditions.setdefault(key, asyncio.Condition())

    async def submit(
        self,
        identity: UmoIdentity,
        message: str,
        *,
        urgent: bool = False,
        source_event: SourceEvent | None = None,
    ) -> None:
        text = str(message or "").strip()
        if not text:
            return
        event = source_event or SourceEvent.transient(identity, text)
        if event.identity != identity:
            raise ValueError("source_event_batch_identity_mismatch")
        key = self._key(identity)
        condition = self._capacity_condition(key)
        ticket = object()
        should_flush = False

        async with condition:
            if self._closed:
                raise RuntimeError("scheduler_closed")
            waiters = self._capacity_waiters.setdefault(key, deque())
            waiters.append(ticket)
            condition.notify_all()
            admitted = False
            try:
                await condition.wait_for(
                    lambda: self._closed
                    or (
                        bool(waiters)
                        and waiters[0] is ticket
                        and self._accepted_counts.get(key, 0) < self.max_queue_size
                    )
                )
                if self._closed:
                    raise RuntimeError("scheduler_closed")
                waiters.popleft()
                self._accepted_counts[key] = self._accepted_counts.get(key, 0) + 1
                admitted = True

                buffer = self._buffers.get(key)
                if buffer is None:
                    buffer = _ScopeBuffer(identity=identity, first_seen=time.monotonic())
                    self._buffers[key] = buffer
                buffer.events.append(event)
                should_flush = (
                    urgent
                    or len(buffer.events) >= self.batch_size
                    or self._accepted_counts[key] >= self.max_queue_size
                )
                if should_flush:
                    self._enqueue_buffer_locked(key)
                else:
                    self._start_timer(key)
            finally:
                if not admitted:
                    try:
                        waiters.remove(ticket)
                    except ValueError:
                        pass
                if not waiters:
                    self._capacity_waiters.pop(key, None)
                condition.notify_all()

        if should_flush:
            await self.flush_scope(identity)

    def _enqueue_buffer_locked(self, key: tuple[str, str]) -> None:
        buffer = self._buffers.pop(key, None)
        if buffer is None or not buffer.events:
            return
        queue = self._queues.setdefault(key, deque())
        for start in range(0, len(buffer.events), self.batch_size):
            queue.append(
                _QueuedBatch(
                    list(buffer.events[start:start + self.batch_size]),
                    buffer.attempts,
                )
            )
        timer = self._timers.pop(key, None)
        if timer is not None and timer is not asyncio.current_task():
            timer.cancel()

    def seal_pending(self, identity: UmoIdentity) -> None:
        """End the buffered batch at a control boundary without changing event IDs."""

        key = self._key(identity)
        self._enqueue_buffer_locked(key)
        if self._queues.get(key):
            self._start_timer(key)

    def _start_timer(self, key: tuple[str, str]) -> None:
        if self._closed:
            return
        current = self._timers.get(key)
        if current is not None and not current.done():
            return
        self._timers[key] = asyncio.create_task(self._flush_after(key))

    def pending_count(self, identity: UmoIdentity) -> int:
        """Count accepted buffered, queued, and inflight events; excludes waiters."""

        return self._accepted_counts.get(self._key(identity), 0)

    async def _send_to_failure_sink(
        self,
        identity: UmoIdentity,
        events: list[SourceEvent],
        attempts: int,
        error: BaseException,
    ) -> None:
        if self.failure_sink is None:
            raise RuntimeError("analysis_failure_sink_unavailable")
        result = self.failure_sink(
            identity, [event.message for event in events], attempts, error,
        )
        if inspect.isawaitable(result):
            await result

    async def _release_capacity(
        self, key: tuple[str, str], count: int,
    ) -> None:
        condition = self._capacity_condition(key)
        async with condition:
            current = self._accepted_counts.get(key, 0)
            if current < count:
                raise RuntimeError("scheduler_capacity_accounting_underflow")
            remaining = current - count
            if remaining:
                self._accepted_counts[key] = remaining
            else:
                self._accepted_counts.pop(key, None)
            condition.notify_all()

    async def _release_completed_capacity(
        self, key: tuple[str, str], count: int,
    ) -> None:
        """Finish accounting even if the caller is cancelled after durable work."""

        release = asyncio.create_task(self._release_capacity(key, count))
        try:
            await asyncio.shield(release)
        except asyncio.CancelledError:
            await release
            raise

    async def _requeue(
        self,
        identity: UmoIdentity,
        events: list[SourceEvent],
        *,
        attempts: int,
        failure_pending: bool = False,
        failure_sink_attempted: bool = False,
        failure_error: str = "",
    ) -> None:
        key = self._key(identity)
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            self._queues.setdefault(key, deque()).appendleft(
                _QueuedBatch(
                    list(events), attempts, failure_pending,
                    failure_sink_attempted, failure_error,
                ),
            )
            if not failure_pending:
                self._start_timer(key)

    async def _transfer_failed_batch(
        self, key: tuple[str, str], batch: _QueuedBatch, error: BaseException,
    ) -> None:
        identity = batch.events[0].identity
        if batch.failure_sink_attempted:
            await self._requeue(
                identity, batch.events, attempts=batch.attempts,
                failure_pending=True, failure_sink_attempted=True,
                failure_error=batch.failure_error or type(error).__name__,
            )
            raise RuntimeError("analysis_failure_sink_already_failed") from error
        if self.failure_sink is None:
            await self._requeue(
                identity, batch.events, attempts=batch.attempts,
                failure_pending=True, failure_error="analysis_failure_sink_unavailable",
            )
            raise RuntimeError("analysis_failure_sink_unavailable") from error

        # Mark before awaiting: even cancellation cannot cause close() to invoke the
        # same possibly-partially-committed sink operation a second time.
        try:
            await self._send_to_failure_sink(
                identity, batch.events, batch.attempts, error,
            )
        except asyncio.CancelledError:
            await self._requeue(
                identity, batch.events, attempts=batch.attempts,
                failure_pending=True, failure_sink_attempted=True,
                failure_error="failure_sink_cancelled",
            )
            raise
        except Exception as sink_error:
            await self._requeue(
                identity, batch.events, attempts=batch.attempts,
                failure_pending=True, failure_sink_attempted=True,
                failure_error=type(sink_error).__name__,
            )
            raise RuntimeError("analysis_failure_sink_failed") from sink_error
        await self._release_completed_capacity(key, len(batch.events))

    async def _flush_after(self, key: tuple[str, str]) -> None:
        try:
            await asyncio.sleep(self.flush_interval_seconds)
            buffer = self._buffers.get(key)
            queue = self._queues.get(key)
            identity = buffer.identity if buffer is not None else (
                queue[0].events[0].identity if queue else None
            )
            if identity is not None:
                await self.flush_scope(identity)
        except asyncio.CancelledError:
            return
        except Exception:
            return

    async def flush_scope(self, identity: UmoIdentity) -> None:
        key = self._key(identity)
        lock = self._locks.setdefault(key, asyncio.Lock())
        analysis_lock = self._analysis_locks.setdefault(key, asyncio.Lock())
        current_task = asyncio.current_task()
        if current_task is not None:
            self._active_flush_tasks.add(current_task)
            self._active_flush_scopes[current_task] = key
        try:
            async with analysis_lock:
                while True:
                    queued: _QueuedBatch | None = None
                    async with lock:
                        queue = self._queues.get(key)
                        queued = queue.popleft() if queue else None
                        if queue is not None and not queue:
                            self._queues.pop(key, None)
                        if queued is None:
                            buffer = self._buffers.pop(key, None)
                            if buffer is None or not buffer.events:
                                return
                            events = list(buffer.events)
                            attempts = buffer.attempts
                        else:
                            events = queued.events
                            attempts = queued.attempts
                        timer = self._timers.pop(key, None)
                        if timer is not None and timer is not current_task:
                            timer.cancel()

                        if queued is not None and queued.failure_pending:
                            continue_with_failure = True
                        else:
                            continue_with_failure = False
                            self._inflight[key] = (identity, events, attempts)

                    if continue_with_failure:
                        await self._transfer_failed_batch(
                            key, queued, RuntimeError(
                                queued.failure_error or "analysis_failure_pending"
                            ),
                        )
                        continue

                    try:
                        batch_id = MemoryEventLedger.batch_id(events)
                        async with self._semaphore:
                            await self.analyzer.analyze(
                                identity,
                                [event.message for event in events],
                                online_event_id=batch_id,
                                source_events=events,
                            )
                    except asyncio.CancelledError:
                        await self._requeue(identity, events, attempts=attempts)
                        raise
                    except Exception as exc:
                        next_attempt = attempts + 1
                        if next_attempt >= self.max_retries:
                            terminal = _QueuedBatch(
                                list(events), next_attempt, failure_pending=True,
                            )
                            await self._transfer_failed_batch(key, terminal, exc)
                        else:
                            await self._requeue(
                                identity, events, attempts=next_attempt,
                            )
                        raise
                    else:
                        # Cancellation after the Analyzer has returned must not
                        # requeue an already processed batch.
                        await self._release_completed_capacity(key, len(events))
                    finally:
                        self._inflight.pop(key, None)
        finally:
            if current_task is not None:
                self._active_flush_tasks.discard(current_task)
                self._active_flush_scopes.pop(current_task, None)

    async def flush_due(self, *, now: float | None = None) -> None:
        current = time.monotonic() if now is None else float(now)
        due = [
            buffer.identity
            for buffer in list(self._buffers.values())
            if current - buffer.first_seen >= self.flush_interval_seconds
        ]
        for identity in due:
            await self.flush_scope(identity)

    async def close(self) -> None:
        if self._closed:
            if self._close_error is not None:
                raise RuntimeError("scheduler_close_failure_handoff_incomplete") from self._close_error
            return
        self._closed = True

        # Reject and wake producers before waiting for any Analyzer or sink work.
        for condition in list(self._capacity_conditions.values()):
            async with condition:
                condition.notify_all()

        current_task = asyncio.current_task()
        timers = list(self._timers.values())
        for timer in timers:
            if timer is not current_task:
                timer.cancel()
        self._timers.clear()
        pending_timers = [timer for timer in timers if timer is not current_task]

        for key in list(self._buffers):
            self._enqueue_buffer_locked(key)
        identities = {
            batch.events[0].identity
            for queue in self._queues.values()
            for batch in queue
            if batch.events
        }
        flushes = [asyncio.create_task(self.flush_scope(identity)) for identity in identities]
        active = [
            task for task in list(self._active_flush_tasks)
            if task is not current_task and not task.done()
        ]
        all_tasks = list(dict.fromkeys([*flushes, *active, *pending_timers]))
        unresolved: set[asyncio.Task] = set()
        if all_tasks:
            _, unresolved = await asyncio.wait(
                all_tasks, timeout=self.close_timeout_seconds,
            )
        if unresolved:
            for task in unresolved:
                task.cancel()
            # Give normal asyncio cancellation handlers one turn to requeue their
            # accepted batch; never wait without a bound for a cancellation-hostile
            # Provider implementation.
            await asyncio.sleep(0)
            unresolved = {task for task in unresolved if not task.done()}

        completed_tasks = [task for task in all_tasks if task.done()]
        if completed_tasks:
            await asyncio.gather(*completed_tasks, return_exceptions=True)

        unresolved_keys = {
            self._active_flush_scopes[task]
            for task in unresolved
            if task in self._active_flush_scopes
        }

        handoff_errors: list[BaseException] = []
        for key in unresolved_keys:
            handoff_errors.append(RuntimeError("scheduler_flush_did_not_stop_before_close"))
        for key, queue in list(self._queues.items()):
            if key in unresolved_keys:
                continue
            for batch in list(queue):
                if not batch.events:
                    queue.remove(batch)
                    continue
                if batch.failure_sink_attempted:
                    handoff_errors.append(RuntimeError(
                        "analysis_failure_sink_previously_failed: "
                        f"{batch.failure_error or 'unknown'}"
                    ))
                    continue
                if self.failure_sink is None:
                    handoff_errors.append(RuntimeError("analysis_failure_sink_unavailable"))
                    continue
                batch.failure_pending = True
                batch.failure_sink_attempted = True
                try:
                    await self._send_to_failure_sink(
                        batch.events[0].identity,
                        batch.events,
                        batch.attempts,
                        RuntimeError("调度器关闭前未能完成语义分析"),
                    )
                except BaseException as exc:
                    batch.failure_error = type(exc).__name__
                    handoff_errors.append(exc)
                    continue
                queue.remove(batch)
                await self._release_completed_capacity(key, len(batch.events))
            if not queue:
                self._queues.pop(key, None)

        if handoff_errors:
            self._close_error = handoff_errors[0]
            raise RuntimeError("scheduler_close_failure_handoff_incomplete") from handoff_errors[0]


__all__ = ["AnalysisScheduler"]
