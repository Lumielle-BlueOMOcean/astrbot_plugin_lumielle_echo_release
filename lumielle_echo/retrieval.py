"""严格隔离后的语义候选召回与综合排序。"""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone

from .embeddings import EmbeddingAdapter
from .identity import UmoIdentity
from .store import MemoryStore, StoredMemory
from .taxonomy import resolve_policy


def _normalize(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "").lower())


def _cosine(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    if not left or len(left) != len(right):
        return 0.0
    numerator = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(item * item for item in left))
    right_norm = math.sqrt(sum(item * item for item in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return max(0.0, numerator / (left_norm * right_norm))


def _local_similarity(memory: StoredMemory, query: str) -> float:
    needle = _normalize(query)
    if not needle:
        return 0.25
    fields = [memory.content, memory.subject_key, *memory.aliases]
    normalized = [_normalize(item) for item in fields if item]
    if any(needle == item for item in normalized):
        return 1.0
    if any(needle in item or item in needle for item in normalized if item):
        return 0.82
    return 0.0


def _pipeline_bonus(type_path: str) -> float:
    if type_path.startswith("anchor."):
        return 4.0
    if type_path.startswith("cognition."):
        return 2.5
    if type_path.startswith("state."):
        return 2.0
    if type_path.startswith("open_loop."):
        return 1.5
    return 0.0


def _freshness_penalty(memory: StoredMemory) -> float:
    try:
        policy = resolve_policy(memory.type_path)
        if not policy.decays:
            return 0.0
        updated = datetime.fromisoformat(memory.updated_at or memory.created_at)
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age_days = max(
            0.0,
            (datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds()
            / 86400,
        )
        return min(3.0, math.log1p(age_days) * 0.35)
    except (TypeError, ValueError):
        return 0.0


class MemoryRetriever:
    def __init__(
        self,
        store: MemoryStore,
        *,
        embedding_provider=None,
        candidate_limit: int = 100,
    ) -> None:
        self.store = store
        self.embeddings = EmbeddingAdapter(embedding_provider)
        self.candidate_limit = max(10, int(candidate_limit))

    async def search(
        self, identity: UmoIdentity, query: str, *, limit: int = 24,
        read_only: bool = False,
    ) -> list[StoredMemory]:
        candidates = self.store.list_active(
            identity, limit=self.candidate_limit, mark_expired=not read_only,
        )
        if str(query or "").strip():
            lexical = self.store.search(
                identity, query, limit=self.candidate_limit,
                mark_expired=not read_only,
            )
            by_id = {item.id: item for item in candidates}
            by_id.update({item.id: item for item in lexical})
            candidates = list(by_id.values())
        query_vector = await self.embeddings.embed(query)
        scored: list[tuple[float, StoredMemory]] = []
        for memory in candidates:
            local = _local_similarity(memory, query)
            vector = _cosine(query_vector, memory.embedding) if query_vector else 0.0
            semantic = max(local, vector)
            if str(query or "").strip() and semantic <= 0:
                continue
            score = (
                semantic * 6.0
                + memory.importance * 0.7
                + math.log2(max(1, memory.mention_count))
                + _pipeline_bonus(memory.type_path)
                - _freshness_penalty(memory)
            )
            scored.append((score, memory))
        scored.sort(key=lambda item: (item[0], item[1].updated_at, item[1].id), reverse=True)
        size = max(0, int(limit))
        if size == 0:
            return []
        ordered = [memory for _, memory in scored]
        selected: list[StoredMemory] = []
        selected_ids: set[int] = set()
        for prefix in ("anchor.", "state.", "open_loop.", "cognition."):
            if len(selected) >= size:
                break
            memory = next(
                (item for item in ordered if item.type_path.startswith(prefix)), None
            )
            if memory is not None:
                selected.append(memory)
                selected_ids.add(memory.id)
        for memory in ordered:
            if len(selected) >= size:
                break
            if memory.id not in selected_ids:
                selected.append(memory)
                selected_ids.add(memory.id)
        return selected
