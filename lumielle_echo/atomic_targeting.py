"""Atomic 生命周期目标上下文。

这里故意只做本地、只读的候选构建；它不调用 Embedding/LLM，也不执行任何写入。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from .atomic_models import AtomicMemory, AtomStatus, is_retrieval_eligible
from .atomic_store import AtomicRepository
from .identity import UmoIdentity


class AtomicTargetError(ValueError):
    """Atomic target context 或 allowlist 无法安全建立。"""

    def __init__(self, code: str):
        self.code = str(code)
        super().__init__(self.code)


@dataclass(frozen=True, slots=True)
class AtomicTargetContextItem:
    atom_id: int
    type_path: str
    subject_key: str
    content: str
    status: str
    importance: float
    confidence: float
    updated_at: str

    @classmethod
    def from_atom(cls, atom: AtomicMemory) -> "AtomicTargetContextItem":
        return cls(
            atom_id=int(atom.id), type_path=atom.type_path,
            subject_key=atom.subject_key, content=atom.content,
            status=getattr(atom.status, "value", str(atom.status)),
            importance=float(atom.importance), confidence=float(atom.confidence),
            updated_at=atom.updated_at,
        )

    def as_prompt(self) -> dict[str, object]:
        return {
            "atom_id": self.atom_id,
            "type_path": self.type_path,
            "subject_key": self.subject_key,
            "content": self.content,
            "status": self.status,
            "importance": self.importance,
            "confidence": self.confidence,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True, slots=True)
class AtomicTargetContext:
    items: tuple[AtomicTargetContextItem, ...]

    @property
    def allowlist(self) -> tuple[int, ...]:
        return tuple(item.atom_id for item in self.items)

    @property
    def atom_map(self) -> dict[int, AtomicTargetContextItem]:
        return {item.atom_id: item for item in self.items}


def _tokens(value: object) -> set[str]:
    # Reuse the production Atomic lexical tokenizer so target recall and
    # official retrieval share the same CJK/Latin token contract.
    return set(AtomicRepository._lexical_tokens(str(value or "")))


class AtomicTargetContextBuilder:
    """Build an identity-scoped target allowlist without external providers."""

    def __init__(self, repository: AtomicRepository) -> None:
        self.repository = repository

    def build(
        self,
        identity: UmoIdentity,
        messages: Iterable[str],
        *,
        limit: int = 40,
    ) -> AtomicTargetContext:
        max_items = max(0, int(limit))
        if max_items == 0:
            return AtomicTargetContext(())
        query = " ".join(str(item) for item in messages)
        query_tokens = _tokens(query)
        now_dt = datetime.now(timezone.utc)
        atoms = [
            atom for atom in self.repository.list_lifecycle_candidates(
                identity, statuses=(AtomStatus.ACTIVE, AtomStatus.DORMANT),
            )
            if atom.status is AtomStatus.DORMANT
            or is_retrieval_eligible(atom.status, atom.type_path, atom.expires_at, now_dt)
        ]
        by_id = {atom.id: atom for atom in atoms}

        # Active lexical candidates use the same FTS/BM25 + deterministic CJK
        # fallback as official Atomic retrieval.  The repository method is
        # read-only with respect to the database; it only updates diagnostics.
        lexical_rows = self.repository.search_lexical(
            identity, query, limit=None, now=now_dt.isoformat(),
        )
        active_lexical = [row.atom for row in lexical_rows if row.atom.id in by_id]

        # Dormant atoms are intentionally absent from search_lexical.  Score
        # them with the repository's tokenizer so lifecycle operations such as
        # forget can still recall a relevant dormant target without Embedding.
        dormant_lexical: list[tuple[tuple[int, int, float, str, int], AtomicMemory]] = []
        for atom in atoms:
            if atom.status is not AtomStatus.DORMANT:
                continue
            terms = _tokens(" ".join((atom.type_path, atom.subject_key, atom.content)))
            overlap = len(query_tokens & terms) if query_tokens else 0
            if overlap:
                frequency = sum(1 for token in query_tokens if token in terms)
                dormant_lexical.append(
                    ((overlap, frequency, float(atom.importance), atom.updated_at, int(atom.id)), atom)
                )
        dormant_lexical.sort(key=lambda item: item[0], reverse=True)

        # Rank lexical candidates by shared tokenizer overlap first. This keeps
        # a strongly matching dormant target ahead of active atoms that only
        # share a common one-character token, while active hits still come
        # from the official repository search above.
        lexical_candidates = []
        for atom in active_lexical:
            terms = _tokens(" ".join((atom.type_path, atom.subject_key, atom.content)))
            overlap = len(query_tokens & terms) if query_tokens else 0
            frequency = sum(1 for token in query_tokens if token in terms)
            lexical_candidates.append(
                ((overlap, frequency, 1, float(atom.importance), atom.updated_at, int(atom.id)), atom)
            )
        lexical_candidates.extend(
            ((score[0], score[1], 0, score[2], score[3], score[4]), atom)
            for score, atom in dormant_lexical
        )
        lexical_candidates.sort(key=lambda item: item[0], reverse=True)
        lexical_ids = {atom.id for _, atom in lexical_candidates}
        ordered: list[AtomicMemory] = [atom for _, atom in lexical_candidates]

        fallback = sorted(
            (
                atom for atom in atoms
                if atom.id not in lexical_ids
            ),
            key=lambda atom: (
                1 if atom.status is AtomStatus.ACTIVE else 0,
                float(atom.importance),
                atom.updated_at,
                int(atom.id),
            ),
            reverse=True,
        )
        ordered.extend(fallback)

        return AtomicTargetContext(tuple(
            AtomicTargetContextItem.from_atom(atom) for atom in ordered[:max_items]
        ))


__all__ = [
    "AtomicTargetContext",
    "AtomicTargetContextBuilder",
    "AtomicTargetContextItem",
    "AtomicTargetError",
]
