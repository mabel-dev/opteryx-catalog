"""Inference over the catalog - proposing metadata for a person to judge.

Nothing here asserts anything. Every proposal carries the evidence it was made
on, is written with `origin='inferred'` and `status='unverified'`, and is inert
until an owner confirms it.

The same run also RE-SCORES what people have already declared (design §9). It is
the identical operation over the identical sketches - so it rides this job
rather than a second one - and it can mark a declared relationship `broken`,
which is the one thing here that changes what a BI client is served. It still
asserts nothing: a break records that the values stopped corresponding, and the
row is kept rather than deleted.
"""

from .relationships import ColumnProfile
from .relationships import RelationshipCandidate
from .relationships import VerificationResult
from .relationships import build_column_profiles
from .relationships import propose_for_workspace
from .relationships import run_for_workspace
from .relationships import score_candidates
from .relationships import verify_declared

__all__ = [
    "ColumnProfile",
    "RelationshipCandidate",
    "VerificationResult",
    "build_column_profiles",
    "propose_for_workspace",
    "run_for_workspace",
    "score_candidates",
    "verify_declared",
]
