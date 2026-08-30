"""Inference over the catalog - proposing metadata for a person to judge.

Nothing here asserts anything. Every output is a proposal carrying the evidence
it was made on, written with `origin='inferred'` and `status='unverified'`, and
inert until an owner confirms it.
"""

from .relationships import ColumnProfile
from .relationships import RelationshipCandidate
from .relationships import build_column_profiles
from .relationships import propose_for_workspace
from .relationships import score_candidates

__all__ = [
    "ColumnProfile",
    "RelationshipCandidate",
    "build_column_profiles",
    "propose_for_workspace",
    "score_candidates",
]
