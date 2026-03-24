from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass

from datasketch import MinHash

from spec2cov.config import DedupConfig

TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(slots=True)
class DedupResult:
    sha256: str
    minhash_b64: str
    token_count: int
    near_duplicate: bool
    similarity: float
    cluster_id: str


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def tokenize(text: str) -> list[str]:
    return TOKEN_RE.findall(text.lower())


def shingles(tokens: list[str], shingle_size: int) -> set[str]:
    if len(tokens) < shingle_size:
        return {" ".join(tokens)} if tokens else set()
    return {" ".join(tokens[index : index + shingle_size]) for index in range(len(tokens) - shingle_size + 1)}


def build_minhash(text: str, config: DedupConfig) -> tuple[MinHash, list[str]]:
    tokens = tokenize(text)
    shingle_values = sorted(shingles(tokens, config.shingle_size))
    minhash = MinHash(num_perm=config.minhash_perm)
    for shingle in shingle_values:
        minhash.update(shingle.encode("utf-8"))
    return minhash, tokens


def encode_minhash(minhash: MinHash) -> str:
    return base64.b64encode(minhash.digest().tobytes()).decode("ascii")


def compare_against_existing(text: str, existing: list[dict[str, str | float | int | None]], config: DedupConfig) -> DedupResult:
    current_sha = sha256_text(text)
    current_minhash, tokens = build_minhash(text, config)
    best_similarity = 0.0
    best_cluster_id = current_sha[:12]
    near_duplicate = False

    for row in existing:
        existing_sha = row.get("content_sha256")
        if existing_sha == current_sha:
            near_duplicate = True
            best_similarity = 1.0
            best_cluster_id = str(row.get("cluster_id") or current_sha[:12])
            break

        prior_text = row.get("text")
        if not prior_text:
            continue
        prior_minhash, _ = build_minhash(str(prior_text), config)
        similarity = current_minhash.jaccard(prior_minhash)
        if similarity > best_similarity:
            best_similarity = similarity
            best_cluster_id = str(row.get("cluster_id") or str(existing_sha)[:12])
        if similarity >= config.jaccard_threshold:
            near_duplicate = True
            break

    return DedupResult(
        sha256=current_sha,
        minhash_b64=encode_minhash(current_minhash),
        token_count=len(tokens),
        near_duplicate=near_duplicate,
        similarity=best_similarity,
        cluster_id=best_cluster_id,
    )
