#!/usr/bin/env python3
"""
RepoBrain demo helper: query SDRs/patterns by file path or keyword.

Examples:
  python3 repobrain/context.py spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java
  python3 repobrain/context.py retrytopic
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
SDR_PATH = ROOT / "repobrain" / "sdr" / "spring-kafka-sdrs-and-patterns.txt"


@dataclass(frozen=True)
class SDREntry:
    pr_number: str
    attribution: str
    decision: str
    justification: str
    scope: str
    example_paths: list[str]
    pattern: str
    raw: str


def parse_entries(text: str) -> list[SDREntry]:
    blocks: list[str] = []
    current: list[str] = []
    for line in text.splitlines():
        if line.startswith("SDR: PR #"):
            if current:
                blocks.append("\n".join(current).strip())
            current = [line]
        elif current:
            current.append(line)
    if current:
        blocks.append("\n".join(current).strip())

    entries: list[SDREntry] = []
    for b in blocks:
        pr = re.search(r"^SDR: PR #(\d+)\s*$", b, re.MULTILINE)
        attribution = re.search(r"^Attribution:\s*(.+)\s*$", b, re.MULTILINE)
        decision = re.search(r"^Decision:\s*(.+)\s*$", b, re.MULTILINE)
        justification = re.search(r"^Justification:\s*(.+)\s*$", b, re.MULTILINE)
        scope = re.search(r"^Scope:\s*(.+)\s*$", b, re.MULTILINE)
        pattern = re.search(r"^Pattern:\s*(.+)\s*$", b, re.MULTILINE)
        example_paths = re.findall(r"^- (.+)$", b, re.MULTILINE)

        entries.append(
            SDREntry(
                pr_number=(pr.group(1) if pr else "?"),
                attribution=(attribution.group(1) if attribution else ""),
                decision=(decision.group(1) if decision else ""),
                justification=(justification.group(1) if justification else ""),
                scope=(scope.group(1) if scope else ""),
                example_paths=example_paths,
                pattern=(pattern.group(1) if pattern else ""),
                raw=b.strip(),
            )
        )
    return entries


def score(entry: SDREntry, query: str) -> int:
    q = query.lower()
    score = 0
    hay = " ".join(
        [
            entry.decision,
            entry.justification,
            entry.scope,
            " ".join(entry.example_paths),
        ]
    ).lower()
    if q in hay:
        score += 10
    # token overlap
    tokens = [t for t in re.split(r"[^a-z0-9_./-]+", q) if t]
    for t in tokens:
        if len(t) < 3:
            continue
        if t in hay:
            score += 2
    # scope match bias
    for p in entry.example_paths:
        if p.lower().startswith(q) or q in p.lower():
            score += 4
    return score


def top_matches(entries: Iterable[SDREntry], query: str, *, limit: int = 6) -> list[tuple[int, SDREntry]]:
    scored = [(score(e, query), e) for e in entries]
    scored = [s for s in scored if s[0] > 0]
    scored.sort(key=lambda x: (-x[0], x[1].pr_number))
    return scored[:limit]


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} <path-or-keyword>", file=sys.stderr)
        return 2

    query = argv[1].strip()
    if not SDR_PATH.exists():
        print(f"missing SDR file: {SDR_PATH}", file=sys.stderr)
        return 2

    entries = parse_entries(SDR_PATH.read_text(encoding="utf-8", errors="replace"))
    matches = top_matches(entries, query)
    if not matches:
        print("no matches")
        return 0

    print(f"query: {query}")
    print(f"matches: {len(matches)}")
    print("")
    for s, e in matches:
        print(f"[score={s}] PR #{e.pr_number} {e.attribution}")
        if e.decision:
            print(f"Decision: {e.decision}")
        if e.scope:
            print(f"Scope: {e.scope}")
        if e.pattern:
            print(f"Pattern: {e.pattern}")
        if e.example_paths:
            print(f"Examples: {', '.join(e.example_paths[:3])}")
        print("")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

