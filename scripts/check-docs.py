#!/usr/bin/env python3
"""Check that the documentation only points at things that exist.

Two classes of drift are caught:

  1. Relative Markdown links (and #anchors) that resolve to a missing file or
     heading -- e.g. a README row still linking a deleted document.
  2. `make <target>` commands in Markdown code that name a target the Makefile
     does not define -- e.g. a walkthrough telling readers to run `make dev`
     after the target was renamed.

Only code spans and fenced code blocks are scanned for make commands, so prose
such as "make the document searchable" is not mistaken for a command.

Usage: python3 scripts/check-docs.py   (from anywhere; exits 1 on any problem)
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parent.parent

LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)|!\[[^\]]*\]\(([^)\s]+)\)")
FENCE = re.compile(r"^\s*(```|~~~)")
CODE_SPAN = re.compile(r"`([^`]+)`")
MAKE_CMD = re.compile(r"(?:^|[\s;&|(])make\s+([^#;&|)\n]*)")
TARGET_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
MAKE_RULE = re.compile(r"^([A-Za-z0-9_.][A-Za-z0-9_. -]*?)\s*:(?!=)")


def markdown_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "*.md"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout
    return [ROOT / line for line in out.splitlines() if (ROOT / line).exists()]


def make_targets() -> set[str]:
    targets: set[str] = set()
    for line in (ROOT / "Makefile").read_text().splitlines():
        if line.startswith(("\t", "#", ".")):
            continue
        match = MAKE_RULE.match(line)
        if match:
            targets.update(match.group(1).split())
    return targets


def slugify(heading: str) -> str:
    """GitHub's heading-anchor algorithm, close enough for this repository."""
    text = re.sub(r"<[^>]+>", "", heading)
    text = re.sub(r"[`*_~]|\[([^\]]*)\]\([^)]*\)", lambda m: m.group(1) or "", text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


_anchor_cache: dict[Path, set[str]] = {}


def anchors(path: Path) -> set[str]:
    if path not in _anchor_cache:
        seen: dict[str, int] = {}
        found: set[str] = set()
        in_fence = False
        for line in path.read_text(encoding="utf-8").splitlines():
            if FENCE.match(line):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            heading = re.match(r"^#{1,6}\s+(.*?)\s*#*\s*$", line)
            if heading:
                slug = slugify(heading.group(1))
                count = seen.get(slug, 0)
                found.add(slug if count == 0 else f"{slug}-{count}")
                seen[slug] = count + 1
            for explicit in re.findall(r'<a\s+(?:name|id)="([^"]+)"', line):
                found.add(explicit)
        _anchor_cache[path] = found
    return _anchor_cache[path]


def check_link(source: Path, target: str) -> str | None:
    if re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I):  # http:, mailto:, ...
        return None
    path_part, _, fragment = target.partition("#")
    path_part = unquote(path_part)
    if path_part.startswith("/"):
        resolved = ROOT / path_part.lstrip("/")
    elif path_part:
        resolved = (source.parent / path_part).resolve()
    else:
        resolved = source
    if not resolved.exists():
        return f"broken link: {target}"
    if fragment and resolved.suffix == ".md" and resolved.is_file():
        if fragment.lower() not in anchors(resolved):
            return f"missing anchor: {target}"
    return None


def make_commands(text: str) -> list[str]:
    targets: list[str] = []
    for match in MAKE_CMD.finditer(text):
        for token in match.group(1).split():
            if "=" in token or token.startswith("-"):
                continue  # VAR=value overrides and make flags
            if not TARGET_TOKEN.match(token):
                break
            targets.append(token)
    return targets


def main() -> int:
    known = make_targets()
    problems: list[str] = []
    for md in markdown_files():
        rel = md.relative_to(ROOT)
        in_fence = False
        for lineno, line in enumerate(md.read_text(encoding="utf-8").splitlines(), 1):
            if FENCE.match(line):
                in_fence = not in_fence
                continue
            code = [line] if in_fence else CODE_SPAN.findall(line)
            for snippet in code:
                for target in make_commands(snippet):
                    if target not in known:
                        problems.append(f"{rel}:{lineno}: unknown make target: make {target}")
            if in_fence:
                continue
            prose = CODE_SPAN.sub("", line)
            for match in LINK.finditer(prose):
                problem = check_link(md, match.group(1) or match.group(2))
                if problem:
                    problems.append(f"{rel}:{lineno}: {problem}")

    for problem in problems:
        print(problem)
    if problems:
        print(f"\n{len(problems)} documentation problem(s).", file=sys.stderr)
        return 1
    print(f"docs ok: {len(markdown_files())} Markdown files, {len(known)} make targets")
    return 0


if __name__ == "__main__":
    sys.exit(main())
