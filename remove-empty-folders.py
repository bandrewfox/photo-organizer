#!/usr/bin/env python3
"""
Delete empty folders that start with a given substring.

Usage examples:
  python remove-empty-folders.py --input camera --startswith around-town --dry-run
  python remove-empty-folders.py -i camera -s around-town --yes

Options:
  --input / -i       Path to the folder containing target subfolders (required)
  --startswith / -s  The substring that target folder names start with (required)
  --recursive / -r   Recursively search for matching directories
  --dry-run / -n     Show what would be done but do not delete (default: False)
  --yes / -y         Do not prompt for confirmation when performing actions
  --ignore-case      Match start substring case-insensitively
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable


def find_target_dirs(root: Path, startswith: str, recursive: bool, ignore_case: bool) -> Iterable[Path]:
    if recursive:
        iterator = (p for p in root.rglob("*") if p.is_dir())
    else:
        iterator = (p for p in root.iterdir() if p.is_dir())

    if ignore_case:
        startswith_lower = startswith.lower()
        for p in iterator:
            if p.name.lower().startswith(startswith_lower):
                yield p
    else:
        for p in iterator:
            if p.name.startswith(startswith):
                yield p


def main(argv=None):
    parser = argparse.ArgumentParser(description="Delete empty folders that start with a substring.")
    parser.add_argument("--input", "-i", required=True, help="Input folder containing subfolders to check")
    parser.add_argument("--startswith", "-s", required=True, help="Substring that target folder names start with")
    parser.add_argument("--recursive", "-r", action="store_true", help="Recursively search for matching directories")
    parser.add_argument("--dry-run", "-n", action="store_true", help="Show changes but do not perform deletions")
    parser.add_argument("--yes", "-y", action="store_true", help="Do not prompt for confirmation")
    parser.add_argument("--ignore-case", action="store_true", help="Match the startswith string case-insensitively")

    args = parser.parse_args(argv)

    root = Path(args.input)
    if not root.exists() or not root.is_dir():
        print(f"Error: input path '{root}' does not exist or is not a directory.")
        return 2

    targets = list(find_target_dirs(root, args.startswith, args.recursive, args.ignore_case))
    if not targets:
        print("No matching directories found.")
        return 0

    # sort by depth so deeper directories are attempted first
    targets_sorted = sorted(targets, key=lambda p: len(p.parts), reverse=True)

    print("Planned targets (will attempt to delete if empty):")
    for p in targets_sorted:
        print(f"  {p}")

    if args.dry_run:
        print("\nDry-run mode enabled; no deletions will be performed. Checking which targets are empty...")
        will_delete = []
        will_skip = []
        errors = []
        for p in targets_sorted:
            try:
                # determine if directory is empty without modifying it
                try:
                    next(p.iterdir())
                    empty = False
                except StopIteration:
                    empty = True

                if empty:
                    print(f"  [WILL DELETE] {p}")
                    will_delete.append(p)
                else:
                    print(f"  [NOT EMPTY]  {p}")
                    will_skip.append(p)
            except Exception as exc:
                print(f"  [ERROR]      {p}: {exc}")
                errors.append((p, exc))

        print()
        print(f"Dry-run summary: {len(will_delete)} would be deleted, {len(will_skip)} skipped (not empty), {len(errors)} errors.")
        return 0

    if not args.yes:
        resp = input("Proceed with deleting empty folders? [y/N]: ").strip().lower()
        if resp not in ("y", "yes"):
            print("Cancelled by user.")
            return 0

    failures = []
    deleted_count = 0
    for p in targets_sorted:
        try:
            # attempt to remove directory; rmdir only succeeds if directory is empty
            p.rmdir()
            print(f"Deleted: {p}")
            deleted_count += 1
        except OSError:
            # not empty or other OS-level error; leave in place
            print(f"Not empty (skipped): {p}")
        except Exception as exc:
            print(f"Failed to delete {p}: {exc}")
            failures.append((p, exc))

    print(f"\nCompleted. Deleted {deleted_count} directories.")
    if failures:
        print(f"Encountered {len(failures)} failures.")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
