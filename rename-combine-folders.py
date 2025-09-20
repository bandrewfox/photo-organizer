#!/usr/bin/env python3
"""
Rename folders that start with a given substring and replace underscores with dashes.

Usage examples:
  python rename-combine-folders.py --input camera --startswith around-town --dry-run
  python rename-combine-folders.py -i camera -s around-town --yes

Options:
  --input / -i       Path to the folder containing target subfolders (required)
  --startswith / -s  The substring that target folder names start with (required)
  --recursive / -r   Recursively search for matching directories
  --dry-run / -n     Show what would be done but do not rename (default: False)
  --yes / -y         Do not prompt for confirmation when performing actions
  --ignore-case      Match start substring case-insensitively
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
import shutil
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


def compute_new_name(old_name: str) -> str:
    return old_name.replace("_", "-")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Rename folders that start with a substring and replace underscores with dashes.")
    parser.add_argument("--input", "-i", required=True, help="Input folder containing subfolders to rename")
    parser.add_argument("--startswith", "-s", required=True, help="Substring that target folder names start with")
    parser.add_argument("--recursive", "-r", action="store_true", help="Recursively search for matching directories")
    parser.add_argument("--dry-run", "-n", action="store_true", help="Show changes but do not perform renames")
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

    actions = []
    for p in targets:
        new_name = compute_new_name(p.name)
        if new_name == p.name:
            # nothing to change
            continue
        new_path = p.with_name(new_name)
        if new_path.exists():
            # destination already exists: will merge contents into destination
            actions.append((p, new_path, "MERGE"))
        else:
            actions.append((p, new_path, "RENAME"))

    if not actions:
        print("No directories needed renaming (no underscores found in matching folder names).")
        return 0

    # Print summary
    print("Planned actions:")
    for src, dst, status in actions:
        if status == "RENAME":
            print(f"  RENAME: {src} -> {dst}")
        else:
            print(f"  MERGE (target exists): {src} -> {dst}")

    if args.dry_run:
        print("\nDry-run mode enabled; no changes made.")
        return 0

    if not args.yes:
        resp = input("Proceed with renaming? [y/N]: ").strip().lower()
        if resp not in ("y", "yes"):
            print("Cancelled by user.")
            return 0

    # Perform renames / merges
    failures = []
    for src, dst, status in actions:
        if status == "MERGE":
            print(f"Merging contents of {src} into {dst}")
            try:
                for item in src.iterdir():
                    dest_item = dst / item.name
                    if dest_item.exists():
                        # avoid overwriting existing files/folders; skip and warn
                        print(f"  Skipping {item} because {dest_item} exists")
                        continue
                    # move file or directory into destination
                    shutil.move(str(item), str(dest_item))
                    print(f"  Moved {item} -> {dest_item}")
                # attempt to remove source directory if empty
                try:
                    src.rmdir()
                    print(f"Removed empty source directory {src}")
                except OSError:
                    print(f"Could not remove {src}: not empty; left in place")
            except Exception as exc:
                print(f"Failed to merge {src} -> {dst}: {exc}")
                failures.append((src, dst, exc))
            continue

        if status == "RENAME":
            try:
                src.rename(dst)
                print(f"Renamed: {src} -> {dst}")
            except Exception as exc:
                print(f"Failed to rename {src} -> {dst}: {exc}")
                failures.append((src, dst, exc))

    if failures:
        print(f"\nCompleted with {len(failures)} failures.")
        return 1

    print("\nAll done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
