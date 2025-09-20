
# =============================================
# Script 2: apply_photo_folders_from_csv.py
# ---------------------------------------------
# Read the CSV (possibly edited by you). For each row, use
# `final_folder` if present, else `proposed_folder`. Create
# the folders under --dest and move/copy the images there.
# Avoids overwriting by adding _1, _2, ... suffixes.
#
# Usage (dry run first):
#   python3 apply_photo_folders_from_csv.py \
#     --csv /path/to/catalog.csv \
#     --dest /path/to/organized \
#     --dry-run
#
# Then execute for real (move):
#   python3 apply_photo_folders_from_csv.py \
#     --csv /path/to/catalog.csv \
#     --dest /path/to/organized
#
# Copy instead of move:
#   python3 apply_photo_folders_from_csv.py --csv ... --dest ... --copy
# =============================================

import argparse as _argparse, csv as _csv, os as _os, shutil as _shutil


def apply_from_csv(csv_path, dest_root, do_copy=False, dry_run=False):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = _csv.DictReader(f)
        required = {"source_path", "proposed_folder"}
        if not required.issubset(reader.fieldnames):
            missing = required - set(reader.fieldnames or [])
            raise SystemExit(f"CSV missing required columns: {missing}")

        ops = []
        for row in reader:
            src = row.get("source_path", "")
            if not src:
                continue
            folder = (row.get("final_folder") or row.get("proposed_folder") or "Unassigned").strip()
            # sanitize folder name
            safe_folder = folder.replace("\\", "_").replace("/", "_")
            target_dir = _os.path.join(dest_root, safe_folder)
            _os.makedirs(target_dir, exist_ok=True)
            dst = _os.path.join(target_dir, _os.path.basename(src))
            ops.append((src, dst))

    print(f"Planned operations: {len(ops)} files -> {_os.path.abspath(dest_root)}")
    for src, dst in ops[:20]:
        print(f" - {src} -> {dst}")
    if len(ops) > 20:
        print(f" ... and {len(ops)-20} more")

    if dry_run:
        print("\nDry run only. No files moved/copied.")
        return

    for src, dst in ops:
        base, ext = _os.path.splitext(_os.path.basename(dst))
        out_dir = _os.path.dirname(dst)
        candidate = _os.path.join(out_dir, base + ext)
        i = 1
        while _os.path.exists(candidate):
            candidate = _os.path.join(out_dir, f"{base}_{i}{ext}")
            i += 1
        if do_copy:
            _shutil.copy2(src, candidate)
        else:
            _shutil.move(src, candidate)

    print("\nDone.")


if __name__ == "__main__":
    ap = _argparse.ArgumentParser(description="Apply folders to photos based on a CSV catalog.")
    ap.add_argument("--csv", help="Path to CSV file", required=True)
    ap.add_argument("--dest", help="Path to destination folder", required=True)
    ap.add_argument("--copy", action="store_true", help="Copy instead of move")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    apply_from_csv(args.csv, args.dest, do_copy=args.copy, dry_run=args.dry_run)
