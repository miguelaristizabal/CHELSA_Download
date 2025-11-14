#!/usr/bin/env python3
import os, re, sys
from collections import defaultdict, Counter

# Default lists dir (override by passing a path as the first arg)
LISTS_DIR = sys.argv[1] if len(sys.argv) > 1 else "/penguin/local/paramo/lists"

# Regex to extract base var and optional month from filenames like:
#  - CHELSA-traCE21k_bio1_-155_V1.0.tif      -> var='bio1', month=None
#  - CHELSA-traCE21k_pr_01_-155_V1.0.tif     -> var='pr',   month='01'
#  - CHELSA-traCE21k_tasmax_07_-155_V1.0.tif -> var='tasmax', month='07'
PATTS = [
    re.compile(r'CHELSA-[^_]+_([a-z0-9]+?)(?:_([0-9]{2}))?_(?:-?\d+)_', re.IGNORECASE),
    re.compile(r'_([a-z0-9]+?)(?:_([0-9]{2}))?_(?:-?\d+)_', re.IGNORECASE),
]

def extract_var_month(fname: str):
    for p in PATTS:
        m = p.search(fname)
        if m:
            var = m.group(1).lower()
            mon = m.group(2) if m.lastindex and m.lastindex >= 2 else None
            return var, mon
    return None, None

def main():
    if not os.path.isdir(LISTS_DIR):
        print(f"Lists directory not found: {LISTS_DIR}", file=sys.stderr)
        sys.exit(1)

    list_files = sorted(f for f in os.listdir(LISTS_DIR)
                        if f.endswith(".txt") and f.startswith("trace_"))

    if not list_files:
        print("No trace_*.txt list files found.", file=sys.stderr)
        sys.exit(1)

    for lf in list_files:
        path = os.path.join(LISTS_DIR, lf)
        by_var_months = defaultdict(set)
        by_var_counts = Counter()

        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                fn = line.strip()
                if not fn or fn.startswith("#"):
                    continue
                var, mon = extract_var_month(fn)
                if var is None:
                    continue
                by_var_counts[var] += 1
                if mon:
                    by_var_months[var].add(mon)

        print(f"\n=== {lf} ===")
        if not by_var_counts:
            print("  (no recognizable entries)")
            continue

        for var in sorted(by_var_counts.keys(),
                          key=lambda v: (re.sub(r'\d+$', '', v), int(re.search(r'\d+$', v).group()) if re.search(r'\d+$', v) else 999)):
            cnt = by_var_counts[var]
            months = sorted(by_var_months[var]) if var in by_var_months else []
            if months:
                print(f"  {var}: {cnt} files, months {{{', '.join(months)}}}")
            else:
                print(f"  {var}: {cnt} files")

if __name__ == "__main__":
    main()
