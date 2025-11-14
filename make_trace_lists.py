# save as make_trace_lists.py
import json, os, re, sys
data = json.load(open("chelsatrace_filelist.json", "r"))
os.makedirs("lists", exist_ok=True)
by_var = {}
for item in data:
    if item.get("IsDir"): continue
    path = item["Path"]           # e.g., "bio1/CHELSA-traCE21k_bio1_-155_V1.0.tif"
    var, fname = path.split("/", 1)
    by_var.setdefault(var, []).append(fname)

def time_key(fn):
    # extract the timeID between underscores (works for negatives too)
    m = re.search(r'_(\-?\d+)_', fn)
    return int(m.group(1)) if m else 0

for var, files in by_var.items():
    files.sort(key=time_key)
    with open(os.path.join("lists", f"trace_{var}.txt"), "w") as out:
        out.write("\n".join(files) + "\n")
print("Wrote:", ", ".join(sorted(f"lists/trace_{v}.txt" for v in by_var)))
