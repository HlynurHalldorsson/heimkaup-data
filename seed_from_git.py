#!/usr/bin/env python3
"""Rebuild the blob store's data tree from the last pre-migration git commit.

The 2026-08-22 migration (46ae44eb) moved tracker data out of git and into a
Vercel Blob store. When that store was lost (suspended on the old Hobby team),
the parent commit still held the complete data tree, in the old layout:

  product_histories/*.json   (same format as blob)
  current_inventory.json     (same format as blob)
  sales_events.json          (flat; blob layout shards it by month + index)

This script converts that tree into the exact blob layout the tracker and the
dashboard expect, and writes _upload_manifest.json so upload_blobs.mjs pushes
everything. Run it from a checkout of the PRE-migration commit's data files.

Usage: python3 seed_from_git.py <src-dir> <out-dir>
"""
import json
import os
import shutil
import sys


def main() -> None:
    src, out = sys.argv[1], sys.argv[2]
    shutil.rmtree(out, ignore_errors=True)
    os.makedirs(os.path.join(out, "sales_events"))

    manifest = []

    # Product histories and the raw inventory snapshot carry over unchanged.
    shutil.copytree(os.path.join(src, "product_histories"),
                    os.path.join(out, "product_histories"))
    shutil.copy(os.path.join(src, "current_inventory.json"),
                os.path.join(out, "current_inventory.json"))
    manifest.append("current_inventory.json")
    for name in os.listdir(os.path.join(out, "product_histories")):
        manifest.append(f"product_histories/{name}")

    # The flat sales_events.json becomes monthly shards plus an index,
    # matching _save_sales_events in heimkaup_inventory_tracker.py.
    with open(os.path.join(src, "sales_events.json"), encoding="utf-8") as f:
        events = json.load(f)
    months = {}
    for event in events:
        months.setdefault(event["timestamp"][:7], []).append(event)
    for month, shard in months.items():
        relpath = f"sales_events/{month}.json"
        with open(os.path.join(out, relpath), "w", encoding="utf-8") as f:
            json.dump(shard, f, ensure_ascii=False)
        manifest.append(relpath)
    with open(os.path.join(out, "sales_events/index.json"), "w") as f:
        json.dump(sorted(months), f)
    manifest.append("sales_events/index.json")

    # current_snapshot.json (compact {id: [stock, price]} map) did not exist in
    # git; derive it from current_inventory.json like _save_current_snapshot.
    with open(os.path.join(src, "current_inventory.json"), encoding="utf-8") as f:
        inventory = json.load(f)
    snapshot = {
        "timestamp": inventory["timestamp"],
        "products": {
            str(p["id"]): [p["total_stock"], round(p["price"] / 100, 2)]
            for p in inventory["products"]
        },
    }
    with open(os.path.join(out, "current_snapshot.json"), "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)
    manifest.append("current_snapshot.json")

    with open(os.path.join(out, "_upload_manifest.json"), "w") as f:
        json.dump(sorted(manifest), f)

    print(f"Staged {len(manifest)} files "
          f"({len(months)} sales months, {len(inventory['products'])} products in snapshot)")


if __name__ == "__main__":
    main()
