# Snapshot rewrite: what the two experiments show

Companion to the figures in `results/plots/`. The figures are deliberately spare; this is where
the setup and the reading go.

**One sentence of background.** A compaction leaves the snapshots that preceded it pinning the old
layout, so the table holds two copies of its data: the one the history points at and the one the
compaction wrote. A snapshot rewrite re-points those older snapshots at the compaction's files, so
the old layout can be deleted. Rows that were deleted *during* the window are the catch: they are
not in the compaction, so the rewrite has to write them back out.

Both experiments are single-node, on generated tables, with every byte read off files the rewrite
actually wrote. Reproduce with the commands in `README.md`.

---

## Experiment 1 — the insert/delete mix

**Setup.** 500,000 rows appended in five files and compacted; that compaction is `C_A`. Then a
window of eight commits, applying 500,000 row-changes in total, each commit inserting and deleting
in one transaction. Then a second compaction, `C_B`, and the rewrite of the window onto it. The one
thing that varies across the eight runs is the **share of those 500,000 row-changes that are
deletions** — 0% at the left, 90% at the right. Deletions are clustered runs, the shape a purge
has, not a uniform scatter.

### `mix_space_reclaimed`

Of the bytes the rewrite frees, blue is what stays freed and orange is what it has to spend writing
the window's deleted rows back out.

- **Left edge (0% deletions): 218 MiB reclaimed, nothing spent.** An insert-only window recovers no
  rows at all, so the rewrite reads no data — it derives the delete vectors from the compaction map
  arithmetic alone. This is the case worth calling out on a slide.
- **Right edge (90% deletions): 22 MiB reclaimed, 98 MiB spent.** The window killed 450,000 of the
  500,000 base rows, so nearly everything the rewrite frees goes straight back into recovering
  them.
- The total bar height falls too, because a delete-heavy window leaves a smaller table behind.

### `mix_before_after`

The same eight runs, as what the history holds down before the rewrite and after it. The left bar
of each pair is `C_A` + everything the window wrote + `C_B`; the right bar is what stands after.
`C_A` and the original interstitial files are gone; in their place is the orange layer — the
window's deleted rows written back out, plus the delete vectors.

| deletions | before | after | shrink |
|---|---|---|---|
| 0% | 436 MiB | 218 MiB | 50% |
| 25% | 354 MiB | 191 MiB | 46% |
| 50% | 273 MiB | 164 MiB | 40% |
| 90% | 142 MiB | 120 MiB | 15% |

**The point of the figure is the diminishing gain.** At the left the table halves; at the right the
two bars are nearly the same height. Note that the purple `C_A` segment is a constant 109 MiB
across every run — the same starting table — so what shrinks the gap is entirely the growth of the
orange layer.

**One number to have ready.** The `maxDeadRatio` guard, which is what stops the rewrite when
recovery gets too expensive, hits its 0.5 default at exactly the 50% column here. Everything to the
right of it is refused by default.

---

## Experiment 2 — window depth, and the cost of the delete vectors

**Setup.** An **insert-only** window: 100,000 rows compacted into `C_A`, then `m` commits each
appending 2,000 rows, then `C_B` and the rewrite. Insert-only means nothing is recovered, so the
delete vectors and the per-snapshot metadata are the *entire* cost. `m` runs from 1 to 32, across
both format versions and both a 20-column and a 2-column schema.

**Why this is in tension with the good news from Experiment 1.** An insert-only window is the
obvious win: no data is read, and each rewritten snapshot is just a delete vector derived from the
map. But every rewritten snapshot has to mask *every row inserted after it*, so the number of
positions masked across the window grows quadratically — `r·m(m+1)/2`, which here is 2,000
positions at `m=1` and **1,056,000 at `m=32`**. The question is whether the obvious win survives
that.

### `depth_reclaim_by_format_version`

Two panels, one per schema width, because the two tables differ by two orders of magnitude in bytes
and sharing a y-axis would hide the crossing. **Watch the y-axis scales: left is 0-36 MiB, right is
-0.6 to +0.6 MiB.**

- **Left (20-column rows): both versions stay comfortably positive** and rise, because the table
  itself grows as the window deepens.
- **Right (2-column rows): v2 crosses zero at about `m=17`** and is 45% underwater by `m=32`. v3
  stays positive throughout.

**The reading, which is the part the figure cannot show on its own: the quadratic term costs the
same number of bytes in both panels.** The v3-minus-v2 gap is identical in the two schemas to three
decimal places — 0.119 vs 0.120 MiB at `m=1`, 1.233 vs 1.235 MiB at `m=32` — because the position
count is identical and only the encoding differs. Row width does not change the cost; it changes
whether the cost matters.

### What the position count actually costs (this replaces a figure)

Measured per rewritten snapshot, 20-column rows:

| | m=1 | m=8 | m=32 |
|---|---|---|---|
| positions masked, window total | 2,000 | 72,000 | 1,056,000 |
| v2 position-delete file | 2.0 KiB | 10.0 KiB | 35.2 KiB |
| v3 deletion vector | 294 B | 525 B | 597 B |
| manifest + manifest list | 14.2 KiB | 14.0 KiB | 13.9 KiB |

- **Under v2 the quadratic count is quadratic in bytes**: a position-delete file stores a row per
  position, measured at ~1.1 bytes each, so the window total goes from 4 KiB to 1.1 MiB.
- **Under v3 it is not worth worrying about.** A deletion vector is flat at roughly 0.5 KiB no
  matter how much it masks: what a snapshot masks is the rows inserted after it, an
  order-preserving compaction lays those out consecutively, and Roaring stores a contiguous range
  as a run rather than as positions. The window total only grows because there are more snapshots,
  not because each vector grows.
- **So under v3 the binding cost is neither** — it is the ~14 KiB of manifest and manifest list
  every retained snapshot needs, twenty-five times the vector beside it, and flat in `m`. That is
  what makes the v3 2-column line flatten rather than climb.

**The takeaway for a slide:** on v3, deep windows are fine and the quadratic position count is a
non-issue. On v2 it is real, and it is the row width of the table that decides whether it sinks
you.

---

## What these figures do not show

Generated tables, one machine, local files, and validated against the Java reader only. No
object-store latency, no window hundreds of commits deep, no production layout. Errata 9 in
`docs/docs/compaction_maps_rewrite.md` stands: the dry run exists so the measurement can be taken
on a real table, and it has not been. This moves the largest measured configuration from 80k rows
in one file to 500k across a multi-file compaction; it does not settle the question.

One gap the sweep did turn up, worth knowing if anyone asks about the dry run: `estimate()` prices
metadata at a flat 10 KiB plus 14 KiB per snapshot, which is right for an insert-only window (139
KiB predicted, 134 KiB measured) and roughly 40% low for any window that recovers rows (139 KiB
predicted, ~225 KiB measured), because a snapshot holding a recovered-rows file needs its own data
manifest and cannot share the restamped one. It does not change the verdict on anything measured
here, where metadata is under 0.2% of what is freed, but the dry run is used precisely to price
small tables.
