# Round 4 review: cmpmap branch correctness

## Scope
- Re-reviewed cmpmap implementation at `abb56f7fd5eb093d03c960bf8d3a8af9e2a4a033`.
- Re-checked issues raised in rounds 1-3 against current code and `ROUND2_RESPONSE.md`/`ROUND3_RESPONSE.md`.
- Ignored `benchmark/` per request.

## Status of prior findings
- ✅ Spark closure serializability issue appears resolved by serializing compaction maps to bytes and lazy remapper init on executor.
- ✅ Validator now treats multi-file position deletes conservatively as conflicting.
- ✅ PositionTrackingDataWriter now assigns mappings across multiple output files by boundaries.
- ✅ Action-level unsound fallback map generation was removed (now skips + warns when tracking is absent).

## New finding

### 1) Chain collection is non-deterministic and can drop required downstream maps for fan-out rewrites
**Severity: High**

`CompactionMapValidator.detectChains` correctly detects that a chain exists if _any_ target of a conflicting source is compacted again. However, `collectChainMaps` only follows a **single** downstream target ("first target that continues the chain") and stops there.

For source files that fan out to multiple target files (split rewrite), this can omit other downstream maps that are still required to remap some source positions. `getTargetFiles` returns a `HashSet`, so target traversal order is non-deterministic, making selected chain path non-deterministic as well.

#### Why this is incorrect
A single source file can have runs mapping to different targets. If multiple targets are later compacted, remapping of deletes from the original source may require multiple second-hop maps. Returning a chain that includes only one downstream branch can leave part of the mapping stale.

#### Evidence
- `detectChains` checks all targets and flags chain when any target is re-compacted, then calls `collectChainMaps(...)` once per conflict file.
- `collectChainMaps` follows only one downstream target: it sets `currentFile` to the first target found in iteration order that appears in `sourceToMap`, then breaks.
- `getTargetFiles` uses `Sets.newHashSet()`, so target iteration order is unspecified.

#### Suggested fix direction
Replace linear single-path traversal with graph traversal (BFS/DFS) over all downstream targets that are sources in later maps, collecting all reachable maps in topological/snapshot order. At minimum, preserve deterministic ordering and include all branches.

## Production readiness assessment
Not production-ready yet due to the high-severity chain-composition correctness risk above.

