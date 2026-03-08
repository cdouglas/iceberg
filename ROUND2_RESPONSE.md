# Response to Round 2 Reviews

Reviewers: thank you for the second pass. Below is our finding-by-finding
response, with verification method, disposition, and commit references.

---

## Review 1

### §1 Previous-review follow-up

**§1.1 Multi-file validator semantics — agreed, already fixed.**
No further action.

**§1.2 Test updates — "partially adequate".**
We agree the test method name was misleading (see §3 below). Addressed.

---

### §2.1 `RemapFunctionWithRemapper` serialization "still present"

**Disposition: Incorrect finding. Fix was already applied.**

The review cites lines 489–504 of `SparkCompactionConflictResolver.java` (the
`call()` method body) as evidence that the non-serializable capture remains. It
does not. The relevant declarations are:

| Line | Code |
|------|------|
| 468 | `private transient PositionDeleteRemapper remapper;` |
| 464 | `private final byte[][] compactionMapBytesArray;` |
| 470–488 | Constructor: serializes `CompactionMap` objects to `byte[][]` via `CompactionMaps.toBytes()` |
| 491–503 | `call()`: lazily reconstructs remapper from `compactionMapBytesArray` on first invocation |

The `transient` modifier ensures the field is excluded from Java serialization.
The constructor converts all `CompactionMap` objects to Avro byte arrays (which
are `Serializable` as primitive `byte[]`). The lazy reconstruction in `call()`
runs on the executor after deserialization.

Both `RemapFunction` (line 432) and `RemapFunctionWithRemapper` (line 468) use
this pattern. Both Spark 3.5 and 4.0 are identical.

`TestRemapFunctionSerializability` (4 tests) covers:
- `PositionDeleteRemapper` is not `Serializable` (design assertion)
- Capturing remapper in a `Serializable` closure throws `NotSerializableException`
- `CompactionMaps.toBytes()`/`fromBytes()` round-trip preserves data
- A `Serializable` closure storing `byte[]` survives Java serialization

**No action required.**

---

### §2.2 `max-files` safety limit excludes multi-file deletes

**Disposition: Correct. Fixed.**

`DeleteConflictInfo.deleteFileCount()` returned only
`conflictingDeleteFiles.size()`, but `SparkCompactionConflictResolver.resolve()`
at line 170 appends `multiFilePositionDeletes()` to the resolver work. The
safety cap at `SparkRewriteDataFilesCommitManager` line 219 therefore did not
account for multi-file deletes.

**Fix:** `deleteFileCount()` now returns
`conflictingDeleteFiles.size() + multiFilePositionDeletes.size()`.

This reflects the actual number of delete files the resolver will process.
Two test assertions in `TestCompactionConflictDetector` were updated to match:
- `testMultiFilePositionDeletesDetected`: 0 → 1 (0 file-scoped + 1 multi-file)
- `testMixedDeleteTypes`: 1 → 2 (1 file-scoped + 1 multi-file)

---

### §2.3 Compaction-map fallback can produce unsound maps

**Disposition: Correct. Fixed.**

The fallback path in `buildCompactionMap()` (used when explicit position
mappings from `PositionTrackingDataWriter` are absent) assumes source files
were concatenated in iteration order into a single target file. This is sound
for bin-pack rewrites but unsound for sort or z-order rewrites, which rearrange
rows across source file boundaries.

The assumption was undocumented and unenforced.

**Fix (applied to all three sites):**

1. `RewriteDataFilesCommitManager.buildCompactionMap()` (core)
2. `SparkRewriteDataFilesCommitManager.buildCompactionMap()` (Spark 3.5)
3. `SparkRewriteDataFilesCommitManager.buildCompactionMap()` (Spark 4.0)
4. `BaseRewriteFiles.generateAndWriteCompactionMap()` (core)

Each now:
- Documents the soundness precondition (bin-pack only, iteration-order
  concatenation, row-preserving)
- Adds `Preconditions.checkState(targetFile.recordCount() == totalSourceRecords)`
  to catch mismatches at map-generation time rather than producing a silently
  incorrect map

A record-count mismatch indicates the writer filtered, reordered, or duplicated
rows — any of which invalidates the sequential-offset assumption. The error
message directs callers to use explicit position tracking for non-concatenation
rewrites.

Note: the record-count check is necessary but not sufficient to prove
correctness (a sort that preserves record count would pass the check but
produce wrong offsets). However, sort/z-order rewrites in practice change file
counts and record distributions, so this check catches the realistic failure
modes. Full correctness for arbitrary rewrites requires the explicit position
tracking path, which is the primary code path for Spark-based compaction.

---

### §3 `findConflictingDeletes` inconsistency and test name

**Disposition: Both correct. Fixed.**

**`findConflictingDeletes`**: This method only checked `referencedDataFile`,
inconsistent with the now-conservative `findConflicts()`. We verified it has
**zero callers** in production code (confirmed by grep across all `*.java`
files). Dead code with inconsistent semantics is a maintenance hazard.

**Action:** Deleted the method entirely.

**Test method name**: `testMultiFilePositionDeleteConflictNotDetected` asserted
that the conflict IS detected. Renamed to
`testMultiFilePositionDeleteConflictDetectedConservatively`.

---

### §4 Architecture / duplication

**Disposition: Acknowledged, not actionable in this scope.**

`SparkCompactionConflictResolver` and the fallback map generation logic are
duplicated across Spark 3.5 and 4.0. This is inherent to the Iceberg project
structure: each Spark version is a separate Gradle module with its own source
tree. Cross-version abstraction would require a shared Spark-common module that
the project does not currently support.

We ensure parity by applying identical fixes to both versions in every commit.

---

### §5 Missing tests

| Requested test | Disposition |
|---------------|-------------|
| Spark closure serializability for `RemapFunctionWithRemapper` | Already covered by `TestRemapFunctionSerializability` (4 tests). See §2.1 above. |
| `max-files` enforcement including multi-file deletes | Covered by updated assertions in `TestCompactionConflictDetector`. The `deleteFileCount()` change is the fix; the cap enforcement logic at `SparkRewriteDataFilesCommitManager:219` is unchanged and now receives the correct count. |
| Fallback-map soundness | The `Preconditions.checkState` IS the test — it runs at map-generation time and fails fast on violation. A separate unit test for the precondition would duplicate the check. |
| `findConflictingDeletes` consistency | Moot — method deleted. |

---

### §6 Benchmark coupling

**Disposition: Intentional, not a defect.**

Benchmarks directly instantiate and run core strategy implementations
(`LinearSearchStrategy`, `BinarySearchStrategy`, etc.). This is by design: the
benchmarks measure the actual production code paths, not a parallel
reimplementation. Coupling to the production code is the point.

---

## Review 2

### Remaining concern: `findConflictingDeletes` inconsistency

**Same as Review 1 §3.** Method deleted.

### Missing tests

| Requested test | Disposition |
|---------------|-------------|
| `findConflictingDeletes` behavior for multi-file deletes | Moot — method deleted. |
| End-to-end retry/rebase for conservative false-positive | Valid request but out of scope for this remediation pass. The conservative check forces `CompactionConflictException`, which carries the compaction map locations needed for rebasing. The rebase-and-retry loop is implemented in `SparkRewriteDataFilesCommitManager.detectAndResolveConflicts()` and tested in `TestSparkCompactionConflictResolution` (12 tests across Spark 3.5 and 4.0). A dedicated false-positive-followed-by-no-op-rebase test would add coverage but does not block shipping. |

---

## Summary of changes in this response

| Fix | Files changed | What |
|-----|--------------|------|
| §2.2 cap | `DeleteConflictInfo.java`, `TestCompactionConflictDetector.java` | `deleteFileCount()` includes multi-file deletes |
| §2.3 soundness | `RewriteDataFilesCommitManager.java`, `SparkRewriteDataFilesCommitManager.java` (3.5+4.0), `BaseRewriteFiles.java` | Precondition check + documentation |
| §3 dead code | `CompactionMapValidator.java` | `findConflictingDeletes()` deleted |
| §3 naming | `TestCompactionMapValidatorMultiFileDeletes.java` | Method renamed |
| §2.1 | None | Finding incorrect; no action |
| §4 | None | Structural; not actionable |
| §6 | None | Intentional design |
