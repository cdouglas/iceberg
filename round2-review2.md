Review of commit fe7fe00a98e970a63ad0b6899d63db583fee6b73

## Scope
Per request, this review is restricted to commit `fe7fe00a98e970a63ad0b6899d63db583fee6b73`.

## What this commit fixes correctly

### 1) Multi-file position delete validator gap is fixed conservatively
`CompactionMapValidator.findConflicts` now handles three cases explicitly and conservatively treats multi-file position deletes as conflicting when compacted files exist.

- File-scoped position deletes and DVs: checked via `referencedDataFile`.
- Multi-file position deletes (`referencedDataFile == null`, content `POSITION_DELETES`): `conflicts.addAll(compactedFiles)`.

This matches soundness requirements for position-delete writes with physical `(file_path, position)` addresses.

## Test verification for the fix

### 2) Validator tests now assert conflict detection for multi-file deletes
`TestCompactionMapValidatorMultiFileDeletes` was updated from "known gap" semantics to requiring conflict detection for multi-file deletes.

### 3) Serializable isolation test was corrected for write-vs-read semantics
`TestSerializableIsolationWithCompaction` now distinguishes:
- SERIALIZABLE read validation behavior (structural compaction with map allowed), and
- position-delete write rebasing behavior (V2 multi-file delete must fail/rebase).

In V2 cases, commit now expects `CompactionConflictException` (or chain exceptions where applicable), while V3 DV case targeting non-compacted file remains allowed.

## Correctness assessment of this commit

### Soundness
The reintroduced conservative check is sound for correctness. It may increase false positives (extra rebasing) but avoids stale-address commits and missed deletions.

### Remaining concern in the same snapshot
`CompactionMapValidator.findConflictingDeletes(...)` still only includes delete files with `referencedDataFile != null`. It does not include multi-file position deletes, which is semantically inconsistent with the now-conservative `findConflicts(...)` behavior.

This method appears utility-like and currently unused, but should either:
1. include multi-file delete files conservatively, or
2. be documented as intentionally file-scoped only.

## Architecture notes (for this commit scope)

- The semantic correction in this commit is directionally right and aligns validation with write-correctness.
- There remains duplication between conflict-detection concepts in validator vs detector APIs. Consolidating shared conflict classification rules (file-scoped / DV / multi-file) into one reusable helper would reduce future regressions.

## Test completeness for this change

Good improvements were made for the specific semantic fix. Additional desirable tests:
1. direct unit test covering `findConflictingDeletes(...)` behavior for multi-file deletes (to pin intended contract),
2. an end-to-end retry/rebase test showing conservative false-positive case succeeds after no-op remap path.

