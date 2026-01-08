# Compaction Map Storage: Architectural Alternatives Analysis

## Executive Summary

This document evaluates three approaches for storing compaction map metadata in Apache Iceberg, analyzing each against implementation complexity, runtime overhead, and backward compatibility. The analysis considers two critical use cases:

1. **Concurrent writers** needing to remap position deletes after compaction
2. **SERIALIZABLE transactions** detecting if their readset has changed

## Background: Current Validation Architecture

Based on code analysis (MergingSnapshotProducer.java:767-807), Iceberg's current conflict detection works as follows:

```java
validationHistory() {
  for each snapshot between startingSnapshotId and parent:
    if snapshot.operation() matches (e.g., REPLACE):
      read snapshot.dataManifests(ops.io())  // Reads manifest list
      collect manifests added in this snapshot

  for each collected manifest:
    read manifest entries  // Reads individual manifests
    check for conflicts (deleted files, added files, etc.)
}
```

**Key Observation**: Transactions ALREADY read manifest lists for validation. Only when conflicts are possible do they read individual manifests.

---

## Alternative 1: Compaction Metadata at Manifest Level

### Design

Add a new optional field to the `ManifestFile` schema (api/src/main/java/org/apache/iceberg/ManifestFile.java:90):

```java
Types.NestedField COMPACTION_MAP_LOCATION =
    optional(520, "compaction_map_location", Types.StringType.get(),
             "Location of compaction map file for this manifest");
```

Each manifest file in a compaction snapshot would contain a reference to its compaction map.

### Implementation Complexity: **Medium**

**Changes Required**:
1. **ManifestFile interface** (API change):
   - Add `String compactionMapLocation()` method
   - Default implementation returns `null`

2. **GenericManifestFile** (1 file):
   - Add field: `private String compactionMapLocation = null;`
   - Update constructor and accessors
   - ~20 lines of code

3. **ManifestFile schema** (1 line):
   - Add field ID 520 (already identified as next available)

4. **Compaction operations** (3 files):
   - Modify `SparkBinPackDataRewriter`, `SparkSortDataRewriter`, `SparkZOrderDataRewriter`
   - Generate compaction map during rewrite
   - Set `compactionMapLocation` in new manifests
   - ~100 lines per strategy

5. **Transaction validation** (1 file):
   - Modify `MergingSnapshotProducer.validateDataFilesExist()`
   - Check for `compactionMapLocation` in manifests
   - Trigger remapping if present
   - ~50 lines

**Total LOC**: ~400 lines

**Risk**: Low - Manifest schema is versioned and optional fields are backward compatible

### Runtime Overhead

#### For Concurrent Writers (Need Remapping):
| Operation | Existing | With Alt 1 | Delta |
|-----------|----------|------------|-------|
| Read snapshot list | 1 read | 1 read | 0 |
| Read manifest list | 1 read | 1 read | 0 |
| **Detect compaction** | **Scan all manifests** | **Check manifest.compactionMapLocation ≠ null** | **Avoided manifest reads** |
| Load compaction map | N/A | 1 read (~3MB) | +1 small file |
| Remap positions | N/A | O(n) positions | +CPU (minimal) |
| Write new deletes | 1 write | 1 write | 0 |

**Key Benefit**: Can detect compaction at manifest list level WITHOUT reading manifest contents.

#### For SERIALIZABLE Transactions (Readset Validation):
| Operation | Existing | With Alt 1 | Delta |
|-----------|----------|------------|-------|
| Read snapshot list | 1 read | 1 read | 0 |
| Read manifest list | 1 read | 1 read | 0 |
| **Detect if compacted** | **Read manifests** | **Check manifest.compactionMapLocation ≠ null** | **Avoided reads!** |
| Fail if readset changed | Immediate | Immediate | 0 |

**Key Benefit**: SERIALIZABLE transactions can detect compaction at manifest list level and fail fast WITHOUT reading manifests.

**Problem Identified**: Manifest lists are ALREADY read by validation logic (line 785-795 of MergingSnapshotProducer.java). Adding metadata here costs nothing extra!

### Backward Compatibility: **Excellent**

- **Old readers**: Ignore field ID 520 (optional field, standard Iceberg versioning)
- **Old writers**: Don't populate field 520, remains null
- **Format version**: No bump required (uses existing optional field mechanism)
- **Graceful degradation**: Without maps, transactions fail and retry as before

**Migration Path**: Zero-downtime deployment. New writers populate field, old readers ignore it.

---

## Alternative 2: Pointer in Manifest List to Compaction Map

### Design

The manifest list already contains a list of `ManifestFile` entries. Add a sibling structure for compaction maps:

```java
// Manifest list file format (ManifestLists.java):
{
  "manifests": [
    { "manifest_path": "...", ... },
    { "manifest_path": "...", ... }
  ],
  "compaction_maps": [
    {
      "manifest_path": "...",  // Which manifest this map applies to
      "map_location": "..."    // Where to find the compaction map
    }
  ]
}
```

### Implementation Complexity: **High**

**Changes Required**:
1. **Manifest list format** (BREAKING CHANGE):
   - Define new `CompactionMapEntry` schema
   - Modify manifest list Avro schema
   - Update `ManifestListWriter.V2Writer` (or create V3)
   - ~200 lines

2. **Snapshot interface** (API change):
   - Add `Map<String, String> compactionMaps()` method
   - Returns mapping of manifest path → compaction map location
   - ~50 lines

3. **BaseSnapshot** (1 file):
   - Parse compaction map entries from manifest list
   - Expose via new API method
   - ~100 lines

4. **Compaction operations** (3 files):
   - Generate compaction maps during rewrite
   - Pass map locations to manifest list writer
   - ~100 lines per strategy

5. **Transaction validation** (1 file):
   - Check snapshot.compactionMaps()
   - Trigger remapping if present
   - ~50 lines

**Total LOC**: ~800 lines

**Risk**: Medium - Requires manifest list format evolution (V2→V3 or extended V2)

### Runtime Overhead

#### For Concurrent Writers:
| Operation | Existing | With Alt 2 | Delta |
|-----------|----------|------------|-------|
| Read snapshot list | 1 read | 1 read | 0 |
| Read manifest list | 1 read | 1 read (larger) | +~1KB for maps |
| Detect compaction | Scan manifests | Check compactionMaps ≠ empty | Avoided reads |
| Load compaction map | N/A | 1 read (~3MB) | +1 small file |
| Remap positions | N/A | O(n) positions | +CPU (minimal) |

**Benefit**: Same as Alt 1 - detect at manifest list level.

#### For SERIALIZABLE Transactions:
| Operation | Existing | With Alt 2 | Delta |
|-----------|----------|------------|-------|
| Read manifest list | 1 read | 1 read (larger) | +~1KB |
| Detect if compacted | Read manifests | Check compactionMaps ≠ empty | Avoided reads |

**Benefit**: Same as Alt 1, but with slightly larger manifest list file (+1KB typical).

### Backward Compatibility: **Fair**

- **Format version bump**: Likely requires manifest list format V3
- **Old readers**: Cannot read new manifest lists OR require extended V2 with ignored fields
- **Old writers**: Cannot write to tables after compaction with maps
- **Migration**: Requires coordinated upgrade or dual-format support

**Deployment**: Requires careful rollout strategy, possibly with feature flag.

---

## Alternative 3: Snapshot Summary (Simple Key-Value)

### Design

Store compaction map locations in the snapshot's summary map:

```java
snapshot.summary().put("compaction-maps",
  "{\"manifest1.avro\": \"map1.avro\", \"manifest2.avro\": \"map2.avro\"}");
```

Or multiple keys:
```java
snapshot.summary().put("compaction-map.manifest1.avro", "map1.avro");
snapshot.summary().put("compaction-map.manifest2.avro", "map2.avro");
```

### Implementation Complexity: **Low**

**Changes Required**:
1. **No schema changes** - Uses existing snapshot.summary() map
2. **Compaction operations** (3 files):
   - Generate compaction maps
   - Add entries to snapshot summary via `SnapshotSummary.Builder.set()`
   - ~100 lines per strategy

3. **Transaction validation** (1 file):
   - Check snapshot.summary() for compaction map keys
   - Parse and trigger remapping
   - ~100 lines

**Total LOC**: ~400 lines

**Risk**: Very low - No format changes, pure extension

### Runtime Overhead

#### For Concurrent Writers:
| Operation | Existing | With Alt 3 | Delta |
|-----------|----------|------------|-------|
| Read snapshot metadata | 1 read | 1 read (larger) | +1KB in JSON |
| Read manifest list | 1 read | 1 read | 0 |
| Detect compaction | Scan manifests | Parse summary map | Avoided manifest reads |
| Load compaction map | N/A | 1 read (~3MB) | +1 small file |

**Benefit**: Detect at snapshot level (even earlier than manifest list).

#### For SERIALIZABLE Transactions:
| Operation | Existing | With Alt 3 | Delta |
|-----------|----------|------------|-------|
| Read snapshot metadata | 1 read | 1 read (larger) | +1KB |
| Detect if compacted | Read manifests | Parse summary map | Avoided manifest reads |

**Benefit**: Detect at snapshot level (earliest possible).

**Drawback**: Snapshot metadata bloat if many manifests compacted (100 manifests = ~10KB added).

### Backward Compatibility: **Perfect**

- **No format changes**: Uses existing snapshot.summary() extensibility
- **Old readers**: Ignore unknown keys in summary map
- **Old writers**: Don't add compaction map keys
- **Format version**: No change required
- **Deployment**: Zero-downtime, can be enabled/disabled via feature flag

**Migration**: Completely transparent. No coordination required.

---

## Comparative Analysis

### Implementation Complexity

| Criterion | Alt 1 (Manifest) | Alt 2 (Manifest List) | Alt 3 (Summary) |
|-----------|------------------|------------------------|-----------------|
| LOC estimate | ~400 | ~800 | ~400 |
| API changes | 1 (ManifestFile) | 2 (Snapshot, ManifestList) | 0 |
| Format changes | 1 optional field | Manifest list format | 0 |
| Risk level | Low | Medium | Very Low |
| Test complexity | Medium | High | Low |
| **Winner** | ✓ | | ✓✓ |

### Runtime Overhead

| Criterion | Alt 1 | Alt 2 | Alt 3 |
|-----------|-------|-------|-------|
| Detection level | Manifest list | Manifest list | Snapshot |
| Extra read I/O (writers) | 0 | +1KB list | +1KB snapshot |
| Extra read I/O (SERIALIZABLE) | 0 | +1KB list | +1KB snapshot |
| Avoided manifest reads | ✓✓ | ✓✓ | ✓✓ |
| Metadata bloat | Minimal | ~1KB per snapshot | ~10KB for 100 manifests |
| **Winner** | ✓✓ | ✓ | ✓ |

### Backward Compatibility

| Criterion | Alt 1 | Alt 2 | Alt 3 |
|-----------|-------|-------|-------|
| Old readers | Compatible | Incompatible or complex | Compatible |
| Old writers | Compatible | Incompatible | Compatible |
| Format version | No bump | V2→V3 or extended V2 | No bump |
| Zero-downtime deploy | Yes | No | Yes |
| Rollback safety | Yes | Risky | Yes |
| **Winner** | ✓ | | ✓✓ |

### Architectural Fit

| Criterion | Alt 1 | Alt 2 | Alt 3 |
|-----------|-------|-------|-------|
| Metadata semantics | ✓✓ (map describes manifest) | ✓ (indirect) | ~ (map is orthogonal) |
| Expiration handling | Automatic w/ manifest | Automatic w/ manifest list | Automatic w/ snapshot |
| Granularity | Per-manifest | Per-manifest | Per-snapshot |
| Extensibility | Standard Iceberg pattern | Requires new format | Standard Iceberg pattern |
| **Winner** | ✓✓ | ✓ | ✓ |

---

## Detailed Use Case Analysis

### Use Case 1: Concurrent Writer with Position Deletes

**Scenario**: Transaction T1 starts at snapshot S1, writes position deletes. Compaction C runs, creates S2. T1 commits against S2.

#### Alternative 1 (Manifest):
```
1. T1 attempts commit against S2
2. validateDataFilesExist() iterates S1→S2
3. For S2 (operation=REPLACE):
   - Reads manifest list (ALREADY HAPPENING)
   - Checks manifests: manifest.compactionMapLocation() ≠ null? YES
   - Reads compaction map (~3MB)
   - Remaps T1's position deletes
   - Writes new delete files
   - Retries commit
4. Success
```

**Total I/O**: +1 small file read (map), no extra manifest list reads

#### Alternative 2 (Manifest List):
```
1. T1 attempts commit against S2
2. validateDataFilesExist() iterates S1→S2
3. For S2:
   - Reads manifest list (already larger by ~1KB)
   - Checks snapshot.compactionMaps() ≠ empty? YES
   - Reads compaction map (~3MB)
   - Remaps T1's position deletes
   - Retries commit
4. Success
```

**Total I/O**: +1KB manifest list + 1 small file read (map)

#### Alternative 3 (Summary):
```
1. T1 attempts commit against S2
2. validateDataFilesExist() iterates S1→S2
3. For S2:
   - Reads snapshot metadata (already larger by ~10KB for 100 files)
   - Checks snapshot.summary() for "compaction-map.*" keys? YES
   - Reads compaction map (~3MB)
   - Remaps T1's position deletes
   - Retries commit
4. Success
```

**Total I/O**: +10KB snapshot metadata + 1 small file read (map)

**Winner**: Alt 1 (cleanest, no bloat)

### Use Case 2: SERIALIZABLE Transaction Validation

**Scenario**: Transaction T1 reads data at S1. Compaction C creates S2. T1 commits with `validateNoConflictingDataFiles()`.

#### Alternative 1 (Manifest):
```
1. T1 attempts commit against S2
2. validateAddedDataFiles() iterates S1→S2
3. For S2 (operation=REPLACE):
   - Reads manifest list (ALREADY HAPPENING)
   - Checks manifests: manifest.compactionMapLocation() ≠ null? YES
   - FAIL: Readset has changed (compaction occurred)
4. User receives clear error: "Conflict detected: compaction remapped data files"
```

**Total I/O**: 0 extra (manifest list already read)

#### Alternative 2 (Manifest List):
```
1. T1 attempts commit against S2
2. validateAddedDataFiles() iterates S1→S2
3. For S2:
   - Reads manifest list (larger by ~1KB)
   - Checks snapshot.compactionMaps() ≠ empty? YES
   - FAIL: Readset has changed
4. User receives error
```

**Total I/O**: +1KB manifest list

#### Alternative 3 (Summary):
```
1. T1 attempts commit against S2
2. validateAddedDataFiles() iterates S1→S2
3. For S2:
   - Reads snapshot metadata (larger by ~10KB)
   - Checks snapshot.summary() for "compaction-map.*"? YES
   - FAIL: Readset has changed
4. User receives error
```

**Total I/O**: +10KB snapshot metadata

**Winner**: Alt 1 (zero extra I/O)

---

## Recommendation Matrix

| Priority | Recommended Alternative | Rationale |
|----------|-------------------------|-----------|
| **Implementation speed** | Alt 3 (Summary) | No format changes, lowest LOC, fastest to ship |
| **Long-term architecture** | Alt 1 (Manifest) | Best semantic fit, no bloat, standard Iceberg pattern |
| **Zero-downtime deploy** | Alt 1 or Alt 3 | Both support backward compatibility |
| **Minimal overhead** | Alt 1 (Manifest) | Zero extra I/O for validation path |
| **Format stability** | Alt 3 (Summary) | No format changes, safest |

---

## Final Recommendation: **Alternative 1 (Manifest-Level Metadata)**

### Why Alternative 1?

1. **Architectural Correctness**: Compaction maps describe how files within a manifest were transformed. This metadata semantically belongs at the manifest level.

2. **Zero Overhead for Critical Path**: SERIALIZABLE transactions already read manifest lists during validation. Adding a field costs nothing. Alternative 3 bloats snapshot metadata unnecessarily.

3. **Standard Iceberg Pattern**: Adding optional fields to schemas is the standard evolution mechanism (see field ID 519 for encryption metadata, field ID 518 for NaN flags).

4. **Clean Expiration**: When manifests are expired, their compaction maps are naturally dereferenced. No special cleanup logic needed.

5. **Backward Compatible**: Old readers ignore field 520. Old writers leave it null. No coordination required for deployment.

6. **Right Granularity**: Maps are per-manifest, matching the granularity of file rewrites during compaction.

### Implementation Roadmap for Alternative 1

**Phase 1: Schema Extension** (Week 1)
- Add field 520 to ManifestFile schema
- Update GenericManifestFile with accessor
- Backward compatibility tests

**Phase 2: Compaction Map Generation** (Weeks 2-3)
- Implement CompactionMap data structure
- Modify SparkBinPackDataRewriter to track position mappings
- Write compaction maps to storage
- Set compactionMapLocation in manifests

**Phase 3: Transaction Integration** (Weeks 4-5)
- Detect compaction via manifest.compactionMapLocation()
- Implement PositionDeleteRemapper
- Integrate with validateDataFilesExist()
- Handle remapping and commit retry

**Phase 4: Testing** (Week 6)
- Unit tests for map generation and remapping
- Integration tests for concurrent compaction + writes
- Backward compatibility tests (old readers/writers)
- Performance benchmarks

### Alternative 3 as Interim Solution

If time-to-market is critical, implement **Alternative 3** first:
- Ships in 2-3 weeks instead of 6
- Provides immediate value
- Can migrate to Alternative 1 later by:
  1. Checking both summary and manifest for maps
  2. New compactions write to manifest (Alt 1)
  3. Old maps in summary still work
  4. Eventually deprecate summary-based maps

This gives a **staged deployment path**: quick wins with Alt 3, ideal architecture with Alt 1.

---

## Appendix: Code References

### Current Validation Logic
- **File**: `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java`
- **Method**: `validationHistory()` (lines 767-807)
- **Key insight**: Manifest lists are ALWAYS read during validation

### Manifest Schema
- **File**: `api/src/main/java/org/apache/iceberg/ManifestFile.java`
- **Line 90**: "// next ID to assign: 520"
- **Extension point**: Adding field 520 is the natural next step

### Snapshot Summary
- **File**: `core/src/main/java/org/apache/iceberg/SnapshotSummary.java`
- **Usage**: Already extensible via `EXTRA_METADATA_PREFIX` (line 60)
- **Pattern**: Custom properties can be added without format changes

### Transaction Validation
- **File**: `core/src/main/java/org/apache/iceberg/BaseRowDelta.java`
- **Methods**: `validateFromSnapshot()`, `validateDeletedFiles()`
- **Integration point**: Where compaction detection would trigger remapping

---

## Conclusion

**Alternative 1 (Manifest-Level Metadata)** is the superior choice for production deployment, offering:
- Zero runtime overhead
- Perfect backward compatibility
- Clean architectural fit
- Standard Iceberg extension pattern

**Alternative 3 (Snapshot Summary)** serves as an excellent interim solution if rapid deployment is needed, with a clear migration path to Alternative 1.

**Alternative 2 (Manifest List Pointer)** introduces unnecessary complexity and compatibility challenges without providing meaningful benefits over Alternative 1.
