# Remapping Optimization Evaluation Report

**Date**: 2026-01-15  
**Status**: Phases 1-5 Completed, Integration Gaps Identified  
**Next Steps**: Phases 6-7 (Production Integration)

## Executive Summary

The remapping optimization implementation (Phases 1-5) is **functionally complete** with excellent algorithmic foundations and comprehensive unit test coverage. However, there are **critical integration gaps** that prevent these optimizations from being used in production workloads:

### ✅ Completed (Excellent)
- All 5 remapping strategies implemented and tested
- 44 comprehensive unit tests covering all algorithms
- Automatic algorithm selection for single-position lookups
- Property-based tests validating correctness

### ❌ Missing (Critical for Production)
- **Bulk API integration**: `runForPositions()` bulk API exists but is never called
- **Smart algorithm selector**: No runtime selection based on data characteristics
- **Predicate pushdown**: No filtering of irrelevant runs based on min/max bounds
- **Integration test coverage**: No tests exercising bulk strategies in realistic scenarios
- **Performance benchmarks**: No JMH benchmarks to validate claimed speedups

## Detailed Findings

### 1. Algorithm Selection (Status: INSUFFICIENT)

#### What's Implemented

`RemappingStrategy.Factory.create()` provides basic threshold-based selection:

```java
// Lines 111-125 in RemappingStrategy.java
public static RemappingStrategy create(List<Run> runs) {
  if (runs.size() < 10) {
    return new LinearSearchStrategy(runs);
  }
  if (runs.size() < 100) {
    return new BinarySearchStrategy(runs);
  }
  return new IntervalTreeStrategy(runs);
}
```

**Selection criteria:**
- m < 10 → Linear search
- 10 ≤ m < 100 → Binary search
- m ≥ 100 → Interval tree

#### Critical Gaps

**Gap 1: No information about n (delete count)**

The factory only knows `m` (number of runs) but not `n` (number of positions to remap). This prevents optimal selection:

```java
// CANNOT make this decision without n:
if (n > m * 100) {
  use RangeQueryStrategy();  // Optimal when n >> m
} else if (sortedPositions) {
  use StreamJoinStrategy();   // Optimal when sorted
}
```

**Gap 2: No sortedness detection**

StreamJoinStrategy requires sorted positions for O(n + m) performance. Without sortedness detection, it falls back to O(n log m) - no better than binary search.

**Gap 3: No gap ratio estimation**

RangeQueryStrategy is optimal when runs have significant gaps (sparse coverage). Without gap ratio, we can't identify these scenarios.

**Gap 4: Bulk strategies are never selected**

The Factory is only called from `GenericFileMapping.runForPosition()` for single-position lookups. StreamJoinStrategy and RangeQueryStrategy are designed for bulk operations but are never used.

#### What's Missing

The smart algorithm selector from `REMAPPING_ALGORITHM_SELECTOR.md` is completely unimplemented:

```java
// PLANNED but NOT IMPLEMENTED:
public class RemappingAlgorithmSelector {
  public CloseableIterable<PositionDelete<?>> selectAndRemap(
      FileMapping mapping,
      CloseableIterable<PositionDelete<?>> deletes,
      RemappingStats stats) {
    
    int m = mapping.runs().size();
    int n = estimateDeleteCount(deletes);
    boolean sorted = isSorted(deletes);
    double gapRatio = estimateGapRatio(mapping);
    
    // Smart decision tree based on data characteristics
    if (m < 10) {
      return new RangeQueryRemappingIterable(mapping, deletes);
    }
    if (n > 0 && n / m > 100) {
      if (gapRatio > 0.3) {
        return new RangeQueryRemappingIterable(mapping, deletes);
      }
    }
    if (sorted && n > m) {
      return new StreamBasedRemappingIterable(mapping, deletes);
    }
    // ...
  }
}
```

### 2. Bulk API Integration (Status: NOT INTEGRATED)

#### What's Implemented

The `runForPositions()` bulk API exists:

```java
// RemappingStrategy.java:71-80
default Map<Long, Run> runForPositions(List<Long> sourcePositions) {
  Map<Long, Run> results = Maps.newHashMapWithExpectedSize(sourcePositions.size());
  for (Long position : sourcePositions) {
    Run run = runForPosition(position);
    if (run != null) {
      results.put(position, run);
    }
  }
  return results;
}
```

StreamJoinStrategy and RangeQueryStrategy override this for O(n + m) and O(m log n) performance.

#### Critical Gap: Never Called

**PositionDeleteRemapper.remapDV()** iterates positions one-by-one:

```java
// Lines 220-237 in PositionDeleteRemapper.java
try (CloseableIterable<Long> positions = reader.readDeletedPositions(dvFile)) {
  for (Long sourcePos : positions) {
    // ONE BY ONE - O(n * log m) instead of O(n + m)!
    CompactionMap.Run run = mapping.runForPosition(sourcePos);
    
    if (run == null) {
      continue;
    }
    
    long targetPos = run.mapPosition(sourcePos);
    remappedPositions
        .computeIfAbsent(mapping.targetFile(), k -> new HashSet<>())
        .add(targetPos);
  }
}
```

**Problem:** For a DV with 1M positions and 100 runs:
- Current: 1M × log(100) ≈ 7M operations
- Optimal (StreamJoin): 1M + 100 ≈ 1M operations
- **Speedup missed: 7x**

#### What Should Happen

```java
// SHOULD BE:
public Map<String, Set<Long>> remapDV(DeleteFile dvFile, FileIO fileIO) {
  FileMapping mapping = fileMappingIndex.get(sourceFile);
  if (mapping == null) {
    return passthroughMapping(dvFile, fileIO);
  }
  
  // Collect all positions first
  List<Long> allPositions = new ArrayList<>();
  try (CloseableIterable<Long> positions = reader.readDeletedPositions(dvFile)) {
    positions.forEach(allPositions::add);
  }
  
  // Use bulk API with smart algorithm selection
  RemappingStrategy strategy = selectOptimalStrategy(mapping, allPositions);
  Map<Long, Run> mappedRuns = strategy.runForPositions(allPositions);
  
  // Group by target file
  return groupByTargetFile(mappedRuns, mapping);
}
```

### 3. Predicate Pushdown (Status: NOT IMPLEMENTED)

#### The Opportunity

When remapping positions, we can filter runs early based on min/max bounds:

```java
// Given deletes: [100, 200, 300, 400, 500]
// Min: 100, Max: 500

// Runs:
Run(0, 0, 50)      // sourceRange [0, 50)   - SKIP (max < 100)
Run(60, 50, 40)    // sourceRange [60, 100) - SKIP (max <= 100)
Run(100, 90, 200)  // sourceRange [100, 300) - KEEP (overlaps [100, 500])
Run(320, 290, 100) // sourceRange [320, 420) - KEEP (overlaps [100, 500])
Run(600, 500, 50)  // sourceRange [600, 650) - SKIP (min > 500)
```

**Benefit:** Only process 2 out of 5 runs (40% reduction).

#### What's Missing

No filtering in any strategy:

**StreamJoinStrategy** (lines 128-166):
```java
// Processes ALL runs even if positions don't overlap
for (Long position : sortedPositions) {
  while (runIndex < runs.size() && position >= currentRunEnd) {
    runIndex++;  // Advances through ALL runs
    if (runIndex < runs.size()) {
      currentRun = runs.get(runIndex);
      currentRunEnd = currentRun.sourcePosition() + currentRun.length();
    }
  }
  // Check current run...
}
```

**Should be:**
```java
public Map<Long, Run> runForPositions(List<Long> sortedPositions) {
  if (sortedPositions.isEmpty()) {
    return Collections.emptyMap();
  }
  
  // Predicate pushdown: filter runs by min/max
  long minPos = sortedPositions.get(0);
  long maxPos = sortedPositions.get(sortedPositions.size() - 1);
  
  List<Run> relevantRuns = runs.stream()
      .filter(r -> {
        long runEnd = r.sourcePosition() + r.length();
        return runEnd > minPos && r.sourcePosition() <= maxPos;
      })
      .collect(Collectors.toList());
  
  // Stream join only over relevant runs
  return streamJoin(sortedPositions, relevantRuns);
}
```

**Impact:** For sparse deletes, this could reduce work by 50-90%.

### 4. Test Coverage (Status: GOOD UNIT, MISSING INTEGRATION)

#### Unit Test Coverage (✅ Excellent)

**TestRemappingStrategies.java** - 44 test methods:

```
Basic tests (5 strategies × 3 test types = 15 tests):
- testLinearSearchBasic(), testBinarySearchBasic(), etc.
- Tests all strategies with same data for consistency

Large-scale tests (5 strategies × 3 scales = 15 tests):
- testLinearSearchLarge(), testBinarySearchLarge(), etc.
- Tests with 10, 100, 1000 runs

Property-based tests (comparing all strategies):
- testAllStrategiesMatch() - 10,000 random lookups
- testAllStrategiesBulkMatch() - bulk API consistency
- testAllStrategiesMatchHighFanIn() - n >> m scenario

Edge cases:
- Empty runs
- Single run
- Gaps between runs
- Out of bounds positions
- Boundary testing
```

**Coverage: ~100% of implemented code**

#### Integration Test Coverage (❌ Missing)

**What exists:**
- `TestPositionDeleteRemappingIntegration.java` - calls `remapDelete()` in loops
- `TestDVRemappingEndToEnd.java` - calls `remapDV()` which uses one-by-one iteration

**What's missing:**
1. No tests exercising bulk strategies (StreamJoin, RangeQuery)
2. No tests validating algorithm selection decisions
3. No tests measuring actual performance improvements
4. No tests with realistic data distributions
5. No tests comparing single-position vs bulk APIs

**Should have:**

```java
@Test
public void testBulkRemappingWithStreamJoin() {
  // Create 100 runs
  List<Run> runs = createRuns(100);
  
  // Create 10,000 SORTED position deletes
  List<Long> sortedPositions = createSortedPositions(10000);
  
  // Test that StreamJoinStrategy is selected and used
  RemappingStrategy strategy = RemappingStrategy.Factory.create(runs);
  
  long startTime = System.nanoTime();
  Map<Long, Run> results = strategy.runForPositions(sortedPositions);
  long elapsed = System.nanoTime() - startTime;
  
  // Verify results correct
  assertThat(results).hasSize(expectedMatches);
  
  // Verify performance (should be ~10ms for stream join, ~100ms for naive)
  assertThat(elapsed).isLessThan(20_000_000); // 20ms
}

@Test
public void testDVRemappingUsesOptimalStrategy() {
  // Create DV with 1M positions
  DeleteFile dv = createLargeDV(1_000_000);
  
  // Create map with 100 runs
  CompactionMap map = createCompactionMap(100);
  
  PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
  
  long startTime = System.nanoTime();
  Map<String, Set<Long>> remapped = remapper.remapDV(dv, fileIO);
  long elapsed = System.nanoTime() - startTime;
  
  // Should complete in <1 second (not 10+ seconds)
  assertThat(elapsed).isLessThan(1_000_000_000); // 1 second
}
```

### 5. Performance Benchmarks (Status: NOT IMPLEMENTED)

#### What's Missing

No JMH benchmarks exist. The planning documents specify:

```java
// REMAPPING_ALGORITHM_SELECTOR.md:291-325
@State(Scope.Benchmark)
public class RemappingAlgorithmBenchmark {
  @Param({"10", "100", "1000", "10000"})
  int numRuns;
  
  @Param({"1000", "10000", "100000", "1000000"})
  int numDeletes;
  
  @Param({"0.0", "0.3", "0.5", "0.8"})
  double gapRatio;
  
  @Param({"true", "false"})
  boolean sorted;
  
  @Benchmark
  public void linearScan() { /* ... */ }
  
  @Benchmark
  public void binarySearch() { /* ... */ }
  
  @Benchmark
  public void intervalTree() { /* ... */ }
  
  @Benchmark
  public void streamJoin() { /* ... */ }
  
  @Benchmark
  public void rangeQuery() { /* ... */ }
  
  @Benchmark
  public void autoSelect() { /* ... */ }
}
```

**Impact:** Without benchmarks, we cannot:
- Verify claimed 100-10,000x speedups
- Validate algorithm selection thresholds
- Identify regressions
- Tune thresholds for production workloads

## Recommendations

### Priority 1: Critical for Production (Phase 6)

**1. Implement Bulk API Usage in PositionDeleteRemapper**

```java
// New method in PositionDeleteRemapper
public Map<String, Set<Long>> remapDVBulk(DeleteFile dvFile, FileIO fileIO) {
  // Collect positions into list
  List<Long> positions = collectPositions(dvFile, fileIO);
  
  // Use bulk API with automatic strategy selection
  FileMapping mapping = fileMappingIndex.get(sourceFile);
  RemappingStrategy strategy = selectOptimalStrategy(mapping, positions);
  Map<Long, Run> mappedRuns = strategy.runForPositions(positions);
  
  // Group by target file
  return groupByTargetFile(mappedRuns, mapping);
}
```

**Effort:** 2-3 days  
**Impact:** 5-10x speedup for DV remapping  
**Risk:** Low (existing bulk APIs tested)

**2. Add Integration Tests for Bulk Strategies**

Create `TestBulkRemappingIntegration.java` with:
- Tests exercising StreamJoinStrategy in realistic scenarios
- Tests exercising RangeQueryStrategy in realistic scenarios
- Performance assertions (timing bounds)
- Comparison tests (bulk vs one-by-one)

**Effort:** 3-5 days  
**Impact:** High confidence in production deployment  
**Risk:** Low

**3. Implement Predicate Pushdown**

Add filtering to bulk strategies:

```java
// In StreamJoinStrategy.runForPositions()
long minPos = sortedPositions.get(0);
long maxPos = sortedPositions.get(sortedPositions.size() - 1);

List<Run> relevantRuns = runs.stream()
    .filter(r -> overlaps(r, minPos, maxPos))
    .collect(Collectors.toList());
```

**Effort:** 1-2 days  
**Impact:** 20-50% additional speedup for sparse deletes  
**Risk:** Low (filtering is simple)

### Priority 2: Important for Optimal Performance (Phase 7)

**4. Implement Smart Algorithm Selector**

Create `RemappingAlgorithmSelector` class from planning documents:
- Estimates delete count from iterable
- Detects sortedness via sampling
- Calculates gap ratio from runs
- Applies decision tree to select optimal strategy

**Effort:** 5-7 days  
**Impact:** 2-10x additional speedup by choosing optimal algorithm  
**Risk:** Medium (needs careful tuning)

**5. Add JMH Performance Benchmarks**

Create benchmark suite:
- Benchmark all strategies across parameter ranges
- Validate claimed speedups (100-10,000x)
- Tune algorithm selection thresholds
- Generate performance report for documentation

**Effort:** 3-5 days  
**Impact:** High confidence in performance claims  
**Risk:** Low (benchmarking only)

### Priority 3: Nice to Have (Future Work)

**6. Streaming API for Memory Efficiency**

Current bulk APIs load all positions into memory. For very large DVs (10M+ positions), use streaming:

```java
public CloseableIterable<PositionDelete<?>> remapDeletesStreaming(
    FileMapping mapping,
    CloseableIterable<PositionDelete<?>> deletes) {
  // Return iterator that remaps on-the-fly
  return new RemappingIterable(mapping, deletes);
}
```

**Effort:** 5-7 days  
**Impact:** Enables remapping of arbitrarily large delete files  
**Risk:** Medium (streaming adds complexity)

**7. RoaringBitmap Integration**

For deletion vectors with dense position sets:

```java
// Use RoaringBitmap for efficient position storage
RoaringBitmap positions = readDVAsRoaringBitmap(dvFile);

// Remap using batch operations
RoaringBitmap remapped = remapPositionsWithRoaring(positions, mapping);

// Write as new DV
writeDVFromRoaringBitmap(remapped, targetFile);
```

**Effort:** 7-10 days  
**Impact:** 10-100x memory reduction for large DVs  
**Risk:** Medium (new dependency, integration complexity)

## Implementation Roadmap

### Phase 6: Production Integration (2-3 weeks)

**Week 1:**
- Day 1-2: Implement bulk API usage in PositionDeleteRemapper
- Day 3-4: Add predicate pushdown to bulk strategies
- Day 5: Code review and refinement

**Week 2:**
- Day 1-3: Write integration tests for bulk strategies
- Day 4-5: Performance testing and validation

**Week 3:**
- Day 1-2: Documentation updates
- Day 3-4: Production rollout preparation
- Day 5: Release

### Phase 7: Optimal Performance (3-4 weeks)

**Week 1-2:**
- Implement smart algorithm selector
- Tune thresholds based on testing
- Add configuration properties

**Week 3:**
- Create JMH benchmark suite
- Run benchmarks across parameter space
- Generate performance report

**Week 4:**
- Documentation and knowledge transfer
- Production monitoring setup
- Final release

## Success Metrics

### Must Have (Phase 6 Complete)
- ✅ Bulk API integrated into remapDV()
- ✅ Integration tests passing for bulk strategies
- ✅ Predicate pushdown implemented
- ✅ 5-10x speedup for DV remapping (measured)

### Should Have (Phase 7 Complete)
- ✅ Smart algorithm selector implemented
- ✅ JMH benchmarks running
- ✅ 10-100x speedup validated for optimal scenarios
- ✅ Configuration properties documented

### Nice to Have (Future)
- ✅ Streaming API implemented
- ✅ RoaringBitmap integration
- ✅ Production telemetry showing usage patterns

## Risk Assessment

### Low Risk Items
- Bulk API integration (APIs already tested)
- Predicate pushdown (simple filtering)
- Integration tests (testing only)
- JMH benchmarks (no production impact)

### Medium Risk Items
- Smart algorithm selector (needs careful tuning)
- Streaming API (complexity in error handling)
- RoaringBitmap integration (new dependency)

### Mitigation Strategies
- Feature flags for new algorithms
- Extensive testing before production
- Gradual rollout with monitoring
- Fallback to proven algorithms on errors

## Conclusion

The remapping optimization implementation has **solid foundations** (algorithms, unit tests) but **critical integration gaps** prevent it from delivering value in production:

1. **Bulk strategies exist but are never used** - remapDV() iterates one-by-one
2. **Algorithm selector can't make optimal decisions** - lacks information about n, sortedness, gaps
3. **No predicate pushdown** - processes all runs even when irrelevant
4. **No integration tests** - bulk strategies untested in realistic scenarios
5. **No performance validation** - claimed speedups unverified

**Recommendation:** Prioritize Phase 6 (Production Integration) to unlock value from existing work. Phase 7 (Optimal Performance) can follow based on production telemetry.

**Estimated Timeline:**
- Phase 6: 2-3 weeks → 5-10x speedup
- Phase 7: 3-4 weeks → 10-100x speedup for optimal scenarios
- Total: 5-7 weeks to full optimization

**Expected Impact:**
- Enables compaction maps for large-scale workloads
- Removes performance bottleneck from adoption
- Positions Iceberg as best-in-class for conflict resolution

---

*Evaluation conducted: 2026-01-15*  
*Implementation status: Phases 1-5 complete, Phases 6-7 pending*
