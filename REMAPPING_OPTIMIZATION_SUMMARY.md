# Remapping Algorithm Optimization - Executive Summary

## The Problem

Current `PositionDeleteRemapper` uses O(n*m) algorithm:
- **n** = number of position deletes to remap
- **m** = number of runs in compaction map

For production workloads this is a **critical bottleneck**:
```
1M deletes × 10K runs = 10 billion operations ≈ 100 seconds per delete file
```

## Proposed Solution

**Phase 1-2: Binary Search** (Quick Win - Week 1-2)
- Replace linear scan with binary search
- **Complexity:** O(n * log m)
- **Speedup:** 100-1000x
- **Risk:** Low (simple change)
- **Effort:** 1-2 weeks

**Phase 3-4: Interval Tree + Stream Join** (Optimal - Week 3-6)
- Interval tree for sparse deletes: O(n * log m)
- Stream-based join for sorted deletes: O(n + m)
- **Speedup:** 1,000-10,000x
- **Risk:** Medium (new data structures)
- **Effort:** 4 weeks

**Phase 5-6: Range Filtering + Testing** (Production Ready - Week 7-9)
- Min/max filtering for memory efficiency
- Comprehensive test suite
- **Risk:** Low
- **Effort:** 3 weeks

## Performance Targets

| Workload | Current | Target | Speedup |
|----------|---------|--------|---------|
| Small (1K deletes, 100 runs) | 10ms | <10ms | 1x (no regression) |
| Medium (100K deletes, 1K runs) | 10s | <100ms | 100x |
| Large (1M deletes, 10K runs) | 100s | <1s | 1,000x |
| Production (10M deletes, 100K runs) | Hours | <10s | 10,000x |

## Implementation Roadmap

### Stage 1: Binary Search (Iceberg 1.7.0)
**Timeline:** 2 weeks
**Risk:** Low
**Deliverable:** 100x speedup, zero risk
```java
// GenericFileMapping.java - Replace linear with binary search
public Run runForPosition(long sourcePosition) {
  // Binary search: O(log m) instead of O(m)
  int left = 0, right = runs.length - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    // ... binary search logic ...
  }
}
```

### Stage 2: Interval Tree (Iceberg 1.8.0)
**Timeline:** 4 weeks
**Risk:** Medium
**Deliverable:** 1000x speedup, overlap detection
```java
// IntervalRunIndex.java - New index structure
public class IntervalRunIndex {
  private final RangeMap<Long, Run> intervalTree;

  public Run runForPosition(long position) {
    return intervalTree.get(position); // O(log m)
  }
}
```

### Stage 3: Stream Join (Iceberg 1.9.0)
**Timeline:** 3 weeks
**Risk:** Medium
**Deliverable:** 10,000x speedup, optimal algorithm
```java
// PositionDeleteRemapper.java - New batch API
public CloseableIterable<PositionDelete<?>> remapDeletesBatch(
    String sourceFile,
    CloseableIterable<PositionDelete<?>> deletes) {
  // Stream-based join: O(n + m) when sorted
  return new StreamBasedRemappingIterable(mapping, deletes);
}
```

## Key Design Decisions

### 1. Algorithm Selection Strategy
```
if (runs.length < 100) {
  use binary_search()  // Simple, fast for small m
} else if (deletes_sorted && n > m) {
  use stream_join()    // Optimal O(n + m)
} else {
  use interval_tree()  // Good for any scenario
}
```

### 2. Built-In Validation
- **Overlapping Runs Detection:** Interval tree naturally catches corrupted maps
- **Gap Detection:** Identifies positions not in any run (filtered rows)
- **Coverage Metrics:** Tracks remapping success rate

### 3. Memory Efficiency
- **Lazy Construction:** Build index only when needed
- **Range Filtering:** Load only relevant portions of map
- **Streaming:** Process deletes without loading all into memory

## Risk Mitigation

### Low-Risk First Step
**Binary Search** is a simple, safe change:
- No new dependencies
- Minimal code changes
- Easy to revert
- 100x speedup alone

### Feature Flags
```java
// Allow gradual rollout
write.compaction-map.use-interval-tree = true/false
write.compaction-map.use-stream-join = true/false
```

### Backwards Compatibility
- No changes to CompactionMap file format
- Existing APIs unchanged
- New batch APIs are additive

## Success Criteria

### Must Have (Phase 1-2)
✅ Binary search implementation
✅ 100x speedup for medium workloads
✅ Zero correctness regressions
✅ Unit tests with 100% coverage

### Should Have (Phase 3-4)
✅ Interval tree for large workloads
✅ 1000x speedup for production scenarios
✅ Overlap detection and validation
✅ Integration tests with real workloads

### Nice to Have (Phase 5-6)
✅ Stream-based join for optimal performance
✅ Range filtering for memory efficiency
✅ 10,000x speedup for sorted deletes
✅ Performance benchmarks and profiling

## Comparable Optimizations in Iceberg

### Similar Pattern: Manifest Index
**Problem:** Scanning all manifests to find relevant data files
**Solution:** Min/max index for partition/column values
**Result:** Skip irrelevant manifests entirely

### Similar Pattern: Deletion Vector Index
**Problem:** Loading all DVs to check if row is deleted
**Solution:** Bitmap index for fast membership testing
**Result:** O(1) deletion checks instead of O(n) scan

### This Optimization
**Problem:** Scanning all runs to find position mapping
**Solution:** Interval tree index for fast interval queries
**Result:** O(log m) lookup instead of O(m) scan

## Resource Requirements

### Development
- **Phase 1-2:** 1 engineer, 2 weeks (binary search)
- **Phase 3-4:** 1 engineer, 4 weeks (interval tree + stream join)
- **Phase 5-6:** 1 engineer, 3 weeks (testing + validation)
- **Total:** 1 engineer, 9 weeks

### Dependencies
- Guava RangeMap (already a dependency)
- JMH for benchmarking (already available)
- No new external dependencies

### Testing Infrastructure
- JMH benchmark suite: ~2 days
- Property-based tests: ~3 days
- Integration tests: ~5 days
- Total testing time: ~2 weeks (included in Phase 6)

## Alternatives Considered

### Option A: Do Nothing
- ✅ Zero effort
- ❌ Performance unacceptable for large workloads
- ❌ Limits adoption of compaction maps

### Option B: Batch-Only Optimization
- ✅ Simpler implementation
- ❌ Doesn't help single-delete API
- ❌ Still O(n*m) for individual remaps

### Option C: Pre-Computed Index in Map File
- ✅ Avoid runtime index building
- ❌ Increases map file size
- ❌ Adds complexity to map generation
- ❌ Can be added later if needed

### Chosen: Phased Optimization
- ✅ Low-risk first step (binary search)
- ✅ Incremental improvements
- ✅ Can stop at any phase if sufficient
- ✅ Each phase adds value independently

## Next Steps

### Immediate (Week 1)
1. Create JMH benchmark suite
2. Profile current implementation
3. Establish baseline metrics

### Short-Term (Week 2-4)
1. Implement binary search
2. Add unit tests
3. Merge to main branch

### Medium-Term (Week 5-8)
1. Implement interval tree
2. Add overlap detection
3. Feature flag rollout

### Long-Term (Week 9+)
1. Stream-based join
2. Range filtering
3. Performance validation

## Questions to Answer Before Starting

1. **What is the typical n:m ratio in production?**
   - Need real-world profiling to choose optimal algorithm

2. **Are runs typically sorted?**
   - Affects whether we can use stream-based join

3. **What percentage of deletes map to gaps?**
   - Affects expected performance characteristics

4. **What are memory constraints?**
   - Determines whether interval tree is viable

5. **Can we assume deletes are sorted?**
   - Determines whether stream join is practical

## Conclusion

**Recommendation:** Proceed with phased approach

**Why:**
- Low-risk first step (binary search) provides 100x speedup
- Each phase independently valuable
- Can assess need for additional phases based on real-world performance
- Total 9-week effort for complete optimization

**Expected Impact:**
- Enables compaction maps for large-scale production workloads
- Removes performance bottleneck from adoption
- Positions Iceberg as having best-in-class conflict resolution

---

*For detailed implementation plan, see: REMAPPING_OPTIMIZATION_PLAN.md*
*For current performance analysis, see: CLAUDE.md (Section: Token-Saving Strategies)*
