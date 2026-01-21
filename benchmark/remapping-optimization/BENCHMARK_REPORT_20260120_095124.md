# Remapping Algorithm Benchmark Report

**Generated:** 2026-01-20 12:06:31
**Benchmark Timestamp:** 20260120_095124
**Mode:** Full benchmark suite

## Overview

This report contains performance benchmarks for the remapping algorithm strategies
used in compaction map position delete remapping.

### Strategies Benchmarked

| Strategy | Complexity | Best For |
|----------|------------|----------|
| LinearSearch | O(n × m) | Baseline comparison |
| BinarySearch | O(n × log m) | General purpose |
| IntervalTree | O(n × log m) | Unsorted positions |
| StreamJoin | O(n + m) | Sorted positions, large m |
| RangeQuery | O(m × log n) | Few runs (small m) |
| SmartSelector | Varies | Automatic optimal selection |

### Parameters

- **numRuns (m):** 10, 100, 1000 - Number of runs in compaction map
- **numPositions (n):** 1000, 10000, 100000 - Number of positions to remap
- **gapRatio:** 0.0 (dense), 0.3 (moderate), 0.5 (sparse)
- **sorted:** true/false - Whether input positions are sorted

## Results Summary

```
Benchmark                                  (gapRatio)  (numPositions)  (numRuns)  (sorted)  Mode  Cnt      Score      Error  Units
RemappingAlgorithmBenchmark.binarySearch          0.0            1000         10      true  avgt    5     23.341 ±    3.563  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0            1000         10     false  avgt    5     14.584 ±    1.597  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0            1000        100      true  avgt    5     32.840 ±    7.290  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0            1000        100     false  avgt    5     25.015 ±    1.113  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0            1000       1000      true  avgt    5     43.669 ±    3.790  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0            1000       1000     false  avgt    5     39.307 ±    1.749  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000         10      true  avgt    5    156.745 ±    5.954  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000         10     false  avgt    5    146.329 ±   28.255  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000        100      true  avgt    5    384.628 ±   66.084  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000        100     false  avgt    5    243.286 ±   44.879  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000       1000      true  avgt    5    495.335 ±   44.859  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000       1000     false  avgt    5    345.794 ±   21.271  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000         10      true  avgt    5   1586.505 ±  399.599  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000         10     false  avgt    5   1658.169 ±  138.493  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000        100      true  avgt    5   2773.614 ±  594.788  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000        100     false  avgt    5   2744.148 ±  565.573  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000       1000      true  avgt    5   6114.952 ± 1535.502  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000       1000     false  avgt    5   3605.689 ±  608.270  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000         10      true  avgt    5     21.398 ±    2.216  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000         10     false  avgt    5     15.161 ±    0.564  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000        100      true  avgt    5     27.062 ±    2.805  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000        100     false  avgt    5     23.551 ±    1.086  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000       1000      true  avgt    5     39.151 ±    4.652  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000       1000     false  avgt    5     38.344 ±    6.396  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000         10      true  avgt    5    145.402 ±   27.260  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000         10     false  avgt    5    141.987 ±   24.346  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000        100      true  avgt    5    295.835 ±    7.107  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000        100     false  avgt    5    232.100 ±   37.709  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000       1000      true  avgt    5    398.764 ±   17.717  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000       1000     false  avgt    5    347.237 ±   26.978  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000         10      true  avgt    5   1576.245 ±  272.318  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000         10     false  avgt    5   1461.691 ±  571.198  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000        100      true  avgt    5   2640.051 ±  303.120  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000        100     false  avgt    5   2473.733 ±  817.107  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000       1000      true  avgt    5   4978.412 ± 1003.035  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000       1000     false  avgt    5   3533.323 ±  404.284  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000         10      true  avgt    5     16.805 ±    1.369  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000         10     false  avgt    5     14.210 ±    1.403  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000        100      true  avgt    5     25.024 ±    1.486  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000        100     false  avgt    5     23.574 ±    1.045  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000       1000      true  avgt    5     36.470 ±    5.514  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000       1000     false  avgt    5     37.183 ±    2.904  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000         10      true  avgt    5    152.748 ±   24.348  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000         10     false  avgt    5    143.493 ±   18.978  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000        100      true  avgt    5    258.827 ±   62.011  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000        100     false  avgt    5    239.423 ±   52.346  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000       1000      true  avgt    5    354.200 ±   25.716  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000       1000     false  avgt    5    343.030 ±   34.918  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000         10      true  avgt    5   1456.934 ±  183.819  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000         10     false  avgt    5   1449.060 ±  216.606  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000        100      true  avgt    5   2472.129 ±  505.331  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000        100     false  avgt    5   2317.193 ±  421.230  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000       1000      true  avgt    5   4100.954 ±  262.568  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000       1000     false  avgt    5   3435.070 ±  215.680  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000         10      true  avgt    5     20.881 ±    2.117  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000         10     false  avgt    5      5.383 ±    0.219  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000        100      true  avgt    5     30.617 ±    4.457  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000        100     false  avgt    5      7.721 ±    1.555  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000       1000      true  avgt    5     51.791 ±    3.058  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000       1000     false  avgt    5     39.159 ±    5.235  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000         10      true  avgt    5     68.008 ±   16.892  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000         10     false  avgt    5     54.335 ±   12.243  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000        100      true  avgt    5    318.887 ±   36.537  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000        100     false  avgt    5     48.573 ±    7.100  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000       1000      true  avgt    5    449.687 ±  225.492  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000       1000     false  avgt    5     59.829 ±   12.318  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000         10      true  avgt    5    456.709 ±   97.756  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000         10     false  avgt    5    534.951 ±   33.491  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000        100      true  avgt    5    608.527 ±  137.561  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000        100     false  avgt    5    481.563 ±   54.091  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000       1000      true  avgt    5   3567.608 ±  864.818  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000       1000     false  avgt    5    530.457 ±   82.865  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000         10      true  avgt    5     11.530 ±    1.979  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000         10     false  avgt    5      3.288 ±    0.241  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000        100      true  avgt    5     16.426 ±    0.847  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000        100     false  avgt    5      5.552 ±    0.896  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000       1000      true  avgt    5     31.213 ±    2.559  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000       1000     false  avgt    5     29.599 ±    6.770  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000         10      true  avgt    5     46.629 ±    8.947  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000         10     false  avgt    5     36.077 ±    3.445  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000        100      true  avgt    5    181.909 ±   54.141  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000        100     false  avgt    5     34.006 ±    1.361  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000       1000      true  avgt    5    261.402 ±   30.939  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000       1000     false  avgt    5     74.055 ±   24.789  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000         10      true  avgt    5    441.004 ±   86.683  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000         10     false  avgt    5    455.209 ±  109.063  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000        100      true  avgt    5    667.630 ±   88.611  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000        100     false  avgt    5    475.333 ±  102.921  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000       1000      true  avgt    5   3225.951 ±  580.589  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000       1000     false  avgt    5    530.431 ±  238.889  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000         10      true  avgt    5     10.680 ±    1.279  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000         10     false  avgt    5      3.838 ±    0.266  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000        100      true  avgt    5     15.849 ±    0.892  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000        100     false  avgt    5      5.911 ±    0.656  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000       1000      true  avgt    5     31.312 ±    2.312  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000       1000     false  avgt    5     35.408 ±    2.025  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000         10      true  avgt    5     47.028 ±    7.886  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000         10     false  avgt    5     32.792 ±    5.077  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000        100      true  avgt    5    165.490 ±   33.386  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000        100     false  avgt    5     34.434 ±    3.823  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000       1000      true  avgt    5    239.887 ±   28.367  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000       1000     false  avgt    5     75.400 ±   12.086  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000         10      true  avgt    5    450.318 ±   70.219  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000         10     false  avgt    5    464.706 ±  136.033  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000        100      true  avgt    5    694.244 ±  122.910  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000        100     false  avgt    5    486.358 ±   87.587  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000       1000      true  avgt    5   2833.839 ±  401.108  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000       1000     false  avgt    5    564.460 ±  119.079  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000         10      true  avgt    5     15.982 ±    2.070  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000         10     false  avgt    5     12.092 ±    0.408  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000        100      true  avgt    5     17.364 ±    2.325  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000        100     false  avgt    5     60.293 ±   17.601  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000       1000      true  avgt    5     12.702 ±    0.844  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000       1000     false  avgt    5    467.870 ±   28.215  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000         10      true  avgt    5     93.694 ±    5.386  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000         10     false  avgt    5     88.890 ±    4.677  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000        100      true  avgt    5    356.622 ±   36.313  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000        100     false  avgt    5    465.288 ±   10.053  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000       1000      true  avgt    5    361.212 ±    5.459  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000       1000     false  avgt    5   5905.999 ±  437.544  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000         10      true  avgt    5    931.917 ±   16.124  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000         10     false  avgt    5    959.530 ±   71.454  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000        100      true  avgt    5   4627.671 ±  125.440  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000        100     false  avgt    5   4861.667 ±  229.053  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000       1000      true  avgt    5  33075.408 ± 2317.715  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000       1000     false  avgt    5  60656.599 ±  902.194  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000         10      true  avgt    5     10.165 ±    0.229  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000         10     false  avgt    5      9.391 ±    0.349  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000        100      true  avgt    5     18.820 ±    0.246  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000        100     false  avgt    5     46.604 ±    0.795  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000       1000      true  avgt    5    144.975 ±    3.497  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000       1000     false  avgt    5    439.622 ±   13.434  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000         10      true  avgt    5     93.111 ±    5.038  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000         10     false  avgt    5     88.961 ±    4.965  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000        100      true  avgt    5    331.819 ±    2.567  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000        100     false  avgt    5    465.229 ±    3.227  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000       1000      true  avgt    5   1608.306 ±   38.147  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000       1000     false  avgt    5   5920.780 ±  502.227  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000         10      true  avgt    5    917.283 ±   74.674  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000         10     false  avgt    5    921.350 ±   16.477  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000        100      true  avgt    5   4708.540 ±  402.965  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000        100     false  avgt    5   4825.588 ±  235.458  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000       1000      true  avgt    5  32154.738 ± 2433.914  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000       1000     false  avgt    5  62432.300 ± 3161.948  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000         10      true  avgt    5      9.541 ±    0.505  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000         10     false  avgt    5      9.357 ±    0.240  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000        100      true  avgt    5     23.376 ±    0.457  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000        100     false  avgt    5     47.188 ±    3.239  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000       1000      true  avgt    5    234.555 ±    5.386  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000       1000     false  avgt    5    414.338 ±   26.342  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000         10      true  avgt    5     93.484 ±    4.746  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000         10     false  avgt    5     89.076 ±    7.109  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000        100      true  avgt    5    355.184 ±   27.751  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000        100     false  avgt    5    469.243 ±   31.179  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000       1000      true  avgt    5   2413.244 ±  102.372  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000       1000     false  avgt    5   8619.174 ± 4311.953  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000         10      true  avgt    5   1272.480 ±  365.566  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000         10     false  avgt    5   1284.652 ±  134.786  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000        100      true  avgt    5   6851.317 ±  845.322  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000        100     false  avgt    5   7295.434 ± 1660.757  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000       1000      true  avgt    5  47359.759 ± 3281.267  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000       1000     false  avgt    5  79518.929 ± 4820.225  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000         10      true  avgt    5     11.344 ±    3.840  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000         10     false  avgt    5     44.705 ±    4.526  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000        100      true  avgt    5     11.337 ±    0.228  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000        100     false  avgt    5     48.268 ±    3.085  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000       1000      true  avgt    5     14.129 ±    1.048  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000       1000     false  avgt    5    104.223 ±    7.848  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000         10      true  avgt    5     25.314 ±    0.582  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000         10     false  avgt    5   1086.277 ±   47.684  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000        100      true  avgt    5    131.766 ±   26.675  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000        100     false  avgt    5   1104.487 ±   98.947  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000       1000      true  avgt    5    137.370 ±   17.881  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000       1000     false  avgt    5   1220.258 ±   78.004  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000         10      true  avgt    5    271.764 ±   94.059  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000         10     false  avgt    5  12053.095 ± 1566.458  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000        100      true  avgt    5    323.000 ±   27.076  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000        100     false  avgt    5  12619.993 ± 1711.819  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000       1000      true  avgt    5   1958.517 ±  320.110  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000       1000     false  avgt    5  11926.562 ±  801.985  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000         10      true  avgt    5      8.885 ±    1.753  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000         10     false  avgt    5     45.686 ±    1.990  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000        100      true  avgt    5      9.071 ±    1.519  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000        100     false  avgt    5     49.395 ±    4.592  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000       1000      true  avgt    5     10.898 ±    0.725  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000       1000     false  avgt    5    104.728 ±    5.409  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000         10      true  avgt    5     25.577 ±    1.367  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000         10     false  avgt    5   1087.257 ±   57.199  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000        100      true  avgt    5     91.061 ±   12.806  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000        100     false  avgt    5   1089.599 ±   39.210  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000       1000      true  avgt    5     92.650 ±   18.557  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000       1000     false  avgt    5   1226.220 ±   40.933  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000         10      true  avgt    5    249.550 ±   56.015  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000         10     false  avgt    5  12040.759 ± 1188.973  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000        100      true  avgt    5    326.181 ±   21.548  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000        100     false  avgt    5  12141.373 ±  943.083  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000       1000      true  avgt    5   1451.867 ±  200.209  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000       1000     false  avgt    5  12558.447 ± 1225.224  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000         10      true  avgt    5      6.550 ±    0.385  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000         10     false  avgt    5     43.906 ±    2.253  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000        100      true  avgt    5      6.824 ±    1.170  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000        100     false  avgt    5     49.858 ±    4.250  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000       1000      true  avgt    5      9.164 ±    1.285  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000       1000     false  avgt    5    110.727 ±    8.373  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000         10      true  avgt    5     26.107 ±    1.050  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000         10     false  avgt    5   1122.363 ±   43.962  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000        100      true  avgt    5     68.756 ±   14.237  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000        100     false  avgt    5   1105.964 ±   63.829  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000       1000      true  avgt    5     71.227 ±    7.305  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000       1000     false  avgt    5   1209.499 ±   76.186  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000         10      true  avgt    5    245.421 ±   20.607  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000         10     false  avgt    5  11922.729 ±  576.967  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000        100      true  avgt    5    345.580 ±   50.345  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000        100     false  avgt    5  12563.988 ± 1060.122  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000       1000      true  avgt    5   1101.394 ±  275.611  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000       1000     false  avgt    5  13290.578 ±  801.255  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000         10      true  avgt    5     11.898 ±    1.653  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000         10     false  avgt    5      8.891 ±    0.540  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000        100      true  avgt    5     18.031 ±    1.645  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000        100     false  avgt    5      4.941 ±    0.433  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000       1000      true  avgt    5     32.126 ±    6.148  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000       1000     false  avgt    5     23.645 ±    2.797  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000         10      true  avgt    5     27.651 ±    0.797  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000         10     false  avgt    5     85.059 ±    5.009  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000        100      true  avgt    5    217.569 ±   44.976  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000        100     false  avgt    5     36.297 ±    9.100  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000       1000      true  avgt    5    287.860 ±   31.864  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000       1000     false  avgt    5     58.683 ±   17.019  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000         10      true  avgt    5    304.137 ±  403.378  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000         10     false  avgt    5    954.521 ±  142.292  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000        100      true  avgt    5    682.175 ±   46.647  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000        100     false  avgt    5    457.327 ±  121.235  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000       1000      true  avgt    5   3284.478 ±  340.628  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000       1000     false  avgt    5    623.214 ±   73.955  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000         10      true  avgt    5      9.570 ±    0.727  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000         10     false  avgt    5      7.750 ±    0.379  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000        100      true  avgt    5     16.922 ±    2.171  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000        100     false  avgt    5      5.229 ±    0.658  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000       1000      true  avgt    5     31.155 ±    5.847  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000       1000     false  avgt    5     29.537 ±    5.167  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000         10      true  avgt    5     29.780 ±    1.189  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000         10     false  avgt    5     87.494 ±    5.607  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000        100      true  avgt    5    175.684 ±   17.263  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000        100     false  avgt    5     35.067 ±    6.621  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000       1000      true  avgt    5    246.394 ±   33.115  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000       1000     false  avgt    5     62.211 ±   10.614  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000         10      true  avgt    5    269.874 ±   75.897  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000         10     false  avgt    5    918.435 ±   97.556  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000        100      true  avgt    5    594.751 ±  102.964  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000        100     false  avgt    5    447.088 ±   60.145  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000       1000      true  avgt    5   3201.923 ± 1221.200  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000       1000     false  avgt    5    491.769 ±  140.494  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000         10      true  avgt    5      8.002 ±    0.228  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000         10     false  avgt    5      8.981 ±    0.482  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000        100      true  avgt    5     15.135 ±    0.949  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000        100     false  avgt    5      5.347 ±    0.320  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000       1000      true  avgt    5     29.838 ±    2.345  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000       1000     false  avgt    5     37.974 ±    4.368  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000         10      true  avgt    5     27.219 ±    0.600  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000         10     false  avgt    5     36.799 ±   28.514  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000        100      true  avgt    5    163.849 ±   33.797  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000        100     false  avgt    5     33.818 ±    5.293  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000       1000      true  avgt    5    232.297 ±   27.515  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000       1000     false  avgt    5     76.903 ±    9.923  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000         10      true  avgt    5    279.012 ±  144.878  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000         10     false  avgt    5    436.511 ±   91.035  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000        100      true  avgt    5    341.178 ±   18.019  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000        100     false  avgt    5    615.606 ±   12.658  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000       1000      true  avgt    5   2924.064 ±  448.888  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000       1000     false  avgt    5    539.631 ±  118.538  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000         10      true  avgt    5     16.397 ±   37.425  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000         10     false  avgt    5      9.566 ±    0.711  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000        100      true  avgt    5     12.663 ±    2.054  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000        100     false  avgt    5     14.748 ±    0.853  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000       1000      true  avgt    5     14.569 ±    3.236  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000       1000     false  avgt    5     23.845 ±    1.288  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000         10      true  avgt    5     25.754 ±    3.042  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000         10     false  avgt    5     90.568 ±    8.050  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000        100      true  avgt    5    123.810 ±   32.448  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000        100     false  avgt    5    148.493 ±   24.219  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000       1000      true  avgt    5    134.829 ±   18.451  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000       1000     false  avgt    5    217.767 ±    7.210  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000         10      true  avgt    5    247.941 ±   18.492  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000         10     false  avgt    5    981.791 ±   37.858  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000        100      true  avgt    5    333.650 ±   38.082  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000        100     false  avgt    5   1528.125 ±  603.468  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000       1000      true  avgt    5   1996.523 ±  787.081  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000       1000     false  avgt    5   2272.062 ±  380.197  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000         10      true  avgt    5      9.740 ±    0.873  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000         10     false  avgt    5      9.601 ±    0.558  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000        100      true  avgt    5      9.987 ±    0.611  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000        100     false  avgt    5     15.370 ±    0.472  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000       1000      true  avgt    5     12.974 ±    1.847  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000       1000     false  avgt    5     24.968 ±    1.555  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000         10      true  avgt    5     26.412 ±    2.410  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000         10     false  avgt    5     92.912 ±   17.574  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000        100      true  avgt    5    109.253 ±   34.534  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000        100     false  avgt    5    157.494 ±   21.458  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000       1000      true  avgt    5    115.647 ±   24.844  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000       1000     false  avgt    5    230.053 ±    7.974  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000         10      true  avgt    5    261.859 ±   54.620  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000         10     false  avgt    5    940.241 ±  100.414  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000        100      true  avgt    5    332.460 ±   45.417  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000        100     false  avgt    5   1508.917 ±  357.158  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000       1000      true  avgt    5   1363.081 ±  367.509  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000       1000     false  avgt    5   2111.054 ±  560.667  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000         10      true  avgt    5      7.253 ±    0.276  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000         10     false  avgt    5      9.421 ±    0.939  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000        100      true  avgt    5      8.219 ±    1.219  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000        100     false  avgt    5     15.655 ±    6.206  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000       1000      true  avgt    5     10.744 ±    1.488  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000       1000     false  avgt    5     25.106 ±    1.277  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000         10      true  avgt    5     28.089 ±    4.647  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000         10     false  avgt    5     97.928 ±   12.097  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000        100      true  avgt    5     91.078 ±   20.129  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000        100     false  avgt    5    153.770 ±   32.569  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000       1000      true  avgt    5     91.147 ±   21.119  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000       1000     false  avgt    5    234.701 ±   19.403  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000         10      true  avgt    5    282.872 ±  185.112  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000         10     false  avgt    5    945.439 ±  127.502  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000        100      true  avgt    5    373.768 ±  152.007  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000        100     false  avgt    5   1620.408 ±  439.656  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000       1000      true  avgt    5   1202.332 ±  432.639  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000       1000     false  avgt    5   2289.677 ±  381.956  us/op

Benchmark result is saved to /home/chris/work/iceberg/core/build/reports/jmh/results.json
```

## Visualizations

### Strategy Comparison
![Strategy Comparison](chart_strategy_comparison.png)

### Smart Selector Overhead
![Selector Overhead](chart_selector_overhead.png)

### Speedup vs Linear Search
![Speedup vs Linear](chart_speedup_vs_linear.png)

## Key Findings

### Smart Selector Performance

**WARNING:** The smart selector shows significant overhead in this benchmark run.

- Average overhead: **56%** (expected: <10%)
- Maximum overhead: **226%**
- High overhead cases: 33 out of 54 scenarios

The selector is suboptimal for `m=1000` sorted cases where it defaults to IntervalTree,
but RangeQuery or StreamJoin are actually faster.

### Strategy Selection Rules (Current)

1. **Few runs (m < 10):**
   - Sorted → RangeQuery
   - Unsorted → BinarySearch

2. **High fan-in (n/m > 100) with gaps:**
   - Sorted → RangeQuery
   - Unsorted → IntervalTree

3. **Medium runs (m < 100):**
   - Sorted with n > m → StreamJoin
   - Otherwise → BinarySearch

4. **Many runs (m ≥ 100):**
   - IntervalTree (currently used, but benchmarks show RangeQuery/StreamJoin often better for sorted)

## Files Generated

- `results_20260120_095124.txt` - Full benchmark output
- `results_20260120_095124.json` - JSON results for programmatic analysis
- `results_20260120_095124.csv` - CSV for spreadsheet analysis

---
*Report generated 2026-01-20 12:06:32*
