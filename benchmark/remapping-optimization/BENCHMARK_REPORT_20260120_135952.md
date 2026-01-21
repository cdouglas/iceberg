# Remapping Algorithm Benchmark Report

**Generated:** 2026-01-20 15:55:58

**Benchmark Timestamp:** 20260120_135952

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
RemappingAlgorithmBenchmark.binarySearch          0.0            1000         10      true  avgt    5     11.417 ±    0.726  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0            1000         10     false  avgt    5      7.058 ±    0.158  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0            1000        100      true  avgt    5     14.504 ±    0.264  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0            1000        100     false  avgt    5     11.718 ±    0.346  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0            1000       1000      true  avgt    5     20.338 ±    1.966  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0            1000       1000     false  avgt    5     19.041 ±    0.704  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000         10      true  avgt    5     76.664 ±    8.370  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000         10     false  avgt    5     69.980 ±    3.785  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000        100      true  avgt    5    158.269 ±   19.672  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000        100     false  avgt    5    108.860 ±   20.515  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000       1000      true  avgt    5    211.931 ±    2.683  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0           10000       1000     false  avgt    5    162.788 ±   17.247  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000         10      true  avgt    5    716.559 ±   19.907  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000         10     false  avgt    5    746.851 ±   80.122  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000        100      true  avgt    5   1283.117 ±  287.523  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000        100     false  avgt    5   1141.734 ±  124.649  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000       1000      true  avgt    5   2780.355 ±  236.345  us/op
RemappingAlgorithmBenchmark.binarySearch          0.0          100000       1000     false  avgt    5   1758.122 ±  563.310  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000         10      true  avgt    5     10.341 ±    0.762  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000         10     false  avgt    5      7.057 ±    0.238  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000        100      true  avgt    5     13.892 ±    1.314  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000        100     false  avgt    5     11.573 ±    0.442  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000       1000      true  avgt    5     18.490 ±    1.008  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3            1000       1000     false  avgt    5     18.309 ±    0.430  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000         10      true  avgt    5     77.478 ±    5.273  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000         10     false  avgt    5     72.482 ±   10.029  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000        100      true  avgt    5    150.783 ±   22.376  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000        100     false  avgt    5    113.371 ±   22.786  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000       1000      true  avgt    5    206.340 ±   26.137  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3           10000       1000     false  avgt    5    176.864 ±   23.997  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000         10      true  avgt    5    764.699 ±  130.170  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000         10     false  avgt    5    767.474 ±  117.213  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000        100      true  avgt    5   1375.408 ±  621.764  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000        100     false  avgt    5   1162.518 ±  153.352  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000       1000      true  avgt    5   2523.278 ±  385.189  us/op
RemappingAlgorithmBenchmark.binarySearch          0.3          100000       1000     false  avgt    5   1671.731 ±  222.324  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000         10      true  avgt    5      8.888 ±    0.528  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000         10     false  avgt    5      7.863 ±    3.835  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000        100      true  avgt    5     13.108 ±    1.030  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000        100     false  avgt    5     11.798 ±    1.048  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000       1000      true  avgt    5     19.091 ±    1.700  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5            1000       1000     false  avgt    5     18.793 ±    0.928  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000         10      true  avgt    5     77.644 ±   11.987  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000         10     false  avgt    5     82.010 ±   13.788  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000        100      true  avgt    5    139.241 ±   19.259  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000        100     false  avgt    5    115.160 ±   15.174  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000       1000      true  avgt    5    185.458 ±   13.129  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5           10000       1000     false  avgt    5    176.265 ±   16.410  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000         10      true  avgt    5    789.704 ±   64.124  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000         10     false  avgt    5    800.450 ±   85.049  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000        100      true  avgt    5   1300.729 ±   70.531  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000        100     false  avgt    5   1232.693 ±  150.140  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000       1000      true  avgt    5   2447.320 ±  321.904  us/op
RemappingAlgorithmBenchmark.binarySearch          0.5          100000       1000     false  avgt    5   1744.585 ±  214.032  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000         10      true  avgt    5     11.869 ±    1.727  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000         10     false  avgt    5      3.073 ±    0.894  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000        100      true  avgt    5     16.707 ±    3.278  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000        100     false  avgt    5      4.116 ±    0.346  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000       1000      true  avgt    5     25.113 ±    0.852  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0            1000       1000     false  avgt    5     19.253 ±    1.806  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000         10      true  avgt    5     36.541 ±    0.972  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000         10     false  avgt    5     30.047 ±    1.141  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000        100      true  avgt    5    158.658 ±   11.230  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000        100     false  avgt    5     28.808 ±    0.680  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000       1000      true  avgt    5    223.925 ±   23.029  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0           10000       1000     false  avgt    5     47.119 ±    1.957  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000         10      true  avgt    5    362.966 ±   13.122  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000         10     false  avgt    5    370.326 ±   39.604  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000        100      true  avgt    5    485.441 ±   43.883  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000        100     false  avgt    5    357.120 ±   14.980  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000       1000      true  avgt    5   2708.901 ±  243.219  us/op
RemappingAlgorithmBenchmark.intervalTree          0.0          100000       1000     false  avgt    5    384.807 ±   36.703  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000         10      true  avgt    5      9.434 ±    0.487  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000         10     false  avgt    5      3.227 ±    0.078  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000        100      true  avgt    5     13.754 ±    0.456  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000        100     false  avgt    5      4.458 ±    0.121  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000       1000      true  avgt    5     25.505 ±    1.001  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3            1000       1000     false  avgt    5     22.759 ±    0.794  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000         10      true  avgt    5     36.991 ±    0.475  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000         10     false  avgt    5     29.656 ±    1.274  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000        100      true  avgt    5    141.865 ±   11.070  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000        100     false  avgt    5     28.980 ±    0.633  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000       1000      true  avgt    5    193.955 ±    7.256  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3           10000       1000     false  avgt    5     49.478 ±    1.557  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000         10      true  avgt    5    353.692 ±   18.875  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000         10     false  avgt    5    352.119 ±   22.164  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000        100      true  avgt    5    502.701 ±   41.913  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000        100     false  avgt    5    353.088 ±   25.690  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000       1000      true  avgt    5   2394.779 ±  233.752  us/op
RemappingAlgorithmBenchmark.intervalTree          0.3          100000       1000     false  avgt    5    411.387 ±   35.349  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000         10      true  avgt    5      8.382 ±    1.253  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000         10     false  avgt    5      3.085 ±    0.112  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000        100      true  avgt    5     12.615 ±    0.495  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000        100     false  avgt    5      4.617 ±    0.402  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000       1000      true  avgt    5     24.714 ±    0.728  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5            1000       1000     false  avgt    5     29.995 ±    4.472  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000         10      true  avgt    5     40.064 ±    3.577  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000         10     false  avgt    5     42.939 ±    8.514  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000        100      true  avgt    5    282.123 ±   64.125  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000        100     false  avgt    5     45.708 ±   27.211  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000       1000      true  avgt    5    296.786 ±  258.594  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5           10000       1000     false  avgt    5    118.703 ±   36.507  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000         10      true  avgt    5   1001.274 ±  590.407  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000         10     false  avgt    5   1052.297 ±  110.569  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000        100      true  avgt    5   1386.694 ±  263.376  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000        100     false  avgt    5    707.599 ±  423.334  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000       1000      true  avgt    5   4380.269 ±  993.306  us/op
RemappingAlgorithmBenchmark.intervalTree          0.5          100000       1000     false  avgt    5    880.654 ±  531.689  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000         10      true  avgt    5     14.658 ±    3.144  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000         10     false  avgt    5      9.931 ±    0.726  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000        100      true  avgt    5     13.359 ±    0.806  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000        100     false  avgt    5     48.204 ±    1.736  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000       1000      true  avgt    5     13.196 ±    1.260  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0            1000       1000     false  avgt    5    463.970 ±   27.817  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000         10      true  avgt    5    112.180 ±   61.359  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000         10     false  avgt    5    114.651 ±   11.322  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000        100      true  avgt    5    454.613 ±   41.008  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000        100     false  avgt    5    608.643 ±   36.545  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000       1000      true  avgt    5    479.635 ±   38.871  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0           10000       1000     false  avgt    5   7636.097 ±  631.819  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000         10      true  avgt    5    914.307 ±   33.412  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000         10     false  avgt    5    940.786 ±   88.452  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000        100      true  avgt    5   4706.430 ±  212.757  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000        100     false  avgt    5   4885.381 ±  417.054  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000       1000      true  avgt    5  32671.704 ± 5561.170  us/op
RemappingAlgorithmBenchmark.linearSearch          0.0          100000       1000     false  avgt    5  61376.593 ± 2318.014  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000         10      true  avgt    5     10.211 ±    0.666  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000         10     false  avgt    5      9.288 ±    0.288  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000        100      true  avgt    5     18.738 ±    0.224  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000        100     false  avgt    5     46.149 ±    2.723  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000       1000      true  avgt    5    142.917 ±    5.683  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3            1000       1000     false  avgt    5    436.887 ±    8.265  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000         10      true  avgt    5     93.585 ±    8.177  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000         10     false  avgt    5     88.568 ±    6.571  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000        100      true  avgt    5    326.735 ±   14.235  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000        100     false  avgt    5    464.744 ±   31.022  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000       1000      true  avgt    5   1588.151 ±   21.691  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3           10000       1000     false  avgt    5   5837.546 ±  345.066  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000         10      true  avgt    5    915.487 ±   23.228  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000         10     false  avgt    5    904.437 ±   28.159  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000        100      true  avgt    5   4865.879 ±  578.491  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000        100     false  avgt    5   4849.345 ±  339.773  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000       1000      true  avgt    5  32114.922 ± 3533.477  us/op
RemappingAlgorithmBenchmark.linearSearch          0.3          100000       1000     false  avgt    5  61578.545 ± 4515.983  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000         10      true  avgt    5      9.696 ±    0.336  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000         10     false  avgt    5      9.183 ±    0.212  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000        100      true  avgt    5     23.538 ±    1.936  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000        100     false  avgt    5     46.189 ±    1.330  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000       1000      true  avgt    5    231.849 ±   11.279  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5            1000       1000     false  avgt    5    411.740 ±   19.077  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000         10      true  avgt    5     93.078 ±    6.583  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000         10     false  avgt    5     88.911 ±   11.507  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000        100      true  avgt    5    346.663 ±    8.044  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000        100     false  avgt    5    460.322 ±   10.947  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000       1000      true  avgt    5   2423.222 ±  123.979  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5           10000       1000     false  avgt    5   5811.113 ±  313.919  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000         10      true  avgt    5    912.012 ±   13.317  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000         10     false  avgt    5    937.616 ±   83.484  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000        100      true  avgt    5   4725.518 ±  107.156  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000        100     false  avgt    5   4752.883 ±  147.560  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000       1000      true  avgt    5  34942.683 ± 4265.513  us/op
RemappingAlgorithmBenchmark.linearSearch          0.5          100000       1000     false  avgt    5  61722.309 ± 1838.496  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000         10      true  avgt    5      8.446 ±    0.236  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000         10     false  avgt    5     33.577 ±    0.645  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000        100      true  avgt    5      9.035 ±    0.274  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000        100     false  avgt    5     36.350 ±    2.522  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000       1000      true  avgt    5     10.740 ±    0.268  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0            1000       1000     false  avgt    5     76.691 ±    4.104  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000         10      true  avgt    5     17.978 ±    0.960  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000         10     false  avgt    5    851.780 ±   24.914  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000        100      true  avgt    5     90.038 ±    4.882  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000        100     false  avgt    5    847.486 ±   24.597  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000       1000      true  avgt    5     95.926 ±    4.218  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0           10000       1000     false  avgt    5   1205.159 ±  542.818  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000         10      true  avgt    5    162.831 ±   43.397  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000         10     false  avgt    5   9290.947 ±  541.999  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000        100      true  avgt    5    221.898 ±   26.743  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000        100     false  avgt    5   9326.389 ±  333.150  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000       1000      true  avgt    5   1359.950 ±  230.139  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.0          100000       1000     false  avgt    5   9585.696 ± 1074.738  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000         10      true  avgt    5      6.609 ±    0.120  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000         10     false  avgt    5     34.034 ±    0.658  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000        100      true  avgt    5      6.932 ±    0.497  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000        100     false  avgt    5     37.583 ±    0.968  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000       1000      true  avgt    5      8.766 ±    0.201  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3            1000       1000     false  avgt    5     78.628 ±    4.243  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000         10      true  avgt    5     17.866 ±    2.249  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000         10     false  avgt    5    833.432 ±   21.155  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000        100      true  avgt    5     67.644 ±    7.315  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000        100     false  avgt    5    857.622 ±   44.403  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000       1000      true  avgt    5     68.458 ±    1.774  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3           10000       1000     false  avgt    5    933.530 ±   51.057  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000         10      true  avgt    5    145.795 ±   22.372  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000         10     false  avgt    5   9101.757 ±  628.591  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000        100      true  avgt    5    217.174 ±   33.246  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000        100     false  avgt    5   9412.658 ±  860.319  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000       1000      true  avgt    5   1064.312 ±  118.243  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.3          100000       1000     false  avgt    5   9281.336 ±  279.314  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000         10      true  avgt    5      5.155 ±    0.100  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000         10     false  avgt    5     33.782 ±    2.656  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000        100      true  avgt    5      5.348 ±    0.161  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000        100     false  avgt    5     37.085 ±    1.044  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000       1000      true  avgt    5      7.411 ±    0.544  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5            1000       1000     false  avgt    5     80.350 ±    1.927  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000         10      true  avgt    5     18.585 ±    0.430  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000         10     false  avgt    5    874.732 ±   65.260  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000        100      true  avgt    5     52.112 ±    0.783  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000        100     false  avgt    5    859.765 ±   25.416  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000       1000      true  avgt    5     54.198 ±    2.445  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5           10000       1000     false  avgt    5    949.530 ±   23.756  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000         10      true  avgt    5    140.673 ±   16.349  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000         10     false  avgt    5   9384.294 ± 1092.101  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000        100      true  avgt    5    219.903 ±   38.919  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000        100     false  avgt    5   9484.762 ±  691.277  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000       1000      true  avgt    5    792.317 ±   83.610  us/op
RemappingAlgorithmBenchmark.rangeQuery            0.5          100000       1000     false  avgt    5   9509.463 ±  717.151  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000         10      true  avgt    5      9.144 ±    0.641  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000         10     false  avgt    5      2.529 ±    0.070  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000        100      true  avgt    5      9.935 ±    0.149  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000        100     false  avgt    5      3.917 ±    0.171  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000       1000      true  avgt    5     13.235 ±    0.439  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0            1000       1000     false  avgt    5     18.499 ±    0.600  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000         10      true  avgt    5     20.619 ±    0.491  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000         10     false  avgt    5     28.991 ±    1.179  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000        100      true  avgt    5     93.924 ±   12.683  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000        100     false  avgt    5     27.224 ±    3.481  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000       1000      true  avgt    5    104.043 ±    8.603  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0           10000       1000     false  avgt    5     46.238 ±    4.657  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000         10      true  avgt    5    152.218 ±   10.285  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000         10     false  avgt    5    348.988 ±   13.311  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000        100      true  avgt    5    226.338 ±   24.016  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000        100     false  avgt    5    378.895 ±   70.493  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000       1000      true  avgt    5   1254.310 ±  154.883  us/op
RemappingAlgorithmBenchmark.smartSelector         0.0          100000       1000     false  avgt    5    395.979 ±   34.698  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000         10      true  avgt    5      7.240 ±    0.090  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000         10     false  avgt    5      2.999 ±    0.027  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000        100      true  avgt    5      7.741 ±    0.244  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000        100     false  avgt    5      4.250 ±    0.195  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000       1000      true  avgt    5     11.309 ±    1.023  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3            1000       1000     false  avgt    5     21.906 ±    0.435  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000         10      true  avgt    5     20.110 ±    2.386  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000         10     false  avgt    5     29.314 ±    3.802  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000        100      true  avgt    5     78.199 ±    3.602  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000        100     false  avgt    5     27.409 ±    0.520  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000       1000      true  avgt    5     83.636 ±    4.549  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3           10000       1000     false  avgt    5     49.019 ±    0.980  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000         10      true  avgt    5    147.023 ±    8.350  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000         10     false  avgt    5    557.884 ±   36.780  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000        100      true  avgt    5    320.092 ±   11.382  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000        100     false  avgt    5    349.506 ±   15.064  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000       1000      true  avgt    5   1046.813 ±  298.350  us/op
RemappingAlgorithmBenchmark.smartSelector         0.3          100000       1000     false  avgt    5    389.646 ±    7.498  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000         10      true  avgt    5      5.820 ±    0.145  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000         10     false  avgt    5      3.081 ±    0.078  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000        100      true  avgt    5      6.197 ±    0.133  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000        100     false  avgt    5      4.495 ±    0.278  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000       1000      true  avgt    5      9.819 ±    0.217  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5            1000       1000     false  avgt    5     27.014 ±    1.027  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000         10      true  avgt    5     20.830 ±    1.134  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000         10     false  avgt    5     25.264 ±    0.238  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000        100      true  avgt    5     54.257 ±    0.869  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000        100     false  avgt    5     27.953 ±    1.974  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000       1000      true  avgt    5     58.833 ±    4.287  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5           10000       1000     false  avgt    5     53.435 ±    1.426  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000         10      true  avgt    5    194.619 ±  197.461  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000         10     false  avgt    5    370.290 ±   17.412  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000        100      true  avgt    5    320.824 ±   24.837  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000        100     false  avgt    5    367.713 ±   33.123  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000       1000      true  avgt    5    788.313 ±   59.917  us/op
RemappingAlgorithmBenchmark.smartSelector         0.5          100000       1000     false  avgt    5    407.405 ±   44.765  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000         10      true  avgt    5      9.115 ±    0.126  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000         10     false  avgt    5      7.664 ±    0.201  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000        100      true  avgt    5      9.537 ±    1.092  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000        100     false  avgt    5     11.391 ±    0.231  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000       1000      true  avgt    5     11.243 ±    0.455  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0            1000       1000     false  avgt    5     18.606 ±    1.044  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000         10      true  avgt    5     18.497 ±    0.571  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000         10     false  avgt    5     70.470 ±    4.925  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000        100      true  avgt    5     92.447 ±    7.286  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000        100     false  avgt    5    114.079 ±   12.775  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000       1000      true  avgt    5    101.375 ±    5.112  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0           10000       1000     false  avgt    5    167.966 ±   10.640  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000         10      true  avgt    5    170.996 ±   61.392  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000         10     false  avgt    5    745.771 ±   72.141  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000        100      true  avgt    5    210.403 ±    8.413  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000        100     false  avgt    5   1172.428 ±  111.547  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000       1000      true  avgt    5   1259.419 ±  169.884  us/op
RemappingAlgorithmBenchmark.streamJoin            0.0          100000       1000     false  avgt    5   1596.540 ±  203.901  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000         10      true  avgt    5      7.209 ±    0.068  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000         10     false  avgt    5      7.752 ±    0.384  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000        100      true  avgt    5      7.379 ±    0.137  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000        100     false  avgt    5     11.342 ±    0.243  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000       1000      true  avgt    5      9.552 ±    0.571  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3            1000       1000     false  avgt    5     18.675 ±    0.515  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000         10      true  avgt    5     19.553 ±    0.252  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000         10     false  avgt    5     71.065 ±    6.070  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000        100      true  avgt    5     76.629 ±    8.874  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000        100     false  avgt    5    114.454 ±   15.777  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000       1000      true  avgt    5     78.216 ±    3.871  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3           10000       1000     false  avgt    5    171.238 ±   13.135  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000         10      true  avgt    5    151.126 ±   34.840  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000         10     false  avgt    5    746.212 ±   33.236  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000        100      true  avgt    5    225.959 ±    8.766  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000        100     false  avgt    5   1189.572 ±  140.639  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000       1000      true  avgt    5   1153.365 ±  111.864  us/op
RemappingAlgorithmBenchmark.streamJoin            0.3          100000       1000     false  avgt    5   1617.104 ±  204.786  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000         10      true  avgt    5      5.979 ±    0.331  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000         10     false  avgt    5      7.711 ±    0.227  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000        100      true  avgt    5      6.349 ±    0.150  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000        100     false  avgt    5     11.388 ±    0.697  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000       1000      true  avgt    5      8.105 ±    0.503  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5            1000       1000     false  avgt    5     19.235 ±    1.013  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000         10      true  avgt    5     20.935 ±    2.833  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000         10     false  avgt    5     72.961 ±    2.675  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000        100      true  avgt    5     63.338 ±    4.288  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000        100     false  avgt    5    115.096 ±   15.169  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000       1000      true  avgt    5     64.980 ±    5.009  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5           10000       1000     false  avgt    5    180.073 ±   21.397  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000         10      true  avgt    5    162.179 ±   12.543  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000         10     false  avgt    5    740.086 ±   61.341  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000        100      true  avgt    5    249.588 ±   28.408  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000        100     false  avgt    5   1190.215 ±  230.264  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000       1000      true  avgt    5    844.020 ±   75.765  us/op
RemappingAlgorithmBenchmark.streamJoin            0.5          100000       1000     false  avgt    5   1742.353 ±  296.163  us/op

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

The smart selector automatically chooses the optimal strategy based on:
- Number of runs (m)
- Number of positions (n)
- Whether positions are sorted
- Gap ratio (sparsity)

Expected overhead: **< 10%** compared to manually selecting the optimal strategy.

### Strategy Selection Rules

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
   - IntervalTree (always optimal)

## Files Generated

- `results_20260120_135952.txt` - Full benchmark output
- `results_20260120_135952.json` - JSON results for programmatic analysis
- `results_20260120_135952.csv` - CSV for spreadsheet analysis
- `chart_strategy_comparison.png` - Strategy performance comparison
- `chart_selector_overhead.png` - Smart selector overhead analysis
- `chart_speedup_vs_linear.png` - Speedup vs baseline

---
*Report generated by run_full_benchmark.sh*
