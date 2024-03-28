# Running benchmarks

## Setup
Start a local standalone Spark cluster as docker containers:
with a single worker that has all available resources
```shell
make run-d
```

Stop the Spark cluster
```shell
make down
```

## Generating data for loading

TPCDS with given scale factor
```shell
make tpcs sf=1
```

TODO: create script for data generation
```
cd /home/spark/tpcds-kit/tools
mkdir /opt/spark/data/sf_XXX
./dsdgen -param ~/lst-bench/conf/tpcds_generation/data_gen_sf1.conf
```

## Benchmarks

### Fast benchmark of iceberg

```shell
./launcher.sh -c conf/connections_config.yaml -t conf/telemetry_config.yaml -l run/spark-3.3.1/config/tpcds/library.yaml -w conf/fast_test_iceberg/workload_config.yaml -e conf/fast_test_iceberg/experiment_config.yaml
```

# Running basic Spark
You can run the spark standalone cluster by running:
```shell
make run
```
or with 3 workers using:
```shell
make run-scaled
```

There are a number of commands to build the standalone cluster,
you should check the Makefile to see them all. But the
simplest one is:
```shell
make build
```

## Web UIs
The master node can be accessed on:
`localhost:9090`. 
The spark history server is accessible through:
`localhost:18080`.

# TODOs

- Provide persistence for raw data and metastore via volumes if required

# About the repo

It is forked from: ...