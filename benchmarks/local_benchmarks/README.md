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

TPCDS with given scale factor X and Y maintenance streams
```shell
make tpcds-gen SF=X MTS=Y
```

## Benchmarks using lst-bench

### Fast benchmark of iceberg

Run a short tpcds with scale factor 1: `make fast-tpcds`

__TODO__: Continue setup of tpcds / lst-bench

### Notes on the benchmark configuration

#### Catalogs
Currently Spark is configured with hadoop file based catalogs in spark-defaults.conf.
These catalogs can be used in the experiments.yaml's. Later we can setup further catalogs to use object stores, ...

#### connection_config.yaml

The connection_config specifies the connection to the local Spark Cluster and the Spark configuration.
It specifies the catalogs and many other Spark configuration. It can be used for all local experiments.

Alternatively, JDBC connections can be configured. Also several concurrent connections can be configured.

#### library.yaml's

Each type of workload (e.g. tpcsd) and cluster setup requires their distict library.yaml.
These libraries implement the tasks of the workloads that can later be used in the workload.yaml.
The library spark/tpcds/library.yaml is setup to work with file based catalogs.

#### workload.yaml's

Workload.yaml's specify the sequence of workload steps (tasks, sessions, ...).
These can contain parameters to be specified in a experiment.yaml

#### experiment.yaml's

experiment.yaml's specifiy the parameters, e.g., the catalogs to be used.

#### telemetry_config.yaml
 telemetry_config should be configured per experiment: Give the telemetry database file a unique name

#### Debugging of setup

Logging:
- Turn on detailed logging of thrift by commenting in spark-default.conf the line `spark.sql.thriftServer.log.level`
- Rebuild the image: `make build`
- Start the Spark cluster in foreground: `make run`

Attach to the master: `make attach`

Attach to Thrift SQL: `make attach-sql`

# Running basic Spark
You can run the spark standalone cluster by running:
```shell
make run
```
or with 3 workers using:
```shell
make run-scaled
```

- Build the image: `make build`
- Delete the Spark Cluster: `make down`
- Attach to the master: `make attach`
- Open a SQL session: `make attach-sql`
- Attach to any container: `docker exec -it CONTAINER bash`
- See Makefile

## Web UIs
The master node can be accessed on:
`localhost:9090`. 
The spark history server is accessible through:
`localhost:18080`.

# TODOs

- TPCDS: Setup query execution and dependent maitenance
- Provide persistence for raw data and metastore via volumes if required
- Implement verification of tpcds tables? The tpcds generator provides facilities.

# About the repo

It is forked from: ...