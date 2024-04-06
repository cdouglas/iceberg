build:
	docker-compose build

build-nc:
	docker-compose build --no-cache

build-progress:
	docker-compose build --no-cache --progress=plain

down:
	docker-compose down --volumes --remove-orphans

run: down build
	docker-compose up

run-scaled: build
	make down && docker-compose up --scale spark-worker=4

run-d: down
	docker-compose up -d

stop:
	docker-compose stop

submit:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

attach:
	docker exec -it da-spark-master bash

run-terminal: build
	docker run -it --rm da-spark-image:latest bash

tpcds-gen:
	docker exec da-spark-master bash /home/spark/local_benchmarks/lst-bench/conf/tpcds_generation/tpcds_data_gen.sh $(SF) $(MTS)

attach-sql:
	docker exec -it da-spark-master spark-sql

fast-tpcds:
	docker exec da-spark-master bash -c 'cd /home/spark/lst-bench/ && /home/spark/lst-bench/launcher.sh -c /home/spark/local_benchmarks/lst-bench/conf/connections_config.yaml -t /home/spark/local_benchmarks/lst-bench/conf/fast_test_iceberg/telemetry_config.yaml -l /home/spark/local_benchmarks/lst-bench/conf/library/spark/tpcds/library.yaml -w /home/spark/local_benchmarks/lst-bench/conf/fast_test_iceberg/workload.yaml -e /home/spark/local_benchmarks/lst-bench/conf/fast_test_iceberg/experiment.yaml'

mvcc-conflicts:
	docker exec da-spark-master ./launcher.sh -c conf/connections_config.yaml -t conf/mvcc_conflicts/telemetry_config.yaml -l conf/library/spark/tpcds/library.yaml -w conf/mvcc_conflicts/workload.yaml -e conf/mvcc_conflicts/experiment.yaml