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
	docker exec da-spark-master /home/spark/tpcds_data_gen.sh $(SF) $(MTS)

attach-sql:
	docker exec -it da-spark-thrift beeline -u jdbc:hive2://localhost:10000

fast-tpcds:
	docker exec da-spark-master ./launcher.sh -c conf/connections_config.yaml -t conf/fast_test_iceberg/telemetry_config.yaml -l conf/library/spark/tpcds/library.yaml -w conf/fast_test_iceberg/workload.yaml -e conf/fast_test_iceberg/experiment.yaml