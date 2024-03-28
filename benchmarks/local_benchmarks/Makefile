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

run-d:
	make down && docker-compose up -d

stop:
	docker-compose stop

submit:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

attach:
	docker exec -it da-spark-master bash

run-terminal: build
	docker run -it --rm da-spark-image:latest bash