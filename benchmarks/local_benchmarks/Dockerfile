FROM apache/spark:3.3.3-scala2.12-java11-python3-r-ubuntu as spark-base
#https://hub.docker.com/r/apache/spark/

ARG LST_BENCH_COMMIT=2475787fb196fab4defeee0290a91a504fe2a2c2

USER spark

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
WORKDIR ${SPARK_HOME}

# Setup Spark related environment variables
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# Copy the default configurations into $SPARK_HOME/conf
RUN mkdir -p "$SPARK_HOME/conf" && mkdir -p "$SPARK_HOME/spark-events" && mkdir -p "$SPARK_HOME/data" && mkdir -p "$SPARK_HOME/warehouse"
COPY --chown=spark:spark conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"

# Copy appropriate entrypoint script
COPY --chown=spark:spark entrypoint.sh /opt/spark/sbin/

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

ENTRYPOINT ["/opt/spark/sbin/entrypoint.sh"]

FROM spark-base as lst-bench

USER root
RUN mkdir /home/spark && \
    chown spark:spark /home/spark

# Install development tools for Java
RUN apt-get update -qq && \
    apt-get install -qq openjdk-11-jdk vim git

# Setup databricks' tpcds data generator
USER root
RUN apt-get update -qq && apt-get install  -qq -y gcc make flex bison git byacc
WORKDIR /home/spark
USER spark
RUN git clone https://github.com/databricks/tpcds-kit.git && \
  cd tpcds-kit/tools && \
  make OS=LINUX

# Setup databricks' tpch data generator
#RUN apt-get update && apt-get install -y gcc make flex bison git byacc && \
#  git clone https://github.com/databricks/tpch-dbgen.git && \
#  cd tpch-dbgen && \
#  make

WORKDIR /home/spark
# TODO: Fix path to JDK for non amd64 architectures
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

RUN git clone -q -- https://github.com/microsoft/lst-bench.git
WORKDIR /home/spark/lst-bench
RUN git checkout -q $LST_BENCH_COMMIT && \
    ./mvnw package -Pspark-jdbc

# Copy over the configurations (change to bind mount in future)
COPY --chown=spark:spark conf/lst-bench/* /home/spark/lst-bench/conf/