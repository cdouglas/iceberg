FROM apache/spark:3.3.3-scala2.12-java11-python3-r-ubuntu as spark-base
#https://hub.docker.com/r/apache/spark/

ARG LST_BENCH_COMMIT=fac17ef52a5f04cb0d9d12e874da365dffef8cec
ENV SPARK_VERSION=3.3.3
ENV SPARK_MAJOR_VERSION=3.3
ENV ICEBERG_VERSION=1.5.0

USER spark

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
WORKDIR ${SPARK_HOME}

# Setup Spark related environment variables
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
# ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copy the default configurations into $SPARK_HOME/conf
RUN mkdir -p "$SPARK_HOME/conf" && mkdir -p "$SPARK_HOME/spark-events" && mkdir -p "$SPARK_HOME/data" && mkdir -p "$SPARK_HOME/warehouse"

# Copy appropriate entrypoint script
COPY --chown=spark:spark entrypoint.sh /opt/spark/sbin/

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENTRYPOINT ["/opt/spark/sbin/entrypoint.sh"]

# Download iceberg spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# Add iceberg spark runtime jar to IJava classpath
ENV IJAVA_CLASSPATH=/opt/spark/jars/*

FROM spark-base as lst-bench

USER root
RUN mkdir /home/spark && \
    chown spark:spark /home/spark

# Install development tools for Java
RUN apt-get update -qq && \
    apt-get install -qq openjdk-11-jdk vim git

# Setup databricks' tpcds data generator
USER root
RUN apt-get update -qq && apt-get install  -qq -y gcc make flex bison git byacc maven
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

RUN git clone -q -- https://github.com/tiemobang/lst-bench.git
WORKDIR /home/spark/lst-bench
RUN git checkout -q $LST_BENCH_COMMIT && \
    ./mvnw package -Pspark-jdbc

# Copy over the configurations (change to bind mount in future)
#COPY --chown=spark:spark conf/lst-bench/* /home/spark/lst-bench/conf/
#COPY --chown=spark:spark conf/tpcds_generation/tpcds_data_gen.sh /home/spark/

WORKDIR /home/spark
# Build google cloud connector from source (takes few minutes)
# The shaded jar contains all the dependencies
RUN git clone -q https://github.com/GoogleCloudDataproc/hadoop-connectors.git && \
    cd hadoop-connectors && \
    mvn package -q -DskipTests && \
    cp -v gcs/target/gcs-connector-*-shaded.jar $SPARK_HOME/jars && \
    cd ${HOME} && \
    rm -rf hadoop-connectors

# Mount point to write out results
RUN mkdir -p /home/spark/local_benchmarks/results && \
    chown -R spark:spark /home/spark/local_benchmarks

# For development
USER root
# Set password for spark user
RUN echo "spark:password" | chpasswd && echo "root:password" | chpasswd && \
    apt-get install -qq -y sudo
RUN echo "spark ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER spark

ARG GS_KEYFILE=".secrets/gs_keyfile.json"
COPY --chown=spark:spark ${GS_KEYFILE} "$SPARK_HOME/conf/gs_keyfile.json"

COPY --chown=spark:spark lst-bench/conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"
COPY --chown=spark:spark lst-bench/conf/spark-env.sh "$SPARK_HOME/conf/spark-env.sh"

WORKDIR /home/spark/local_benchmarks
