# UrbanMind big-data node: Hadoop (HDFS + YARN + MapReduce), Pig, Hive, Spark (PySpark + Scala shell)
# All on Java 8, which every one of these versions supports.
FROM eclipse-temurin:8-jdk-jammy

ENV HADOOP_VERSION=3.3.6 \
    PIG_VERSION=0.17.0 \
    HIVE_VERSION=3.1.3 \
    SPARK_VERSION=3.5.3

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip curl procps netcat-openbsd less \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

# fetch <path under apache dist>: try the fast CDN first, fall back to the archive
RUN printf '#!/bin/sh\nset -e\ncurl -fsSL --retry 3 "https://dlcdn.apache.org/$1" -o /tmp/pkg.tgz || curl -fSL --retry 3 "https://archive.apache.org/dist/$1" -o /tmp/pkg.tgz\ntar -xzf /tmp/pkg.tgz -C /opt\nrm /tmp/pkg.tgz\n' > /usr/local/bin/fetch-apache \
    && chmod +x /usr/local/bin/fetch-apache

RUN fetch-apache hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm -rf /opt/hadoop/share/doc

RUN fetch-apache pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz \
    && ln -s /opt/pig-${PIG_VERSION} /opt/pig

# Hive 3.1.3 ships guava 19, Hadoop 3.3 needs guava 27 -> swap it or the Hive CLI crashes on start
RUN fetch-apache hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
    && ln -s /opt/apache-hive-${HIVE_VERSION}-bin /opt/hive \
    && rm -f /opt/hive/lib/guava-19.0.jar \
    && cp /opt/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar /opt/hive/lib/ \
    && rm -f /opt/hive/lib/log4j-slf4j-impl-*.jar

RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION} pyarrow==16.1.0 pandas==2.2.2 \
        kafka-python-ng==2.2.3 feedparser==6.0.11 requests python-dotenv

# Kafka connector for Spark Structured Streaming, baked in so nothing downloads at run time
RUN SPARK_JARS=$(python3 -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))") \
    && M=https://repo1.maven.org/maven2 \
    && curl -fsSL -o $SPARK_JARS/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar $M/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    && curl -fsSL -o $SPARK_JARS/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    && curl -fsSL -o $SPARK_JARS/kafka-clients-3.4.1.jar $M/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fsSL -o $SPARK_JARS/commons-pool2-2.11.1.jar $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

ENV JAVA_HOME=/opt/java/openjdk \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    HIVE_HOME=/opt/hive \
    PIG_HOME=/opt/pig \
    PIG_CLASSPATH=/opt/hadoop/etc/hadoop \
    SPARK_HOME=/usr/local/lib/python3.10/dist-packages/pyspark \
    PYSPARK_PYTHON=python3 \
    HADOOP_HEAPSIZE_MAX=512m
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PIG_HOME/bin:$SPARK_HOME/bin

COPY conf/core-site.xml conf/hdfs-site.xml conf/mapred-site.xml conf/yarn-site.xml $HADOOP_CONF_DIR/
COPY conf/hive-site.xml $HIVE_HOME/conf/
RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh

WORKDIR /workspace
CMD ["bash", "/workspace/scripts/entrypoint.sh"]
