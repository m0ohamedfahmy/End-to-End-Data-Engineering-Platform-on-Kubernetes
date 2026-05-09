# Dockerfile
#
# Custom Spark image with hadoop-aws and aws-java-sdk-bundle pre-installed.
#
# BUG 4 FIX: original Dockerfile set SPARK_CLASSPATH but did NOT add the
# jars directory to spark-defaults.conf, so Spark's internal classloader
# sometimes misses the JARs depending on how the executor is launched.
# We add both ENV and spark-defaults.conf entries to be certain.
#
# BUILD:
#   docker build -t spark-minio-s3a-custom:3.5.0 .
#
# LOAD INTO KIND:
#   kind load docker-image spark-minio-s3a-custom:3.5.0 --name <your-cluster-name>
#
# VERIFY loaded:
#   docker exec <kind-node-name> crictl images | grep spark-minio

FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

USER root

RUN mkdir -p /opt/spark/jars

# hadoop-aws: provides the S3AFileSystem implementation
RUN curl -fL -o /opt/spark/jars/hadoop-aws.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# aws-java-sdk-bundle: AWS SDK that hadoop-aws depends on at runtime
# Version 1.12.262 is compatible with hadoop-aws 3.3.4
RUN curl -fL -o /opt/spark/jars/aws-java-sdk-bundle.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Verify both JARs downloaded correctly (non-zero size)
RUN ls -lh /opt/spark/jars/hadoop-aws.jar /opt/spark/jars/aws-java-sdk-bundle.jar

# Make JARs readable by the spark user (UID 185)
RUN chown -R 185:185 /opt/spark/jars/

# Add to spark-defaults.conf so ALL Spark components (driver + executor) pick
# up the JARs via the internal classloader — more reliable than SPARK_CLASSPATH
# alone for the executor side.
RUN mkdir -p /opt/spark/conf && \
    echo "spark.driver.extraClassPath   /opt/spark/jars/*" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.executor.extraClassPath /opt/spark/jars/*" >> /opt/spark/conf/spark-defaults.conf

# ENV approach as well — belt and suspenders
ENV SPARK_CLASSPATH="/opt/spark/jars/*"

USER 185