# syntax=docker/dockerfile:1
# Образ Spark-блока `BLOCK` (read, filter, aggregate). Собирается из корня
# репозитория, чтобы взять контракты e3.shared:
#   docker build -f blocks/Dockerfile --build-arg BLOCK=filter -t e3-filter .
FROM apache/spark:3.5.9-java17-python3

USER root

# S3A: hadoop-aws той же версии, что hadoop-client в Spark 3.5.9 (3.3.4), и его AWS SDK.
ADD --checksum=sha256:53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/
ADD --checksum=sha256:873fe7cf495126619997bec21c44de5d992544aea7e632fdc77adb1a0915bae5 \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/hadoop-aws-3.3.4.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Контракты Backend ↔ DS — те же модули, что у Control Plane; версии как в uv.lock.
RUN pip3 install --no-cache-dir pydantic==2.14.1 jsonschema==4.26.0 boto3==1.43.118
ARG SITE=/usr/local/lib/python3.10/dist-packages
COPY src/e3/__init__.py ${SITE}/e3/
COPY src/e3/shared/__init__.py src/e3/shared/contracts.py src/e3/shared/manifest.py ${SITE}/e3/shared/

ARG BLOCK
COPY blocks/spark_io.py /opt/e3/spark_io.py
COPY blocks/${BLOCK}/main.py /opt/e3/main.py

USER 185
