FROM apache/spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir \
    boto3==1.34.0 \
    psycopg2-binary==2.9.9

# Download S3A + Hadoop AWS JARs
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /opt/spark-jobs
COPY jobs/ .
ENV PYTHONUNBUFFERED=1
