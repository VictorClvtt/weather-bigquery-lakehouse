FROM quay.io/astronomer/astro-runtime:13.2.0

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

# Install PySpark
RUN pip install --no-cache-dir pyspark==3.5.1

USER astro