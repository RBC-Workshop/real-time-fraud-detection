# Multi-stage build for Spark Jobs (Scala/Maven)
FROM maven:3.8.6-openjdk-8-slim AS builder

WORKDIR /app
COPY ["Fraud Detection/pom.xml", "./pom.xml"]
COPY ["Fraud Detection/src", "./src"]

# Build the application
RUN mvn clean package -DskipTests

# Runtime stage with Spark
FROM bitnami/spark:3.3.2

USER root

# Install additional dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the built JAR
COPY --from=builder /app/target/original-fruaddetection-spark.jar app.jar

# Create directories for models and checkpoints
RUN mkdir -p /app/models /app/checkpoints /app/data

# Environment variables for configuration
ENV SPARK_MASTER=local[*]
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV CASSANDRA_HOST=localhost
ENV CASSANDRA_PORT=9042
ENV MODEL_PATH=/app/models
ENV CHECKPOINT_PATH=/app/checkpoints

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
  CMD curl -f http://localhost:4040 || exit 1

EXPOSE 4040 4041

CMD ["spark-submit", "--class", "com.datamantra.spark.jobs.RealTimeFraudDetection.DstreamFraudDetection", \
     "--master", "$SPARK_MASTER", \
     "--conf", "spark.cassandra.connection.host=$CASSANDRA_HOST", \
     "--conf", "spark.cassandra.connection.port=$CASSANDRA_PORT", \
     "app.jar"]
