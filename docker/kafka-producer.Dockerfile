# Multi-stage build for Kafka Producer (Scala/Maven)
FROM maven:3.8.6-openjdk-8-slim AS builder

WORKDIR /app
COPY ["Creditcard Producer/pom.xml", "./pom.xml"]
COPY ["Creditcard Producer/src", "./src"]

# Build the application with dependencies
RUN mvn clean package -DskipTests

# Runtime stage
FROM openjdk:8-jre-slim

WORKDIR /app

# Copy the built JAR
COPY --from=builder /app/target/creditcard-tr-producer-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD java -cp app.jar com.datamantra.producer.HealthCheck || exit 1

# Environment variables for configuration
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV KAFKA_TOPIC=creditcardTransaction
ENV PRODUCER_FILE=/app/data/transactions.csv

EXPOSE 8080

CMD ["java", "-jar", "app.jar"]
