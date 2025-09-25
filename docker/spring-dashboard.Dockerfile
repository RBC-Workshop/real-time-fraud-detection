# Multi-stage build for Spring Boot Dashboard
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app
COPY ["Fraud Alert Dashboard/pom.xml", "./pom.xml"]
COPY ["Fraud Alert Dashboard/src", "./src"]

# Build the application
RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy the built JAR
COPY --from=builder /app/target/fraud-alert-dashboard-*.jar app.jar

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# Environment variables for configuration
ENV CASSANDRA_HOST=localhost
ENV CASSANDRA_PORT=9042
ENV CASSANDRA_KEYSPACE=creditcard
ENV SERVER_PORT=8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health || exit 1

EXPOSE 8080

CMD ["java", "-jar", "app.jar"]
