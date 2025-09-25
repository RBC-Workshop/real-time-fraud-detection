# Flask API Service Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY ["Cassandra Python/requirements.txt", "./requirements.txt"]
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ["Cassandra Python/", "./"]

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# Environment variables for configuration
ENV CASSANDRA_HOST=localhost
ENV CASSANDRA_PORT=9042
ENV CASSANDRA_KEYSPACE=creditcard
ENV FLASK_PORT=5050

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:5050/api/health || exit 1

EXPOSE 5050

CMD ["python", "app.py"]
