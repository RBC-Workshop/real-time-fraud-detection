# PySpark Fraud Detection Implementation

This is a Python/PySpark implementation of the real-time fraud detection streaming job, migrated from the original Scala implementation.

## Overview

This PySpark implementation provides the same functionality as the Scala Structured Streaming job:
- Consumes credit card transactions from Kafka
- Joins with customer data from Cassandra
- Applies ML models (preprocessing pipeline and RandomForest) for fraud prediction
- Writes predictions to Cassandra tables
- Uses automatic checkpoint-based offset management
- Supports graceful shutdown

## Directory Structure

```
src/main/python/datamantra/
├── __init__.py
├── config/
│   ├── __init__.py
│   └── config.py              # Configuration management (replaces Typesafe Config)
├── kafka/
│   ├── __init__.py
│   ├── kafka_config.py        # Kafka configuration parameters
│   └── kafka_source.py        # Kafka streaming source
├── cassandra/
│   ├── __init__.py
│   ├── cassandra_config.py    # Cassandra configuration parameters
│   └── cassandra_sink_foreach.py  # Custom ForeachWriter for Cassandra
├── schema/
│   ├── __init__.py
│   └── schema.py              # Data schemas and field name constants
├── utils/
│   ├── __init__.py
│   └── utils.py               # Utility functions (distance calculation)
├── spark/
│   ├── __init__.py
│   ├── spark_config.py        # Spark configuration parameters
│   └── graceful_shutdown.py  # Graceful shutdown handler
└── jobs/
    ├── __init__.py
    └── structured_streaming_fraud_detection.py  # Main streaming job
```

## Prerequisites

1. **Python 3.6+**
2. **Apache Spark 2.4.x** (compatible with the existing Spark 2.2.1 infrastructure)
3. **Cassandra** (running and accessible)
4. **Kafka** (running with topic `creditcardTransaction`)
5. **Pre-trained ML models** (preprocessing pipeline and RandomForest model)

## Installation

1. Install Python dependencies:
```bash
cd "Fraud Detection"
pip install -r requirements.txt
```

The requirements.txt includes:
- `pyspark==2.4.8` - PySpark framework
- `cassandra-driver==3.25.0` - Python driver for Cassandra
- `pyhocon==0.3.60` - HOCON configuration parser (compatible with existing config files)

## Configuration

The PySpark job uses the same HOCON configuration files as the Scala version:
- `src/main/resources/spark/application-local.conf` - Local development settings
- `src/main/resources/spark/application.conf` - Production settings
- `src/main/resources/spark/application-cluster.conf` - Cluster settings

Example configuration structure:
```hocon
config {
  mode = local
  spark {
    gracefulShutdown = "true"
    checkpoint = "file:///tmp/checkpoint"
    model.path = "spark/training/RandomForestModel"
    model.preprocessing.path = "spark/training/PreprocessingModel"
    shutdownPath = "/tmp/shutdownmarker"
  }
  kafka {
    topic = "creditcardTransaction"
    group.id = "RealTime Creditcard FraudDetection"
    enable.auto.commit = "false"
    bootstrap.servers = "localhost:9092"
    auto.offset.reset = "earliest"
  }
  cassandra {
    keyspace = "creditcard"
    table.fraud.transaction = "fraud_transaction"
    table.non.fraud.transaction = "non_fraud_transaction"
    table.customer = "customer"
    host = "localhost"
  }
}
```

## Running the Job

### Local Mode

```bash
spark-submit \
  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.7 \
  --conf spark.cassandra.connection.host=localhost \
  --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint \
  src/main/python/datamantra/jobs/structured_streaming_fraud_detection.py \
  src/main/resources/spark/application-local.conf
```

### Cluster Mode

```bash
spark-submit \
  --master spark://your-master:7077 \
  --deploy-mode cluster \
  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.7 \
  --conf spark.cassandra.connection.host=your-cassandra-host \
  src/main/python/datamantra/jobs/structured_streaming_fraud_detection.py \
  src/main/resources/spark/application.conf
```

### Default Settings (No Config File)

If no configuration file is provided, the job will use default local settings:

```bash
spark-submit \
  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.7 \
  src/main/python/datamantra/jobs/structured_streaming_fraud_detection.py
```

## Key Features

### 1. Kafka Source Configuration
Uses `spark.readStream.format("kafka")` with automatic offset management via checkpointing.

### 2. Checkpoint-Based Offset Management
Structured Streaming automatically manages offsets through checkpointing, eliminating the need for manual offset tracking in Cassandra.

### 3. Data Processing Pipeline
- JSON parsing from Kafka messages
- Schema validation and type casting
- Customer data join with broadcast optimization
- Distance calculation using Haversine formula (UDF)

### 4. ML Model Integration
- Loads pre-trained PipelineModel for preprocessing
- Loads RandomForest classification model
- Compatible with models trained in Scala Spark

### 5. Cassandra Sink
Custom ForeachWriter implementation for efficient streaming writes to Cassandra using prepared statements.

### 6. Graceful Shutdown
Monitors shutdown marker file (`/tmp/shutdownmarker`) and gracefully stops streaming queries.

## Differences from Scala Implementation

1. **Configuration Management**: Uses `pyhocon` library instead of Typesafe Config, but maintains HOCON format compatibility
2. **Type System**: Python's dynamic typing vs Scala's static typing
3. **ForeachWriter**: Python implementation using cassandra-driver instead of Datastax Spark Connector's native Scala API
4. **Import Style**: Python modules vs Scala packages, but maintains similar organization

## Monitoring and Operations

### Graceful Shutdown
To gracefully stop the streaming job:
```bash
touch /tmp/shutdownmarker
```

The job will detect the marker file and stop all streaming queries gracefully.

### Viewing Streaming Queries
The job creates two named queries:
- `fraudQuery` - For fraud transactions
- `nonFraudQuery` - For non-fraud transactions

These can be monitored through Spark UI on port 4040.

### Checkpoint Location
The default checkpoint location is `/tmp/checkpoint`. In production, use a fault-tolerant location like HDFS or S3.

## Troubleshooting

### Issue: Cannot connect to Cassandra
**Solution**: Verify Cassandra is running and accessible at the configured host. Check firewall rules and network connectivity.

### Issue: Kafka topic not found
**Solution**: Create the Kafka topic before starting the job:
```bash
kafka-topics --create --topic creditcardTransaction --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Issue: ML models not found
**Solution**: Ensure the preprocessing and RandomForest models have been trained and saved at the configured paths. Run the Spark ML training job first.

### Issue: Import errors
**Solution**: Ensure all dependencies are installed: `pip install -r requirements.txt`

## Migration Notes

This PySpark implementation is functionally equivalent to the Scala Structured Streaming version:
- ✅ Kafka source with automatic offset management
- ✅ Customer data join and feature engineering
- ✅ ML model integration (preprocessing + RandomForest)
- ✅ Cassandra sink for fraud and non-fraud transactions
- ✅ Graceful shutdown handling
- ✅ Compatible with existing configuration files

The legacy DStream implementation was intentionally not migrated, as Structured Streaming provides better fault tolerance and simpler offset management.

## Performance Considerations

1. **Broadcast Join**: Customer data is broadcasted to all executors for efficient joins
2. **Caching**: Customer age DataFrame is cached to avoid recomputation
3. **Prepared Statements**: Cassandra writes use prepared statements for efficiency
4. **Checkpoint Interval**: Default 1000ms check interval for shutdown marker

## Further Reading

- [PySpark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Cassandra Spark Connector](https://github.com/datastax/spark-cassandra-connector)
