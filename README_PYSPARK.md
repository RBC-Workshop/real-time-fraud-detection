# PySpark Real-time Fraud Detection Pipeline

This directory contains the PySpark conversion of the original Scala Structured Streaming fraud detection pipeline. The conversion maintains the same functionality while using PySpark APIs.

## Structure

```
pyspark_fraud_detection/
├── __init__.py
├── config/                     # Configuration modules
│   ├── __init__.py
│   ├── config.py              # Main configuration class
│   ├── spark_config.py        # Spark-specific settings
│   ├── cassandra_config.py    # Cassandra connection settings
│   └── kafka_config.py        # Kafka connection settings
├── kafka/                     # Kafka integration
│   ├── __init__.py
│   └── kafka_source.py        # Kafka streaming source
├── cassandra/                 # Cassandra integration
│   ├── __init__.py
│   ├── cassandra_driver.py    # Cassandra operations
│   └── foreach_sink/
│       ├── __init__.py
│       └── cassandra_sink_foreach.py  # ForeachWriter implementation
├── spark/                     # Spark utilities
│   ├── __init__.py
│   ├── data_reader.py         # Data reading utilities
│   └── graceful_shutdown.py   # Graceful shutdown handling
├── creditcard/                # Schema and data definitions
│   ├── __init__.py
│   ├── schema.py              # Data schemas
│   └── enums.py               # Column name constants
├── utils/                     # Utility functions
│   ├── __init__.py
│   └── utils.py               # Distance calculation and UDFs
└── jobs/                      # Main streaming jobs
    ├── __init__.py
    └── structured_streaming_fraud_detection.py  # Main streaming job
```

## Key Conversions

### 1. Configuration Classes
- **Scala objects** → **Python classes** with class methods
- **Scala Map** → **Python dictionaries**
- **TypeSafe Config** → **Python configparser**

### 2. Kafka Source
- **Scala implicit parameters** → **Explicit SparkSession parameter**
- **Scala case class casting** → **Python DataFrame operations**
- **Scala imports** → **Python imports with proper package structure**

### 3. Cassandra Integration
- **Scala ForeachWriter** → **Python ForeachWriter class**
- **Datastax Spark Connector** → **Cassandra Python driver**
- **Scala CQL string interpolation** → **Python f-strings**

### 4. Main Streaming Job
- **Scala DataFrame operations** → **PySpark DataFrame operations**
- **Scala ML Pipeline** → **PySpark ML Pipeline**
- **Scala UDF** → **Python UDF with proper type annotations**

### 5. Utility Functions
- **Scala math functions** → **Python math module**
- **Scala distance calculation** → **Python Haversine formula implementation**

## Usage

```python
from pyspark_fraud_detection.jobs.structured_streaming_fraud_detection import main

# Run with default configuration
main()

# Run with configuration file
main(["/path/to/config.conf"])
```

## Dependencies

Install required packages:
```bash
pip install -r requirements.txt
```

## Key Features Preserved

1. **Structured Streaming**: Uses DataFrame-based streaming (not RDD-based)
2. **Automatic Checkpointing**: Maintains state automatically without manual offset management
3. **ML Model Integration**: Loads and applies preprocessing and Random Forest models
4. **Broadcast Joins**: Optimizes customer data joins using broadcast
5. **Graceful Shutdown**: Handles shutdown signals properly
6. **Cassandra Integration**: Writes fraud/non-fraud transactions to separate tables
7. **Distance Calculation**: Maintains Haversine formula for merchant-customer distance

## Differences from Scala Version

1. **Language Syntax**: Python syntax instead of Scala
2. **Import Structure**: Python package imports instead of Scala imports
3. **Configuration**: Python configparser instead of TypeSafe Config
4. **Logging**: Python logging instead of log4j
5. **Type Annotations**: Python type hints for better code clarity

The core business logic, data flow, and functionality remain identical to the original Scala implementation.
