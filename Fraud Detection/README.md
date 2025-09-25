# DataMantra - Python/PySpark Fraud Detection Infrastructure

This package provides the foundational Python/PySpark infrastructure for real-time fraud detection, converted from the original Scala implementation while maintaining identical functionality.

## Package Structure

```
datamantra/
├── config/          # Configuration management
│   ├── config.py           # Main configuration loader (Config.scala equivalent)
│   ├── spark_config.py     # Spark-specific configuration (SparkConfig.scala equivalent)
│   └── cassandra_config.py # Cassandra configuration (CassandraConfig.scala equivalent)
├── data/            # Data loading and processing
│   └── data_reader.py      # CSV and Cassandra data reading (DataReader.scala equivalent)
├── schema/          # Data schema definitions
│   ├── schemas.py          # Spark SQL schemas (Schema.scala equivalent)
│   └── enums.py           # Field name constants (CreditcardEnum.scala equivalent)
└── utils/           # Utility functions
    └── utils.py           # Geographic distance calculations (Utils.scala equivalent)
```

## Features

- **Dual Deployment Mode Support**: Supports both local development and cluster deployment modes
- **HOCON Configuration**: Uses PyHOCON library to maintain compatibility with existing .conf files
- **Schema Validation**: Preserves exact field types and nullability from Scala schemas
- **Cassandra Integration**: Maintains existing Cassandra connectivity patterns
- **Kafka Support**: Includes offset handling for Kafka message processing
- **ML Model Loading**: Supports both local filesystem and S3 paths for model storage

## Installation

```bash
pip install -r requirements.txt
python setup.py install
```

For development:
```bash
pip install -e .
```

## Usage

### Configuration Loading

```python
from datamantra.config import Config

# Load configuration from file
Config.parse_args(["/path/to/application.conf"])

# Use default settings for local development
Config.parse_args([])
```

### Data Loading

```python
from datamantra.data import DataReader
from datamantra.schema import customer_schema
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Load CSV data with schema validation
df = DataReader.read("data/customer.csv", customer_schema, spark)

# Load from Cassandra
cassandra_df = DataReader.read_from_cassandra("creditcard", "customer", spark)
```

### Schema Usage

```python
from datamantra.schema import transaction_schema, TransactionKafka

# Use predefined schemas
df = spark.read.schema(transaction_schema).csv("transactions.csv")

# Access field names via enums
cc_num_field = TransactionKafka.cc_num
amount_field = TransactionKafka.amt
```

### Utility Functions

```python
from datamantra.utils import get_distance

# Calculate geographic distance using Haversine formula
distance_km = get_distance(40.7128, -74.0060, 34.0522, -118.2437)
```

## Configuration

The package uses HOCON format configuration files with three main sections:

- `config.common`: Shared settings for all deployment modes
- `config.local`: Local development settings
- `config.cluster`: Production cluster settings

Example configuration structure:
```hocon
config {
  common {
    spark {
      gracefulShutdown = "true"
    }
    cassandra {
      keyspace = "creditcard"
    }
  }
  local {
    spark {
      customer.datasource = "data/customer.csv"
      model.path = "spark/RandomForestModel"
    }
    cassandra {
      host = "localhost"
    }
  }
  cluster {
    spark {
      model.path = "S3Location/RandomForestModel"
    }
    cassandra {
      host = "publicIp"
    }
  }
}
```

## Requirements

- Python 3.7+
- PySpark 2.4+
- PyHOCON for configuration parsing
- Cassandra driver for database connectivity
- Kafka Python client for message processing

## Compatibility

This Python implementation maintains 100% functional compatibility with the original Scala version:

- Identical schema definitions and field types
- Same configuration parameter names and structure
- Equivalent utility function results (especially geographic distance calculations)
- Compatible Cassandra and Kafka integration patterns
- Support for the same ML model loading workflows

## Development

The package follows PySpark best practices and Python naming conventions (snake_case) while preserving the original functionality and behavior of the Scala implementation.
