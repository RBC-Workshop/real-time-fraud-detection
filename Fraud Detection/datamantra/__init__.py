"""
DataMantra - Python/PySpark fraud detection infrastructure package.

This package provides the foundational components for real-time fraud detection
including configuration management, data loading, schema definitions, and utilities.
"""

from .config.config import Config
from .config.spark_config import SparkConfig
from .config.cassandra_config import CassandraConfig
from .data.data_reader import DataReader
from .schema.schemas import (
    transaction_schema,
    fraud_checked_transaction_schema,
    customer_schema,
    kafka_transaction_schema
)
from .schema.enums import TransactionKafka, Customer, TransactionCassandra
from .utils.utils import get_distance

__version__ = "1.0.0"
__author__ = "DataMantra Team"

__all__ = [
    "Config",
    "SparkConfig", 
    "CassandraConfig",
    "DataReader",
    "transaction_schema",
    "fraud_checked_transaction_schema",
    "customer_schema", 
    "kafka_transaction_schema",
    "TransactionKafka",
    "Customer",
    "TransactionCassandra",
    "get_distance"
]
