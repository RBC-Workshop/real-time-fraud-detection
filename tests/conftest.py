"""
Pytest configuration and shared fixtures for fraud detection testing.

Provides test infrastructure setup including Spark sessions, test data,
and mock services for comprehensive end-to-end testing.
"""

import pytest
import os
import sys
import tempfile
import shutil
from typing import Generator, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

sys.path.append(os.path.join(os.path.dirname(__file__), '../src/python'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../Fraud Detection'))

from datamantra.schema.schemas import kafka_transaction_schema


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """Create Spark session for testing with optimized configuration."""
    spark = SparkSession.builder \
        .appName("FraudDetectionTesting") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def temp_checkpoint_dir() -> Generator[str, None, None]:
    """Create temporary checkpoint directory for streaming tests."""
    temp_dir = tempfile.mkdtemp(prefix="fraud_detection_checkpoint_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_transaction_data() -> list:
    """Generate sample transaction data for testing."""
    return [
        {
            "cc_num": "1234567890123456",
            "trans_time": "2023-01-01 10:00:00",
            "trans_num": "T001",
            "category": "grocery_pos",
            "merchant": "Test Store",
            "amt": 45.67,
            "lat": 40.7128,
            "long": -74.0060,
            "merch_lat": 40.7589,
            "merch_long": -73.9851,
            "first": "John",
            "last": "Doe"
        },
        {
            "cc_num": "2345678901234567",
            "trans_time": "2023-01-01 11:00:00",
            "trans_num": "T002",
            "category": "gas_transport",
            "merchant": "Gas Station",
            "amt": 78.90,
            "lat": 34.0522,
            "long": -118.2437,
            "merch_lat": 34.0522,
            "merch_long": -118.2437,
            "first": "Jane",
            "last": "Smith"
        }
    ]


@pytest.fixture
def sample_customer_data() -> list:
    """Generate sample customer data for testing."""
    return [
        {
            "cc_num": "1234567890123456",
            "dob": "1985-05-15",
            "lat": 40.7128,
            "long": -74.0060
        },
        {
            "cc_num": "2345678901234567",
            "dob": "1990-08-22",
            "lat": 34.0522,
            "long": -118.2437
        }
    ]


@pytest.fixture
def transaction_schema() -> StructType:
    """Get transaction schema for DataFrame creation."""
    return kafka_transaction_schema


@pytest.fixture
def customer_schema() -> StructType:
    """Get customer schema for DataFrame creation."""
    return StructType([
        StructField("cc_num", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("long", DoubleType(), True)
    ])


@pytest.fixture
def mock_ml_models_path(tmp_path) -> Dict[str, str]:
    """Create mock ML model paths for testing."""
    preprocessing_path = tmp_path / "preprocessing_model"
    rf_model_path = tmp_path / "random_forest_model"
    
    preprocessing_path.mkdir()
    rf_model_path.mkdir()
    
    return {
        "preprocessing_model_path": str(preprocessing_path),
        "random_forest_model_path": str(rf_model_path)
    }


@pytest.fixture
def test_config() -> Dict[str, Any]:
    """Provide test configuration settings."""
    return {
        "kafka": {
            "bootstrap.servers": "localhost:9092",
            "topic": "test_creditcardTransaction",
            "group.id": "test_fraud_detection",
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest"
        },
        "cassandra": {
            "host": "localhost",
            "keyspace": "test_fraud_detection",
            "fraud_table": "test_fraud_transaction",
            "non_fraud_table": "test_non_fraud_transaction",
            "offset_table": "test_kafka_offset"
        },
        "spark": {
            "app_name": "TestFraudDetection",
            "checkpoint_location": "/tmp/test_checkpoint"
        }
    }
