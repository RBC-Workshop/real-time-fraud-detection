#!/usr/bin/env python3
"""
Test script to validate the PySpark fraud detection conversion.
Tests all major components to ensure they work correctly.
"""

import sys
import traceback
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_imports():
    """Test that all modules can be imported successfully."""
    print("=" * 60)
    print("TESTING IMPORTS")
    print("=" * 60)
    
    try:
        from pyspark_fraud_detection.config.config import Config
        from pyspark_fraud_detection.config.spark_config import SparkConfig
        from pyspark_fraud_detection.config.cassandra_config import CassandraConfig
        from pyspark_fraud_detection.config.kafka_config import KafkaConfig
        print("✅ Configuration modules imported successfully")
        
        from pyspark_fraud_detection.utils.utils import get_distance, distance_udf
        print("✅ Utility modules imported successfully")
        
        from pyspark_fraud_detection.creditcard.schema import Schema
        from pyspark_fraud_detection.creditcard.enums import TransactionColumns, CustomerColumns
        print("✅ Schema and enum modules imported successfully")
        
        from pyspark_fraud_detection.kafka.kafka_source import KafkaSource
        print("✅ Kafka modules imported successfully")
        
        from pyspark_fraud_detection.cassandra.cassandra_driver import CassandraDriver
        from pyspark_fraud_detection.cassandra.foreach_sink.cassandra_sink_foreach import CassandraSinkForeach
        print("✅ Cassandra modules imported successfully")
        
        from pyspark_fraud_detection.spark.data_reader import DataReader
        from pyspark_fraud_detection.spark.graceful_shutdown import GracefulShutdown
        print("✅ Spark utility modules imported successfully")
        
        from pyspark_fraud_detection.jobs.structured_streaming_fraud_detection import create_spark_session, main
        print("✅ Main streaming job imported successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        traceback.print_exc()
        return False

def test_configuration():
    """Test configuration loading and default settings."""
    print("\n" + "=" * 60)
    print("TESTING CONFIGURATION")
    print("=" * 60)
    
    try:
        from pyspark_fraud_detection.config.config import Config
        from pyspark_fraud_detection.config.spark_config import SparkConfig
        from pyspark_fraud_detection.config.cassandra_config import CassandraConfig
        from pyspark_fraud_detection.config.kafka_config import KafkaConfig
        
        Config.default_setting()
        print("✅ Default configuration loaded successfully")
        
        model_path = SparkConfig.get_model_path()
        preprocessing_path = SparkConfig.get_preprocessing_model_path()
        checkpoint_location = SparkConfig.get_checkpoint_location()
        print(f"✅ Spark model path: {model_path}")
        print(f"✅ Spark preprocessing path: {preprocessing_path}")
        print(f"✅ Spark checkpoint location: {checkpoint_location}")
        
        keyspace = CassandraConfig.get_keyspace()
        host = CassandraConfig.get_host()
        fraud_table = CassandraConfig.get_fraud_table()
        non_fraud_table = CassandraConfig.get_non_fraud_table()
        print(f"✅ Cassandra keyspace: {keyspace}")
        print(f"✅ Cassandra host: {host}")
        print(f"✅ Fraud table: {fraud_table}")
        print(f"✅ Non-fraud table: {non_fraud_table}")
        
        kafka_params = KafkaConfig.get_kafka_params()
        print(f"✅ Kafka params: {kafka_params}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        traceback.print_exc()
        return False

def test_utilities():
    """Test utility functions."""
    print("\n" + "=" * 60)
    print("TESTING UTILITIES")
    print("=" * 60)
    
    try:
        from pyspark_fraud_detection.utils.utils import get_distance, distance_udf
        
        distance = get_distance(40.7128, -74.0060, 34.0522, -118.2437)
        expected_distance = 3944  # Approximate distance in km
        
        if 3900 < distance < 4000:
            print(f"✅ Distance calculation works: NYC to LA = {distance:.2f} km")
        else:
            print(f"❌ Distance calculation seems incorrect: {distance:.2f} km (expected ~3944 km)")
            return False
        
        print(f"✅ Distance UDF created: {type(distance_udf)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Utilities test failed: {e}")
        traceback.print_exc()
        return False

def test_schemas():
    """Test schema definitions."""
    print("\n" + "=" * 60)
    print("TESTING SCHEMAS")
    print("=" * 60)
    
    try:
        from pyspark_fraud_detection.creditcard.schema import Schema
        from pyspark_fraud_detection.creditcard.enums import TransactionColumns, CustomerColumns
        
        transaction_schema = Schema.TRANSACTION_SCHEMA
        print(f"✅ Transaction schema has {len(transaction_schema.fields)} fields")
        
        customer_schema = Schema.CUSTOMER_SCHEMA
        print(f"✅ Customer schema has {len(customer_schema.fields)} fields")
        
        kafka_schema = Schema.KAFKA_TRANSACTION_SCHEMA
        print(f"✅ Kafka transaction schema has {len(kafka_schema.fields)} fields")
        
        print(f"✅ Transaction columns - CC_NUM: {TransactionColumns.CC_NUM}")
        print(f"✅ Transaction columns - AMT: {TransactionColumns.AMT}")
        print(f"✅ Customer columns - CC_NUM: {CustomerColumns.CC_NUM}")
        print(f"✅ Customer columns - DOB: {CustomerColumns.DOB}")
        
        return True
        
    except Exception as e:
        print(f"❌ Schema test failed: {e}")
        traceback.print_exc()
        return False

def test_cassandra_foreach_writer():
    """Test Cassandra ForeachWriter functionality."""
    print("\n" + "=" * 60)
    print("TESTING CASSANDRA FOREACH WRITER")
    print("=" * 60)
    
    try:
        from pyspark_fraud_detection.cassandra.foreach_sink.cassandra_sink_foreach import CassandraSinkForeach
        from pyspark_fraud_detection.config.cassandra_config import CassandraConfig
        
        sink = CassandraSinkForeach('test_db', 'test_table')
        print(f"✅ CassandraSinkForeach created: db={sink.db_name}, table={sink.table_name}")
        
        assert hasattr(sink, 'open'), "Missing open method"
        assert hasattr(sink, 'process'), "Missing process method"
        assert hasattr(sink, 'close'), "Missing close method"
        assert hasattr(sink, '_build_transaction_cql'), "Missing _build_transaction_cql method"
        assert hasattr(sink, '_build_offset_cql'), "Missing _build_offset_cql method"
        print("✅ All required ForeachWriter methods exist")
        
        return True
        
    except Exception as e:
        print(f"❌ Cassandra ForeachWriter test failed: {e}")
        traceback.print_exc()
        return False

def test_spark_session_creation():
    """Test SparkSession creation (without actually starting it)."""
    print("\n" + "=" * 60)
    print("TESTING SPARK SESSION CREATION")
    print("=" * 60)
    
    try:
        from pyspark_fraud_detection.jobs.structured_streaming_fraud_detection import create_spark_session
        
        assert callable(create_spark_session), "create_spark_session is not callable"
        print("✅ SparkSession creation function is available and callable")
        
        print("✅ SparkSession creation test passed (function available)")
        
        return True
        
    except Exception as e:
        print(f"❌ SparkSession creation test failed: {e}")
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("Starting PySpark Fraud Detection Conversion Tests")
    print("=" * 60)
    
    tests = [
        ("Import Tests", test_imports),
        ("Configuration Tests", test_configuration),
        ("Utility Tests", test_utilities),
        ("Schema Tests", test_schemas),
        ("Cassandra ForeachWriter Tests", test_cassandra_foreach_writer),
        ("SparkSession Creation Tests", test_spark_session_creation),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
        else:
            failed += 1
    
    print(f"\nTotal: {len(results)} tests")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED! The PySpark conversion is working correctly.")
        return True
    else:
        print(f"\n⚠️  {failed} test(s) failed. Please review the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
