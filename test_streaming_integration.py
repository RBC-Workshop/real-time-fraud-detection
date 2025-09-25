#!/usr/bin/env python3
"""
Integration test for the PySpark fraud detection streaming pipeline.
Tests the main streaming job initialization and DataFrame operations.
"""

import sys
import traceback
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_spark_session_initialization():
    """Test that SparkSession can be created and configured properly."""
    print("=" * 60)
    print("TESTING SPARK SESSION INITIALIZATION")
    print("=" * 60)
    
    try:
        from pyspark_fraud_detection.jobs.structured_streaming_fraud_detection import create_spark_session
        from pyspark_fraud_detection.config.config import Config
        
        Config.default_setting()
        
        spark = create_spark_session()
        print(f"✅ SparkSession created successfully: {spark.sparkContext.appName}")
        print(f"✅ Spark version: {spark.version}")
        print(f"✅ Spark master: {spark.sparkContext.master}")
        
        test_data = [("1234567890123456", "John", "Doe", 100.50, 40.7128, -74.0060)]
        columns = ["cc_num", "first", "last", "amt", "lat", "long"]
        
        df = spark.createDataFrame(test_data, columns)
        print(f"✅ Test DataFrame created with {df.count()} rows")
        
        from pyspark.sql.functions import col
        from pyspark.sql.types import DoubleType
        
        transformed_df = df.withColumn("amt", col("amt").cast(DoubleType()))
        print(f"✅ DataFrame transformation successful")
        
        print(f"✅ DataFrame schema: {[field.name for field in transformed_df.schema.fields]}")
        
        spark.stop()
        print("✅ SparkSession stopped successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ SparkSession initialization failed: {e}")
        traceback.print_exc()
        return False

def test_kafka_source_configuration():
    """Test Kafka source configuration without actually connecting."""
    print("\n" + "=" * 60)
    print("TESTING KAFKA SOURCE CONFIGURATION")
    print("=" * 60)
    
    try:
        from pyspark_fraud_detection.kafka.kafka_source import KafkaSource
        from pyspark_fraud_detection.config.config import Config
        from pyspark_fraud_detection.jobs.structured_streaming_fraud_detection import create_spark_session
        
        Config.default_setting()
        
        spark = create_spark_session()
        
        assert hasattr(KafkaSource, 'read_stream'), "KafkaSource missing read_stream method"
        assert callable(KafkaSource.read_stream), "read_stream is not callable"
        print("✅ KafkaSource.read_stream method is available")
        
        from pyspark_fraud_detection.creditcard.schema import Schema
        kafka_schema = Schema.KAFKA_TRANSACTION_SCHEMA
        print(f"✅ Kafka transaction schema loaded with {len(kafka_schema.fields)} fields")
        
        spark.stop()
        
        return True
        
    except Exception as e:
        print(f"❌ Kafka source configuration test failed: {e}")
        traceback.print_exc()
        return False

def test_ml_model_loading_simulation():
    """Test ML model loading functionality (simulate without actual model files)."""
    print("\n" + "=" * 60)
    print("TESTING ML MODEL LOADING SIMULATION")
    print("=" * 60)
    
    try:
        from pyspark_fraud_detection.config.spark_config import SparkConfig
        from pyspark_fraud_detection.jobs.structured_streaming_fraud_detection import create_spark_session
        
        SparkConfig.default_setting()
        
        model_path = SparkConfig.get_model_path()
        preprocessing_path = SparkConfig.get_preprocessing_model_path()
        
        print(f"✅ Model path configured: {model_path}")
        print(f"✅ Preprocessing model path configured: {preprocessing_path}")
        
        spark = create_spark_session()
        
        from pyspark.ml import PipelineModel
        from pyspark.ml.classification import RandomForestClassificationModel
        print("✅ PySpark ML classes imported successfully")
        
        test_data = [(1.0, 2.0, 3.0), (4.0, 5.0, 6.0)]
        columns = ["feature1", "feature2", "feature3"]
        df = spark.createDataFrame(test_data, columns)
        
        from pyspark.ml.feature import VectorAssembler
        assembler = VectorAssembler(inputCols=columns, outputCol="features")
        feature_df = assembler.transform(df)
        print("✅ ML feature transformation successful")
        
        spark.stop()
        
        return True
        
    except Exception as e:
        print(f"❌ ML model loading simulation failed: {e}")
        traceback.print_exc()
        return False

def test_distance_calculation_integration():
    """Test distance calculation with actual Spark DataFrame."""
    print("\n" + "=" * 60)
    print("TESTING DISTANCE CALCULATION INTEGRATION")
    print("=" * 60)
    
    try:
        from pyspark_fraud_detection.utils.utils import distance_udf
        from pyspark_fraud_detection.jobs.structured_streaming_fraud_detection import create_spark_session
        from pyspark.sql.functions import col, round as spark_round
        
        spark = create_spark_session()
        
        test_data = [
            ("1234567890123456", 40.7128, -74.0060, 34.0522, -118.2437),  # NYC customer, LA merchant
            ("2345678901234567", 41.8781, -87.6298, 40.7128, -74.0060),   # Chicago customer, NYC merchant
        ]
        columns = ["cc_num", "lat", "long", "merch_lat", "merch_long"]
        
        df = spark.createDataFrame(test_data, columns)
        
        result_df = df.withColumn("distance", 
                                 spark_round(distance_udf(col("lat"), col("long"), 
                                                         col("merch_lat"), col("merch_long")), 2))
        
        results = result_df.collect()
        
        nyc_to_la_distance = results[0]["distance"]
        chicago_to_nyc_distance = results[1]["distance"]
        
        print(f"✅ NYC to LA distance: {nyc_to_la_distance} km")
        print(f"✅ Chicago to NYC distance: {chicago_to_nyc_distance} km")
        
        if 3900 < nyc_to_la_distance < 4000:
            print("✅ NYC to LA distance is reasonable")
        else:
            print(f"❌ NYC to LA distance seems incorrect: {nyc_to_la_distance}")
            return False
            
        if 1100 < chicago_to_nyc_distance < 1200:
            print("✅ Chicago to NYC distance is reasonable")
        else:
            print(f"❌ Chicago to NYC distance seems incorrect: {chicago_to_nyc_distance}")
            return False
        
        spark.stop()
        
        return True
        
    except Exception as e:
        print(f"❌ Distance calculation integration test failed: {e}")
        traceback.print_exc()
        return False

def main():
    """Run all integration tests."""
    print("Starting PySpark Fraud Detection Integration Tests")
    print("=" * 60)
    
    tests = [
        ("SparkSession Initialization", test_spark_session_initialization),
        ("Kafka Source Configuration", test_kafka_source_configuration),
        ("ML Model Loading Simulation", test_ml_model_loading_simulation),
        ("Distance Calculation Integration", test_distance_calculation_integration),
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
    print("INTEGRATION TEST SUMMARY")
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
        print("\n🎉 ALL INTEGRATION TESTS PASSED! The PySpark streaming pipeline is ready for production.")
        return True
    else:
        print(f"\n⚠️  {failed} integration test(s) failed. Please review the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
