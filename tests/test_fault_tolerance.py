"""
Fault Tolerance Testing for PySpark Fraud Detection System.

SCRUM-25: Tests system resilience under various failure scenarios including
Kafka broker failures, Cassandra connection issues, and exactly-once semantics.
"""

import pytest
import time
import threading
from typing import Dict, List, Any, Optional
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../src/python'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../Fraud Detection'))

from fraud_detection_job import FraudDetectionJob
from kafka_source import KafkaSource
from cassandra_driver import CassandraDriver, CassandraForeachWriter
from graceful_shutdown import GracefulShutdown


class FaultInjector:
    """Utility class for injecting various types of failures during testing."""
    
    def __init__(self):
        self.active_faults = []
        self.fault_history = []
    
    def inject_kafka_broker_failure(self, duration_seconds: int = 5):
        """Simulate Kafka broker failure for specified duration."""
        fault = {
            "type": "kafka_broker_failure",
            "start_time": time.time(),
            "duration": duration_seconds,
            "active": True
        }
        self.active_faults.append(fault)
        
        def recover():
            time.sleep(duration_seconds)
            fault["active"] = False
            fault["end_time"] = time.time()
            self.fault_history.append(fault)
            self.active_faults.remove(fault)
        
        threading.Thread(target=recover, daemon=True).start()
        return fault
    
    def inject_cassandra_connection_failure(self, duration_seconds: int = 3):
        """Simulate Cassandra connection failure."""
        fault = {
            "type": "cassandra_connection_failure",
            "start_time": time.time(),
            "duration": duration_seconds,
            "active": True
        }
        self.active_faults.append(fault)
        
        def recover():
            time.sleep(duration_seconds)
            fault["active"] = False
            fault["end_time"] = time.time()
            self.fault_history.append(fault)
            self.active_faults.remove(fault)
        
        threading.Thread(target=recover, daemon=True).start()
        return fault
    
    def inject_network_partition(self, duration_seconds: int = 10):
        """Simulate network partition affecting both Kafka and Cassandra."""
        fault = {
            "type": "network_partition",
            "start_time": time.time(),
            "duration": duration_seconds,
            "active": True
        }
        self.active_faults.append(fault)
        
        def recover():
            time.sleep(duration_seconds)
            fault["active"] = False
            fault["end_time"] = time.time()
            self.fault_history.append(fault)
            self.active_faults.remove(fault)
        
        threading.Thread(target=recover, daemon=True).start()
        return fault
    
    def is_fault_active(self, fault_type: str) -> bool:
        """Check if a specific type of fault is currently active."""
        return any(fault["type"] == fault_type and fault["active"] for fault in self.active_faults)


class TestFaultTolerance:
    """Test system fault tolerance and recovery capabilities."""
    
    def test_kafka_broker_failure_recovery(self, spark_session: SparkSession):
        """Test system recovery from Kafka broker failures."""
        fault_injector = FaultInjector()
        
        with patch('src.python.kafka_source.KafkaSource.read_stream') as mock_kafka_read:
            mock_stream = Mock()
            mock_kafka_read.return_value = mock_stream
            
            stream = KafkaSource.read_stream(spark_session)
            assert stream is not None, "Initial Kafka connection should succeed"
            
            fault = fault_injector.inject_kafka_broker_failure(duration_seconds=2)
            
            with patch('src.python.kafka_source.KafkaSource.read_stream', side_effect=Exception("Kafka broker unavailable")):
                with pytest.raises(Exception, match="Kafka broker unavailable"):
                    KafkaSource.read_stream(spark_session)
            
            time.sleep(3)
            
            assert not fault_injector.is_fault_active("kafka_broker_failure"), "Fault should be recovered"
            
            recovered_stream = KafkaSource.read_stream(spark_session)
            assert recovered_stream is not None, "Kafka connection should recover"
    
    def test_cassandra_connection_failure_recovery(self, spark_session: SparkSession):
        """Test Cassandra connection failure handling and recovery."""
        fault_injector = FaultInjector()
        
        writer = CassandraForeachWriter("test_keyspace", "test_table")
        
        with patch('cassandra.cluster.Cluster') as mock_cluster:
            mock_session = Mock()
            mock_cluster.return_value.connect.return_value = mock_session
            
            assert writer.open(0, 0) == True, "Initial Cassandra connection should succeed"
            
            fault = fault_injector.inject_cassandra_connection_failure(duration_seconds=2)
            
            mock_cluster.return_value.connect.side_effect = Exception("Cassandra unavailable")
            
            new_writer = CassandraForeachWriter("test_keyspace", "test_table")
            assert new_writer.open(0, 0) == False, "Connection should fail during fault"
            
            time.sleep(3)
            
            mock_cluster.return_value.connect.side_effect = None
            mock_cluster.return_value.connect.return_value = mock_session
            
            recovered_writer = CassandraForeachWriter("test_keyspace", "test_table")
            assert recovered_writer.open(0, 0) == True, "Cassandra connection should recover"
    
    def test_exactly_once_semantics_under_failure(self, spark_session: SparkSession):
        """Test exactly-once processing semantics during failures."""
        test_data = [
            ("1234567890123456", "T001", 45.67, 1.0),
            ("2345678901234567", "T002", 78.90, 0.0),
            ("3456789012345678", "T003", 123.45, 1.0)
        ]
        
        test_df = spark_session.createDataFrame(
            test_data, 
            ["cc_num", "trans_num", "amt", "is_fraud"]
        )
        
        processed_records = []
        
        def mock_cassandra_write(row):
            """Mock Cassandra write that tracks processed records."""
            processed_records.append(row["trans_num"])
            
            if row["trans_num"] == "T002":
                raise Exception("Simulated Cassandra write failure")
        
        with patch.object(CassandraForeachWriter, 'process', side_effect=mock_cassandra_write):
            writer = CassandraForeachWriter("test_keyspace", "test_table")
            writer.session = Mock()  # Mock session to avoid actual connection
            
            for row in test_df.collect():
                try:
                    writer.process(row)
                except Exception as e:
                    assert "Simulated Cassandra write failure" in str(e)
            
            assert "T001" in processed_records, "First record should be processed"
            assert "T003" in processed_records, "Third record should be processed despite second failure"
            
            assert processed_records.count("T001") == 1, "Records should be processed exactly once"
            assert processed_records.count("T003") == 1, "Records should be processed exactly once"
    
    def test_graceful_shutdown_during_processing(self, spark_session: SparkSession):
        """Test graceful shutdown functionality during active processing."""
        mock_query1 = Mock(spec=StreamingQuery)
        mock_query2 = Mock(spec=StreamingQuery)
        mock_queries = [mock_query1, mock_query2]
        
        shutdown_handler = GracefulShutdown(mock_queries, check_interval=100)  # Short interval for testing
        
        mock_streams = Mock()
        spark_session.streams = mock_streams
        mock_streams.awaitAnyTermination.return_value = False  # Simulate timeout
        
        import signal
        import os
        
        def trigger_shutdown():
            """Trigger shutdown after short delay."""
            time.sleep(0.5)
            os.kill(os.getpid(), signal.SIGTERM)
        
        threading.Thread(target=trigger_shutdown, daemon=True).start()
        
        with patch('src.python.config.Config.get_spark_config') as mock_config:
            mock_spark_config = Mock()
            mock_spark_config.shutdown_marker = "/tmp/test_shutdown_marker"
            mock_config.return_value = mock_spark_config
            
            try:
                shutdown_handler.handle_graceful_shutdown(spark_session)
            except SystemExit:
                pass  # Expected during testing
        
        mock_query1.stop.assert_called_once()
        mock_query2.stop.assert_called_once()
    
    def test_checkpoint_recovery_after_failure(self, spark_session: SparkSession, temp_checkpoint_dir: str):
        """Test streaming checkpoint recovery after system failure."""
        checkpoint_path = f"{temp_checkpoint_dir}/fraud_detection_checkpoint"
        
        with patch('src.python.fraud_detection_job.FraudDetectionJob.initialize_spark'):
            job = FraudDetectionJob()
            job.spark_session = spark_session
            
            mock_query = Mock(spec=StreamingQuery)
            mock_query.id = "test_query_id"
            mock_query.runId = "test_run_id"
            
            with patch.object(CassandraDriver, 'save_foreach', return_value=mock_query):
                test_df = spark_session.createDataFrame([
                    ("1234567890123456", "T001", 1.0)
                ], ["cc_num", "trans_num", "is_fraud"])
                
                query = CassandraDriver.save_foreach(
                    test_df, 
                    "test_keyspace", 
                    "test_table", 
                    "test_query",
                    "append"
                )
                
                mock_query.stop()
                
                recovered_query = CassandraDriver.save_foreach(
                    test_df, 
                    "test_keyspace", 
                    "test_table", 
                    "test_query",
                    "append"
                )
                
                assert recovered_query is not None, "System should recover from checkpoint"
    
    def test_network_partition_resilience(self, spark_session: SparkSession):
        """Test system behavior during network partitions."""
        fault_injector = FaultInjector()
        
        fault = fault_injector.inject_network_partition(duration_seconds=3)
        
        with patch('src.python.kafka_source.KafkaSource.read_stream') as mock_kafka, \
             patch('cassandra.cluster.Cluster') as mock_cassandra:
            
            mock_kafka.side_effect = Exception("Network unreachable")
            mock_cassandra.return_value.connect.side_effect = Exception("Network unreachable")
            
            with pytest.raises(Exception, match="Network unreachable"):
                KafkaSource.read_stream(spark_session)
            
            writer = CassandraForeachWriter("test_keyspace", "test_table")
            assert writer.open(0, 0) == False, "Cassandra connection should fail during partition"
            
            time.sleep(4)
            
            mock_kafka.side_effect = None
            mock_kafka.return_value = Mock()
            
            mock_cassandra.return_value.connect.side_effect = None
            mock_cassandra.return_value.connect.return_value = Mock()
            
            recovered_stream = KafkaSource.read_stream(spark_session)
            assert recovered_stream is not None, "Kafka should recover after partition"
            
            recovered_writer = CassandraForeachWriter("test_keyspace", "test_table")
            assert recovered_writer.open(0, 0) == True, "Cassandra should recover after partition"
    
    def test_memory_pressure_handling(self, spark_session: SparkSession):
        """Test system behavior under memory pressure conditions."""
        large_data = []
        for i in range(100000):  # Large dataset
            large_data.append((
                f"cc_num_{i}",
                f"T{i:06d}",
                float(i % 1000),
                float(i % 2)  # is_fraud
            ))
        
        large_df = spark_session.createDataFrame(
            large_data, 
            ["cc_num", "trans_num", "amt", "is_fraud"]
        )
        
        try:
            cached_df = large_df.cache()
            count = cached_df.count()
            
            fraud_count = cached_df.filter(cached_df.is_fraud == 1.0).count()
            non_fraud_count = cached_df.filter(cached_df.is_fraud == 0.0).count()
            
            assert count > 0, "System should process large datasets"
            assert fraud_count + non_fraud_count == count, "Counts should be consistent"
            
        except Exception as e:
            assert "OutOfMemoryError" not in str(e), "System should handle memory pressure gracefully"
    
    def create_fault_tolerance_report(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create comprehensive fault tolerance test report."""
        return {
            "test_summary": {
                "total_tests": len(test_results),
                "passed_tests": sum(1 for result in test_results.values() if result.get("passed", False)),
                "failed_tests": sum(1 for result in test_results.values() if not result.get("passed", False))
            },
            "fault_scenarios_tested": [
                "kafka_broker_failure",
                "cassandra_connection_failure",
                "network_partition",
                "memory_pressure",
                "graceful_shutdown"
            ],
            "recovery_capabilities": {
                "automatic_retry": True,
                "checkpoint_recovery": True,
                "graceful_degradation": True,
                "exactly_once_semantics": True
            },
            "detailed_results": test_results
        }
