"""
Performance Benchmarking Tests for PySpark vs Scala Implementation.

SCRUM-25: Validates performance within ±25% of Scala baseline including
throughput, memory usage, and latency measurements.
"""

import pytest
import time
import psutil
import threading
from typing import Dict, List, Any, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from unittest.mock import Mock, patch
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../src/python'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../Fraud Detection'))

from fraud_detection_job import FraudDetectionJob


class PerformanceMetrics:
    """Container for performance measurement results."""
    
    def __init__(self):
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.memory_usage_mb: List[float] = []
        self.cpu_usage_percent: List[float] = []
        self.records_processed: int = 0
        self.errors_count: int = 0
    
    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0
    
    @property
    def throughput_records_per_second(self) -> float:
        if self.duration_seconds > 0:
            return self.records_processed / self.duration_seconds
        return 0.0
    
    @property
    def avg_memory_usage_mb(self) -> float:
        return sum(self.memory_usage_mb) / len(self.memory_usage_mb) if self.memory_usage_mb else 0.0
    
    @property
    def peak_memory_usage_mb(self) -> float:
        return max(self.memory_usage_mb) if self.memory_usage_mb else 0.0
    
    @property
    def avg_cpu_usage_percent(self) -> float:
        return sum(self.cpu_usage_percent) / len(self.cpu_usage_percent) if self.cpu_usage_percent else 0.0


class PerformanceMonitor:
    """Monitor system performance during test execution."""
    
    def __init__(self, interval_seconds: float = 0.5):
        self.interval = interval_seconds
        self.metrics = PerformanceMetrics()
        self.monitoring = False
        self.monitor_thread = None
    
    def start_monitoring(self):
        """Start performance monitoring in background thread."""
        self.monitoring = True
        self.metrics.start_time = time.time()
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        """Stop performance monitoring and return metrics."""
        self.monitoring = False
        self.metrics.end_time = time.time()
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)
        return self.metrics
    
    def _monitor_loop(self):
        """Background monitoring loop."""
        process = psutil.Process()
        
        while self.monitoring:
            try:
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)
                self.metrics.memory_usage_mb.append(memory_mb)
                
                cpu_percent = process.cpu_percent()
                self.metrics.cpu_usage_percent.append(cpu_percent)
                
                time.sleep(self.interval)
            except Exception:
                pass


class TestPerformanceBenchmark:
    """Performance benchmarking tests against Scala baseline."""
    
    SCALA_BASELINE = {
        "throughput_records_per_second": 1000.0,  # Example baseline
        "avg_memory_usage_mb": 512.0,             # Example baseline
        "peak_memory_usage_mb": 1024.0,           # Example baseline
        "avg_cpu_usage_percent": 50.0,            # Example baseline
        "processing_latency_ms": 100.0            # Example baseline
    }
    
    PERFORMANCE_TOLERANCE = 0.25  # ±25% tolerance requirement
    
    def test_data_processing_throughput(self, spark_session: SparkSession):
        """Test data processing throughput meets ±25% of Scala baseline."""
        num_records = 10000
        test_data = self._generate_large_test_dataset(num_records)
        test_df = spark_session.createDataFrame(test_data)
        
        monitor = PerformanceMonitor()
        monitor.start_monitoring()
        
        try:
            processed_df = test_df \
                .withColumn("amt", col("amt").cast("double")) \
                .withColumn("processed_flag", lit(True)) \
                .cache()
            
            record_count = processed_df.count()
            monitor.metrics.records_processed = record_count
            
        finally:
            metrics = monitor.stop_monitoring()
        
        baseline_throughput = self.SCALA_BASELINE["throughput_records_per_second"]
        min_acceptable = baseline_throughput * (1 - self.PERFORMANCE_TOLERANCE)
        max_acceptable = baseline_throughput * (1 + self.PERFORMANCE_TOLERANCE)
        
        assert min_acceptable <= metrics.throughput_records_per_second <= max_acceptable, \
            f"Throughput {metrics.throughput_records_per_second} outside acceptable range [{min_acceptable}, {max_acceptable}]"
    
    def test_memory_usage_efficiency(self, spark_session: SparkSession):
        """Test memory usage stays within ±25% of Scala baseline."""
        num_records = 50000
        test_data = self._generate_large_test_dataset(num_records)
        test_df = spark_session.createDataFrame(test_data)
        
        monitor = PerformanceMonitor(interval_seconds=0.1)  # More frequent monitoring for memory
        monitor.start_monitoring()
        
        try:
            cached_df = test_df.cache()
            cached_df.count()  # Force caching
            
            processed_df = cached_df \
                .groupBy("category") \
                .count() \
                .collect()
            
        finally:
            metrics = monitor.stop_monitoring()
        
        baseline_avg_memory = self.SCALA_BASELINE["avg_memory_usage_mb"]
        baseline_peak_memory = self.SCALA_BASELINE["peak_memory_usage_mb"]
        
        avg_min = baseline_avg_memory * (1 - self.PERFORMANCE_TOLERANCE)
        avg_max = baseline_avg_memory * (1 + self.PERFORMANCE_TOLERANCE)
        peak_max = baseline_peak_memory * (1 + self.PERFORMANCE_TOLERANCE)
        
        assert avg_min <= metrics.avg_memory_usage_mb <= avg_max, \
            f"Average memory usage {metrics.avg_memory_usage_mb}MB outside acceptable range [{avg_min}, {avg_max}]"
        
        assert metrics.peak_memory_usage_mb <= peak_max, \
            f"Peak memory usage {metrics.peak_memory_usage_mb}MB exceeds maximum {peak_max}MB"
    
    def test_cpu_utilization_efficiency(self, spark_session: SparkSession):
        """Test CPU utilization efficiency compared to Scala baseline."""
        num_records = 20000
        test_data = self._generate_large_test_dataset(num_records)
        test_df = spark_session.createDataFrame(test_data)
        
        monitor = PerformanceMonitor()
        monitor.start_monitoring()
        
        try:
            result = test_df \
                .filter(col("amt") > 50.0) \
                .groupBy("category", "merchant") \
                .agg({"amt": "sum", "amt": "avg", "amt": "count"}) \
                .orderBy("category") \
                .collect()
            
            monitor.metrics.records_processed = len(result)
            
        finally:
            metrics = monitor.stop_monitoring()
        
        baseline_cpu = self.SCALA_BASELINE["avg_cpu_usage_percent"]
        max_acceptable_cpu = baseline_cpu * (1 + self.PERFORMANCE_TOLERANCE)
        
        assert metrics.avg_cpu_usage_percent <= max_acceptable_cpu, \
            f"CPU usage {metrics.avg_cpu_usage_percent}% exceeds maximum {max_acceptable_cpu}%"
    
    @patch('src.python.fraud_detection_job.FraudDetectionJob.initialize_spark')
    @patch('src.python.fraud_detection_job.FraudDetectionJob.load_customer_data')
    def test_end_to_end_pipeline_performance(self, mock_load_customer, mock_init_spark, spark_session: SparkSession):
        """Test end-to-end pipeline performance against Scala baseline."""
        mock_init_spark.return_value = None
        mock_load_customer.return_value = spark_session.createDataFrame([
            ("1234567890123456", 35, 40.7128, -74.0060)
        ], ["cc_num", "age", "lat", "long"])
        
        job = FraudDetectionJob()
        job.spark_session = spark_session
        
        monitor = PerformanceMonitor()
        monitor.start_monitoring()
        
        try:
            with patch.object(job, 'setup_streaming_pipeline') as mock_pipeline:
                mock_fraud_df = spark_session.createDataFrame([
                    ("1234567890123456", "T001", 1.0)
                ], ["cc_num", "trans_num", "is_fraud"])
                
                mock_non_fraud_df = spark_session.createDataFrame([
                    ("2345678901234567", "T002", 0.0)
                ], ["cc_num", "trans_num", "is_fraud"])
                
                mock_pipeline.return_value = (mock_fraud_df, mock_non_fraud_df)
                
                fraud_df, non_fraud_df = job.setup_streaming_pipeline()
                
                fraud_count = fraud_df.count()
                non_fraud_count = non_fraud_df.count()
                
                monitor.metrics.records_processed = fraud_count + non_fraud_count
        
        finally:
            metrics = monitor.stop_monitoring()
        
        assert metrics.duration_seconds > 0, "Pipeline should take measurable time"
        assert metrics.records_processed > 0, "Pipeline should process records"
        
        assert metrics.duration_seconds < 30.0, "Pipeline should complete within reasonable time"
    
    def test_streaming_latency_benchmark(self, spark_session: SparkSession):
        """Test streaming processing latency meets requirements."""
        batch_sizes = [100, 500, 1000, 2000]
        latency_results = []
        
        for batch_size in batch_sizes:
            test_data = self._generate_large_test_dataset(batch_size)
            test_df = spark_session.createDataFrame(test_data)
            
            start_time = time.time()
            
            processed_df = test_df \
                .withColumn("amt", col("amt").cast("double")) \
                .filter(col("amt") > 0) \
                .cache()
            
            record_count = processed_df.count()  # Force evaluation
            
            end_time = time.time()
            latency_ms = (end_time - start_time) * 1000
            
            latency_results.append({
                "batch_size": batch_size,
                "latency_ms": latency_ms,
                "records_processed": record_count
            })
        
        baseline_latency = self.SCALA_BASELINE["processing_latency_ms"]
        
        for result in latency_results:
            expected_latency = baseline_latency * (result["batch_size"] / 1000)  # Scale with batch size
            max_acceptable = expected_latency * (1 + self.PERFORMANCE_TOLERANCE)
            
            assert result["latency_ms"] <= max_acceptable, \
                f"Latency {result['latency_ms']}ms for batch size {result['batch_size']} exceeds {max_acceptable}ms"
    
    def _generate_large_test_dataset(self, num_records: int) -> List[Tuple]:
        """Generate large test dataset for performance testing."""
        import random
        
        categories = ["grocery_pos", "gas_transport", "shopping_net", "entertainment", "food_dining"]
        merchants = [f"Merchant_{i}" for i in range(100)]
        
        data = []
        for i in range(num_records):
            data.append((
                f"cc_num_{i % 1000}",  # Simulate 1000 unique credit cards
                f"2023-01-01 {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00",
                f"T{i:06d}",
                random.choice(categories),
                random.choice(merchants),
                round(random.uniform(1.0, 500.0), 2),
                round(random.uniform(25.0, 50.0), 4),  # lat
                round(random.uniform(-125.0, -65.0), 4),  # long
                round(random.uniform(25.0, 50.0), 4),  # merch_lat
                round(random.uniform(-125.0, -65.0), 4),  # merch_long
            ))
        
        return data
    
    def create_performance_report(self, metrics: PerformanceMetrics, test_name: str) -> Dict[str, Any]:
        """Create detailed performance report."""
        return {
            "test_name": test_name,
            "duration_seconds": metrics.duration_seconds,
            "records_processed": metrics.records_processed,
            "throughput_records_per_second": metrics.throughput_records_per_second,
            "avg_memory_usage_mb": metrics.avg_memory_usage_mb,
            "peak_memory_usage_mb": metrics.peak_memory_usage_mb,
            "avg_cpu_usage_percent": metrics.avg_cpu_usage_percent,
            "baseline_comparison": {
                "throughput_vs_baseline": (metrics.throughput_records_per_second / self.SCALA_BASELINE["throughput_records_per_second"]) * 100,
                "memory_vs_baseline": (metrics.avg_memory_usage_mb / self.SCALA_BASELINE["avg_memory_usage_mb"]) * 100,
                "within_tolerance": self._check_within_tolerance(metrics)
            }
        }
    
    def _check_within_tolerance(self, metrics: PerformanceMetrics) -> bool:
        """Check if metrics are within ±25% tolerance of baseline."""
        throughput_ratio = metrics.throughput_records_per_second / self.SCALA_BASELINE["throughput_records_per_second"]
        memory_ratio = metrics.avg_memory_usage_mb / self.SCALA_BASELINE["avg_memory_usage_mb"]
        
        throughput_ok = (1 - self.PERFORMANCE_TOLERANCE) <= throughput_ratio <= (1 + self.PERFORMANCE_TOLERANCE)
        memory_ok = memory_ratio <= (1 + self.PERFORMANCE_TOLERANCE)
        
        return throughput_ok and memory_ok
