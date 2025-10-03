"""
Graceful Shutdown Module

Migrated from: com.datamantra.spark.GracefulShutdown.scala
Provides graceful shutdown handling for Spark Streaming jobs
"""
import os
import time

from .spark_config import SparkConfig


class GracefulShutdown:
    """
    Graceful shutdown handler for Structured Streaming
    """
    
    stop_flag = False
    
    @classmethod
    def check_shutdown_marker(cls):
        """Check if shutdown marker file exists"""
        if not cls.stop_flag:
            cls.stop_flag = os.path.exists(SparkConfig.shutdown_marker)
    
    @classmethod
    def handle_graceful_shutdown(cls, check_interval_millis, streaming_queries, spark):
        """
        Handle graceful shutdown for Structured Streaming
        
        Args:
            check_interval_millis: Interval in milliseconds to check for shutdown
            streaming_queries: List of StreamingQuery objects
            spark: SparkSession instance
        """
        is_stopped = False
        
        while not is_stopped:
            print("Calling awaitAnyTermination")
            is_stopped = spark.streams.awaitAnyTermination(check_interval_millis / 1000.0)
            
            if is_stopped:
                print("Confirmed! The streaming context is stopped. Exiting application...")
            else:
                print("Streaming App is still running. Timeout...")
            
            cls.check_shutdown_marker()
            
            if not is_stopped and cls.stop_flag:
                print("Stopping streaming queries right now")
                for query in streaming_queries:
                    query.stop()
                spark.stop()
                print("Streaming queries stopped!")
