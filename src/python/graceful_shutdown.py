"""
Graceful shutdown handler for PySpark Structured Streaming.

Provides graceful shutdown functionality equivalent to GracefulShutdown.scala
with file marker support and signal handling.
"""

import logging
import os
import signal
import sys
import time
from typing import List
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), '../../Fraud Detection'))

from config import Config


class GracefulShutdown:
    """Graceful shutdown handler equivalent to GracefulShutdown.scala."""
    
    def __init__(self, queries: List[StreamingQuery], check_interval: int = 1000):
        self.logger = logging.getLogger(__name__)
        self.queries = queries
        self.check_interval = check_interval
        self.stop_flag = False
        self.shutdown_marker_path = None
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop_flag = True
    
    def _check_shutdown_marker(self) -> None:
        """Check for shutdown marker file."""
        if not self.stop_flag and self.shutdown_marker_path:
            self.stop_flag = os.path.exists(self.shutdown_marker_path)
    
    def handle_graceful_shutdown(self, spark_session: SparkSession) -> None:
        """
        Handle graceful shutdown with polling mechanism.
        
        Equivalent to GracefulShutdown.handleGracefulShutdown() in Scala.
        """
        spark_config = Config.get_spark_config()
        self.shutdown_marker_path = spark_config.shutdown_marker
        
        is_stopped = False
        
        while not is_stopped:
            self.logger.info("Calling awaitAnyTermination...")
            is_stopped = spark_session.streams.awaitAnyTermination(self.check_interval)
            
            if is_stopped:
                self.logger.info("Confirmed! The streaming context is stopped. Exiting application...")
            else:
                self.logger.info("Streaming App is still running. Timeout...")
            
            self._check_shutdown_marker()
            
            if not is_stopped and self.stop_flag:
                self.logger.info("Stopping streaming queries right now")
                for query in self.queries:
                    query.stop()
                spark_session.stop()
                self.logger.info("Streaming queries stopped!")
                break


class DStreamGracefulShutdown:
    """Graceful shutdown handler for DStream processing."""
    
    def __init__(self, ssc, check_interval: int = 1000):
        self.logger = logging.getLogger(__name__)
        self.ssc = ssc
        self.check_interval = check_interval
        self.stop_flag = False
        self.shutdown_marker_path = None
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop_flag = True
    
    def _check_shutdown_marker(self) -> None:
        """Check for shutdown marker file."""
        if not self.stop_flag and self.shutdown_marker_path:
            self.stop_flag = os.path.exists(self.shutdown_marker_path)
    
    def handle_graceful_shutdown(self, spark_session: SparkSession) -> None:
        """
        Handle graceful shutdown for DStream with polling mechanism.
        
        Equivalent to GracefulShutdown.handleGracefulShutdown() for DStream in Scala.
        Uses StreamingContext.awaitTerminationOrTimeout() instead of streams API.
        """
        spark_config = Config.get_spark_config()
        self.shutdown_marker_path = spark_config.shutdown_marker
        
        is_stopped = False
        
        while not is_stopped:
            self.logger.info("Calling awaitTerminationOrTimeout...")
            is_stopped = self.ssc.awaitTerminationOrTimeout(self.check_interval / 1000.0)
            
            if is_stopped:
                self.logger.info("Confirmed! The streaming context is stopped. Exiting application...")
            else:
                self.logger.info("Streaming App is still running. Timeout...")
            
            self._check_shutdown_marker()
            
            if not is_stopped and self.stop_flag:
                self.logger.info("Stopping streaming context right now")
                self.ssc.stop(stopSparkContext=True, stopGraceFully=True)
                spark_session.stop()
                self.logger.info("Streaming context stopped!")
                break
