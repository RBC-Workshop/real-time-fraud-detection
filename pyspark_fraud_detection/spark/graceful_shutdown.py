"""Graceful shutdown handling for streaming queries."""

import logging
import time
import signal
import sys
from typing import List
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """Handle graceful shutdown of streaming queries."""
    
    @staticmethod
    def handle_graceful_shutdown(check_interval_ms: int, queries: List[StreamingQuery]):
        """
        Handle graceful shutdown of streaming queries.
        
        Args:
            check_interval_ms: Interval to check for shutdown signal in milliseconds
            queries: List of streaming queries to manage
        """
        def signal_handler(sig, frame):
            logger.info("Received shutdown signal, stopping streaming queries...")
            for query in queries:
                if query.isActive:
                    query.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, stopping queries...")
            for query in queries:
                if query.isActive:
                    query.stop()
