"""
Spark configuration and utilities module
"""
from .spark_config import SparkConfig
from .graceful_shutdown import GracefulShutdown

__all__ = ['SparkConfig', 'GracefulShutdown']
