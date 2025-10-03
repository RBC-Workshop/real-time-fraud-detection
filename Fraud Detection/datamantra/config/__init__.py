"""
Configuration module for DataMantra fraud detection system.

This module handles configuration loading and management for different deployment modes.
"""

from .config import Config
from .spark_config import SparkConfig
from .cassandra_config import CassandraConfig

__all__ = ["Config", "SparkConfig", "CassandraConfig"]
