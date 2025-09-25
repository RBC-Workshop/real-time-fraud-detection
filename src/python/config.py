"""
Configuration management for PySpark fraud detection streaming pipeline.

Provides unified access to Kafka, Cassandra, and Spark configurations
equivalent to the Scala configuration system.
"""

import os
import sys
import logging
from typing import Dict, Any, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '../../Fraud Detection'))

from datamantra.config.config import Config as BaseConfig
from datamantra.config.spark_config import SparkConfig
from datamantra.config.cassandra_config import CassandraConfig


class KafkaConfig:
    """Kafka configuration manager equivalent to KafkaConfig.scala."""
    
    logger = logging.getLogger(__name__)
    
    kafka_params: Dict[str, str] = {}
    
    @classmethod
    def load(cls) -> None:
        """Load Kafka configuration from application.conf."""
        cls.logger.info("Loading Kafka Settings")
        if BaseConfig.application_conf:
            cls.kafka_params["topic"] = BaseConfig.application_conf.get("config.kafka.topic", "creditcardTransaction")
            cls.kafka_params["enable.auto.commit"] = BaseConfig.application_conf.get("config.kafka.enable.auto.commit", "false")
            cls.kafka_params["group.id"] = BaseConfig.application_conf.get("config.kafka.group.id", "RealTime Creditcard FraudDetection")
            cls.kafka_params["bootstrap.servers"] = BaseConfig.application_conf.get("config.kafka.bootstrap.servers", "localhost:9092")
            cls.kafka_params["auto.offset.reset"] = BaseConfig.application_conf.get("config.kafka.auto.offset.reset", "earliest")
        else:
            cls.default_setting()
    
    @classmethod
    def default_setting(cls) -> None:
        """Set default Kafka configuration for local development."""
        cls.kafka_params["topic"] = "creditcardTransaction"
        cls.kafka_params["enable.auto.commit"] = "false"
        cls.kafka_params["group.id"] = "RealTime Creditcard FraudDetection"
        cls.kafka_params["bootstrap.servers"] = "localhost:9092"
        cls.kafka_params["auto.offset.reset"] = "earliest"


class Config:
    """Main configuration manager for the PySpark streaming application."""
    
    @classmethod
    def initialize(cls, args: Optional[list] = None) -> None:
        """Initialize all configuration subsystems."""
        BaseConfig.parse_args(args or [])
        KafkaConfig.load()
    
    @classmethod
    def get_kafka_config(cls) -> Dict[str, str]:
        """Get Kafka configuration parameters."""
        return KafkaConfig.kafka_params.copy()
    
    @classmethod
    def get_spark_config(cls) -> SparkConfig:
        """Get Spark configuration."""
        return SparkConfig
    
    @classmethod
    def get_cassandra_config(cls) -> CassandraConfig:
        """Get Cassandra configuration."""
        return CassandraConfig
