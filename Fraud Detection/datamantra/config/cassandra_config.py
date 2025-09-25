"""
Cassandra configuration management for the fraud detection system.

This module handles Cassandra-specific configuration including keyspace
and table settings equivalent to CassandraConfig.scala.
"""

import logging
from typing import Optional


class CassandraConfig:
    """Cassandra configuration manager."""
    
    logger = logging.getLogger(__name__)
    
    keyspace: Optional[str] = None
    fraud_transaction_table: Optional[str] = None
    non_fraud_transaction_table: Optional[str] = None
    kafka_offset_table: Optional[str] = None
    customer: Optional[str] = None
    cassandra_host: Optional[str] = None
    
    @classmethod
    def load(cls) -> None:
        """Load Cassandra configuration from application.conf."""
        from .config import Config
        
        cls.logger.info("Loading Cassandra Settings")
        if Config.application_conf:
            cls.keyspace = Config.application_conf.get("config.cassandra.keyspace", "creditcard")
            cls.fraud_transaction_table = Config.application_conf.get("config.cassandra.table.fraud.transaction", "fraud_transaction")
            cls.non_fraud_transaction_table = Config.application_conf.get("config.cassandra.table.non.fraud.transaction", "non_fraud_transaction")
            cls.kafka_offset_table = Config.application_conf.get("config.cassandra.table.kafka.offset", "kafka_offset")
            cls.customer = Config.application_conf.get("config.cassandra.table.customer", "customer")
            cls.cassandra_host = Config.application_conf.get("config.cassandra.host", "localhost")
        else:
            cls.logger.warning("No application configuration found, using default settings")
            cls.default_setting()
    
    @classmethod
    def default_setting(cls) -> None:
        """Set default Cassandra configuration for local development."""
        cls.keyspace = "creditcard"
        cls.fraud_transaction_table = "fraud_transaction"
        cls.non_fraud_transaction_table = "non_fraud_transaction"
        cls.kafka_offset_table = "kafka_offset"
        cls.customer = "customer"
        cls.cassandra_host = "localhost"
