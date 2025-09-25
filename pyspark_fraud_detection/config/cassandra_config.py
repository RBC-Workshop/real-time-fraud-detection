import logging
from typing import Optional
from .config import Config

logger = logging.getLogger(__name__)


class CassandraConfig:
    """Cassandra configuration settings."""
    
    keyspace: Optional[str] = None
    fraud_transaction_table: Optional[str] = None
    non_fraud_transaction_table: Optional[str] = None
    kafka_offset_table: Optional[str] = None
    customer: Optional[str] = None
    cassandra_host: Optional[str] = None
    
    KEYSPACE = "creditcard"
    FRAUD_TRANSACTION_TABLE = "fraud_transaction"
    NON_FRAUD_TRANSACTION_TABLE = "non_fraud_transaction"
    KAFKA_OFFSET_TABLE = "kafka_offset"
    CUSTOMER_TABLE = "customer"
    
    @classmethod
    def load(cls):
        """Load Cassandra settings from configuration file."""
        logger.info("Loading Cassandra Settings")
        if Config.application_conf:
            cls.keyspace = Config.application_conf.get('config', 'cassandra.keyspace')
            cls.fraud_transaction_table = Config.application_conf.get('config', 'cassandra.table.fraud.transaction')
            cls.non_fraud_transaction_table = Config.application_conf.get('config', 'cassandra.table.non.fraud.transaction')
            cls.kafka_offset_table = Config.application_conf.get('config', 'cassandra.table.kafka.offset')
            cls.customer = Config.application_conf.get('config', 'cassandra.table.customer')
            cls.cassandra_host = Config.application_conf.get('config', 'cassandra.host')
    
    @classmethod
    def default_setting(cls):
        """Set default Cassandra configuration values."""
        cls.keyspace = cls.KEYSPACE
        cls.fraud_transaction_table = cls.FRAUD_TRANSACTION_TABLE
        cls.non_fraud_transaction_table = cls.NON_FRAUD_TRANSACTION_TABLE
        cls.kafka_offset_table = cls.KAFKA_OFFSET_TABLE
        cls.customer = cls.CUSTOMER_TABLE
        cls.cassandra_host = "localhost"
    
    @classmethod
    def get_keyspace(cls) -> str:
        """Get the Cassandra keyspace."""
        return cls.keyspace or cls.KEYSPACE
    
    @classmethod
    def get_host(cls) -> str:
        """Get the Cassandra host."""
        return cls.cassandra_host or "localhost"
    
    @classmethod
    def get_fraud_table(cls) -> str:
        """Get the fraud transaction table name."""
        return cls.fraud_transaction_table or cls.FRAUD_TRANSACTION_TABLE
    
    @classmethod
    def get_non_fraud_table(cls) -> str:
        """Get the non-fraud transaction table name."""
        return cls.non_fraud_transaction_table or cls.NON_FRAUD_TRANSACTION_TABLE
