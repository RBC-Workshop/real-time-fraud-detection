"""
Cassandra Configuration Module

Migrated from: com.datamantra.cassandra.CassandraConfig.scala
"""


class CassandraConfig:
    """
    Cassandra configuration parameters
    """
    
    keyspace = None
    fraud_transaction_table = None
    non_fraud_transaction_table = None
    kafka_offset_table = None
    customer = None
    cassandra_host = None
    
    @classmethod
    def load(cls):
        """Load Cassandra settings from application configuration"""
        from ..config.config import Config
        
        cls.keyspace = Config.application_conf.get_string("config.cassandra.keyspace")
        cls.fraud_transaction_table = Config.application_conf.get_string("config.cassandra.table.fraud.transaction")
        cls.non_fraud_transaction_table = Config.application_conf.get_string("config.cassandra.table.non.fraud.transaction")
        cls.kafka_offset_table = Config.application_conf.get_string("config.cassandra.table.kafka.offset")
        cls.customer = Config.application_conf.get_string("config.cassandra.table.customer")
        cls.cassandra_host = Config.application_conf.get_string("config.cassandra.host")
    
    @classmethod
    def default_setting(cls):
        """Default Cassandra settings for local development"""
        cls.keyspace = "creditcard"
        cls.fraud_transaction_table = "fraud_transaction"
        cls.non_fraud_transaction_table = "non_fraud_transaction"
        cls.kafka_offset_table = "kafka_offset"
        cls.customer = "customer"
        cls.cassandra_host = "localhost"
