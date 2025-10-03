"""
Kafka Configuration Module

Migrated from: com.datamantra.kafka.KafkaConfig.scala
"""


class KafkaConfig:
    """
    Kafka configuration parameters
    """
    
    kafka_params = {}
    
    @classmethod
    def load(cls):
        """Load Kafka settings from application configuration"""
        from ..config.config import Config
        
        cls.kafka_params["topic"] = Config.application_conf.get_string("config.kafka.topic")
        cls.kafka_params["enable.auto.commit"] = Config.application_conf.get_string("config.kafka.enable.auto.commit")
        cls.kafka_params["group.id"] = Config.application_conf.get_string("config.kafka.group.id")
        cls.kafka_params["bootstrap.servers"] = Config.application_conf.get_string("config.kafka.bootstrap.servers")
        cls.kafka_params["auto.offset.reset"] = Config.application_conf.get_string("config.kafka.auto.offset.reset")
    
    @classmethod
    def default_setting(cls):
        """Default Kafka settings for local development"""
        cls.kafka_params["topic"] = "creditcardTransaction"
        cls.kafka_params["enable.auto.commit"] = "false"
        cls.kafka_params["group.id"] = "RealTime Creditcard FraudDetection"
        cls.kafka_params["bootstrap.servers"] = "localhost:9092"
        cls.kafka_params["auto.offset.reset"] = "earliest"
