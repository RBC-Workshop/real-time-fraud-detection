import logging
from typing import Dict
from .config import Config

logger = logging.getLogger(__name__)


class KafkaConfig:
    """Kafka configuration settings."""
    
    kafka_params: Dict[str, str] = {}
    
    @classmethod
    def load(cls):
        """Load Kafka settings from configuration file."""
        logger.info("Loading Kafka Settings")
        if Config.application_conf:
            cls.kafka_params = {
                "topic": Config.application_conf.get('config', 'kafka.topic'),
                "enable.auto.commit": Config.application_conf.get('config', 'kafka.enable.auto.commit'),
                "group.id": Config.application_conf.get('config', 'kafka.group.id'),
                "bootstrap.servers": Config.application_conf.get('config', 'kafka.bootstrap.servers'),
                "auto.offset.reset": Config.application_conf.get('config', 'kafka.auto.offset.reset')
            }
    
    @classmethod
    def default_setting(cls):
        """Set default Kafka configuration values."""
        cls.kafka_params = {
            "topic": "creditcardTransaction",
            "enable.auto.commit": "false",
            "group.id": "RealTime Creditcard FraudDetection",
            "bootstrap.servers": "localhost:9092",
            "auto.offset.reset": "earliest"
        }
    
    @classmethod
    def get_kafka_params(cls) -> Dict[str, str]:
        """Get Kafka parameters dictionary."""
        if not cls.kafka_params:
            cls.default_setting()
        return cls.kafka_params
