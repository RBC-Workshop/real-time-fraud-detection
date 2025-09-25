import os
import configparser
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class Config:
    """Main configuration class for the fraud detection pipeline."""
    
    application_conf: Optional[configparser.ConfigParser] = None
    run_mode = "local"
    local_project_dir = ""
    
    @classmethod
    def parse_args(cls, args):
        """Parse configuration from command line arguments."""
        if len(args) == 0:
            cls.default_setting()
        else:
            cls.application_conf = configparser.ConfigParser()
            cls.application_conf.read(args[0])
            cls.run_mode = cls.application_conf.get('config', 'mode')
            if cls.run_mode == "local":
                cls.local_project_dir = f"file:///{os.path.expanduser('~')}/frauddetection/"
            cls.load_config()
    
    @classmethod
    def load_config(cls):
        """Load all configuration modules."""
        from .cassandra_config import CassandraConfig
        from .kafka_config import KafkaConfig
        from .spark_config import SparkConfig
        
        CassandraConfig.load()
        KafkaConfig.load()
        SparkConfig.load()
    
    @classmethod
    def default_setting(cls):
        """Set default configuration values."""
        from .cassandra_config import CassandraConfig
        from .kafka_config import KafkaConfig
        from .spark_config import SparkConfig
        
        CassandraConfig.default_setting()
        KafkaConfig.default_setting()
        SparkConfig.default_setting()
