"""
Configuration Management Module

Migrated from: com.datamantra.config.Config.scala
Replaces Typesafe Config with Python configuration management using pyhocon
"""
import os
from pyhocon import ConfigFactory


class Config:
    """
    Configuration manager for loading Spark, Kafka, and Cassandra settings
    """
    
    application_conf = None
    run_mode = "local"
    local_project_dir = ""
    
    @classmethod
    def parse_args(cls, args):
        """
        Parse configuration from file or use default settings
        
        Args:
            args: List of command-line arguments, first arg should be config file path
        """
        if len(args) == 0:
            cls.default_setting()
        else:
            config_file = args[0]
            cls.application_conf = ConfigFactory.parse_file(config_file)
            cls.run_mode = cls.application_conf.get_string("config.mode")
            if cls.run_mode == "local":
                home_dir = os.path.expanduser("~")
                cls.local_project_dir = f"file:///{home_dir}/frauddetection/"
            cls.load_config()
    
    @classmethod
    def load_config(cls):
        """Load all configuration modules"""
        from ..cassandra.cassandra_config import CassandraConfig
        from ..kafka.kafka_config import KafkaConfig
        from ..spark.spark_config import SparkConfig
        
        CassandraConfig.load()
        KafkaConfig.load()
        SparkConfig.load()
    
    @classmethod
    def default_setting(cls):
        """Use default settings for local development"""
        from ..cassandra.cassandra_config import CassandraConfig
        from ..kafka.kafka_config import KafkaConfig
        from ..spark.spark_config import SparkConfig
        
        CassandraConfig.default_setting()
        KafkaConfig.default_setting()
        SparkConfig.default_setting()
