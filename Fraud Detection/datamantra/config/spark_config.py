"""
Spark configuration management for the fraud detection system.

This module handles Spark-specific configuration including model paths,
batch intervals, and deployment mode settings equivalent to SparkConfig.scala.
"""

import logging
from typing import Optional
from pyspark import SparkConf

from .cassandra_config import CassandraConfig


class SparkConfig:
    """Spark configuration manager."""
    
    logger = logging.getLogger(__name__)
    
    spark_conf = SparkConf()
    
    transaction_datasource: Optional[str] = None
    customer_datasource: Optional[str] = None
    model_path: Optional[str] = None
    preprocessing_model_path: Optional[str] = None
    shutdown_marker: Optional[str] = None
    batch_interval: Optional[int] = None
    
    @classmethod
    def load(cls) -> None:
        """Load Spark configuration from application.conf."""
        from .config import Config
        
        cls.logger.info("Loading Spark Settings")
        if Config.application_conf:
            cls.spark_conf.set("spark.streaming.stopGracefullyOnShutdown", 
                              Config.application_conf.get("config.spark.gracefulShutdown", "true")) \
                         .set("spark.sql.streaming.checkpointLocation", 
                              Config.application_conf.get("config.spark.checkpoint", "/tmp/checkpoint")) \
                         .set("spark.cassandra.connection.host", 
                              Config.application_conf.get("config.cassandra.host", "localhost"))
            
            cls.shutdown_marker = Config.application_conf.get("config.spark.shutdownPath", "/tmp/shutdownmarker")
            cls.batch_interval = int(Config.application_conf.get("config.spark.batch.interval", "5000"))
            cls.transaction_datasource = (Config.local_project_dir + 
                                        Config.application_conf.get("config.spark.transaction.datasource", "data/transactions.csv"))
            cls.customer_datasource = (Config.local_project_dir + 
                                     Config.application_conf.get("config.spark.customer.datasource", "data/customer.csv"))
            cls.model_path = (Config.local_project_dir + 
                             Config.application_conf.get("config.spark.model.path", "spark/RandomForestModel"))
            cls.preprocessing_model_path = (Config.local_project_dir + 
                                          Config.application_conf.get("config.spark.model.preprocessing.path", "spark/PreprocessingModel"))
        else:
            cls.logger.warning("No application configuration found, using default settings")
            cls.default_setting()
    
    @classmethod
    def default_setting(cls) -> None:
        """Set default Spark configuration for local development."""
        cls.spark_conf.setMaster("local[*]") \
                     .set("spark.cassandra.connection.host", CassandraConfig.cassandra_host) \
                     .set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
        
        cls.shutdown_marker = "/tmp/shutdownmarker"
        cls.transaction_datasource = "src/main/resources/data/transactions.csv"
        cls.customer_datasource = "src/main/resources/data/customer.csv"
        cls.model_path = "src/main/resources/spark/training/RandomForestModel"
        cls.preprocessing_model_path = "src/main/resources/spark/training/PreprocessingModel"
        cls.batch_interval = 5000
