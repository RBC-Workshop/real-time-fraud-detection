"""
Spark Configuration Module

Migrated from: com.datamantra.spark.SparkConfig.scala
"""


class SparkConfig:
    """
    Spark configuration parameters
    """
    
    transaction_datasource = None
    customer_datasource = None
    model_path = None
    preprocessing_model_path = None
    shutdown_marker = None
    batch_interval = None
    checkpoint_location = None
    
    @classmethod
    def load(cls):
        """Load Spark settings from application configuration"""
        from ..config.config import Config
        
        cls.checkpoint_location = Config.application_conf.get_string("config.spark.checkpoint")
        cls.shutdown_marker = Config.application_conf.get_string("config.spark.shutdownPath")
        cls.batch_interval = int(Config.application_conf.get_string("config.spark.batch.interval"))
        cls.transaction_datasource = (Config.local_project_dir + 
                                     Config.application_conf.get_string("config.spark.transaction.datasource"))
        cls.customer_datasource = (Config.local_project_dir + 
                                  Config.application_conf.get_string("config.spark.customer.datasource"))
        cls.model_path = (Config.local_project_dir + 
                         Config.application_conf.get_string("config.spark.model.path"))
        cls.preprocessing_model_path = (Config.local_project_dir + 
                                       Config.application_conf.get_string("config.spark.model.preprocessing.path"))
    
    @classmethod
    def default_setting(cls):
        """Default Spark settings for local development"""
        cls.checkpoint_location = "/tmp/checkpoint"
        cls.shutdown_marker = "/tmp/shutdownmarker"
        cls.transaction_datasource = "src/main/resources/data/transactions.csv"
        cls.customer_datasource = "src/main/resources/data/customer.csv"
        cls.model_path = "src/main/resources/spark/training/RandomForestModel"
        cls.preprocessing_model_path = "src/main/resources/spark/training/PreprocessingModel"
        cls.batch_interval = 5000
