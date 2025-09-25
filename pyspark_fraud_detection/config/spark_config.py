import logging
from typing import Optional
from .config import Config

logger = logging.getLogger(__name__)


class SparkConfig:
    """Spark configuration settings."""
    
    transaction_datasource: Optional[str] = None
    customer_datasource: Optional[str] = None
    model_path: Optional[str] = None
    preprocessing_model_path: Optional[str] = None
    shutdown_marker: Optional[str] = None
    batch_interval: int = 5000
    
    @classmethod
    def load(cls):
        """Load Spark settings from configuration file."""
        logger.info("Loading Spark Settings")
        if Config.application_conf:
            cls.shutdown_marker = Config.application_conf.get('config', 'spark.shutdownPath')
            cls.batch_interval = int(Config.application_conf.get('config', 'spark.batch.interval'))
            cls.transaction_datasource = Config.local_project_dir + Config.application_conf.get('config', 'spark.transaction.datasource')
            cls.customer_datasource = Config.local_project_dir + Config.application_conf.get('config', 'spark.customer.datasource')
            cls.model_path = Config.local_project_dir + Config.application_conf.get('config', 'spark.model.path')
            cls.preprocessing_model_path = Config.local_project_dir + Config.application_conf.get('config', 'spark.model.preprocessing.path')
    
    @classmethod
    def default_setting(cls):
        """Set default Spark configuration values."""
        cls.shutdown_marker = "/tmp/shutdownmarker"
        cls.transaction_datasource = "src/main/resources/data/transactions.csv"
        cls.customer_datasource = "src/main/resources/data/customer.csv"
        cls.model_path = "src/main/resources/spark/training/RandomForestModel"
        cls.preprocessing_model_path = "src/main/resources/spark/training/PreprocessingModel"
        cls.batch_interval = 5000
    
    @classmethod
    def get_model_path(cls) -> str:
        """Get the Random Forest model path."""
        return cls.model_path or "src/main/resources/spark/training/RandomForestModel"
    
    @classmethod
    def get_preprocessing_model_path(cls) -> str:
        """Get the preprocessing model path."""
        return cls.preprocessing_model_path or "src/main/resources/spark/training/PreprocessingModel"
    
    @classmethod
    def get_checkpoint_location(cls) -> str:
        """Get the checkpoint location for streaming."""
        return "/tmp/checkpoint"
