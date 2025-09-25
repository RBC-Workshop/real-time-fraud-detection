"""
ML pipeline for fraud detection using PySpark ML models.

Provides model loading and prediction functionality equivalent to the
Scala ML pipeline with preprocessing and RandomForest classification.
"""

import logging
import sys
import os
from pyspark.sql import DataFrame
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel

sys.path.append(os.path.join(os.path.dirname(__file__), '../../Fraud Detection'))

from config import Config


class MLPipeline:
    """ML pipeline manager for fraud detection models."""
    
    logger = logging.getLogger(__name__)
    
    def __init__(self):
        self.preprocessing_model = None
        self.random_forest_model = None
        self._load_models()
    
    def _load_models(self) -> None:
        """Load preprocessing and RandomForest models from configured paths."""
        spark_config = Config.get_spark_config()
        
        try:
            self.logger.info(f"Loading preprocessing model from: {spark_config.preprocessing_model_path}")
            self.preprocessing_model = PipelineModel.load(spark_config.preprocessing_model_path)
            
            self.logger.info(f"Loading RandomForest model from: {spark_config.model_path}")
            self.random_forest_model = RandomForestClassificationModel.load(spark_config.model_path)
            
            self.logger.info("Successfully loaded both ML models")
            
        except Exception as e:
            self.logger.error(f"Failed to load ML models: {str(e)}")
            raise RuntimeError(f"ML model loading failed: {str(e)}")
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply ML pipeline transformation: preprocessing -> classification.
        
        Equivalent to the two-stage ML pipeline in Scala version:
        1. Apply preprocessing model for feature engineering
        2. Apply RandomForest model for fraud prediction
        3. Rename prediction column to 'is_fraud'
        """
        if not self.preprocessing_model or not self.random_forest_model:
            raise RuntimeError("ML models not loaded properly")
        
        feature_df = self.preprocessing_model.transform(df)
        
        prediction_df = self.random_forest_model.transform(feature_df)
        
        result_df = prediction_df.withColumnRenamed("prediction", "is_fraud")
        
        return result_df
