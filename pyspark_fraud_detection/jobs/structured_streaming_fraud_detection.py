"""
PySpark Structured Streaming job for real-time fraud detection.

Converted from Scala implementation to maintain the same functionality
for detecting fraudulent credit card transactions in real-time.
"""

import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, broadcast, round as spark_round, to_timestamp, 
    current_date, datediff, to_date
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel

from ..config.config import Config
from ..config.cassandra_config import CassandraConfig
from ..config.spark_config import SparkConfig
from ..kafka.kafka_source import KafkaSource
from ..cassandra.cassandra_driver import CassandraDriver
from ..spark.data_reader import DataReader
from ..spark.graceful_shutdown import GracefulShutdown
from ..utils.utils import distance_udf

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create and configure SparkSession for fraud detection."""
    return (SparkSession.builder
            .appName("PySpark Structured Streaming Fraud Detection")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.streaming.checkpointLocation", SparkConfig.get_checkpoint_location())
            .config("spark.cassandra.connection.host", CassandraConfig.get_host())
            .config("spark.sql.autoBroadcastJoinThreshold", "52428800")
            .getOrCreate())


def main(args=None):
    """
    Main function for PySpark Structured Streaming Fraud Detection.
    
    Args:
        args: Command line arguments
    """
    if args is None:
        args = sys.argv[1:]
    
    Config.parse_args(args)
    
    spark = create_spark_session()
    
    try:
        customer_df = DataReader.read_from_cassandra(
            CassandraConfig.get_keyspace(), 
            CassandraConfig.customer or "customer", 
            spark
        )
        
        customer_age_df = customer_df.withColumn(
            "age", 
            (datediff(current_date(), to_date(col("dob"))) / 365).cast(IntegerType())
        )
        customer_age_df.cache()
        
        raw_stream = KafkaSource.read_stream(spark)
        
        transaction_stream = (raw_stream
                             .selectExpr("transaction.*", "partition", "offset")
                             .withColumn("amt", col("amt").cast(DoubleType()))
                             .withColumn("merch_lat", col("merch_lat").cast(DoubleType()))
                             .withColumn("merch_long", col("merch_long").cast(DoubleType()))
                             .drop("first")
                             .drop("last"))
        
        processed_transaction_df = (transaction_stream
                                   .join(broadcast(customer_age_df), ["cc_num"])
                                   .withColumn("distance", 
                                             spark_round(distance_udf(col("lat"), col("long"), 
                                                                    col("merch_lat"), col("merch_long")), 2))
                                   .select(col("cc_num"), col("trans_num"), 
                                          to_timestamp(col("trans_time"), "yyyy-MM-dd HH:mm:ss").alias("trans_time"),
                                          col("category"), col("merchant"), col("amt"), 
                                          col("merch_lat"), col("merch_long"), col("distance"), 
                                          col("age"), col("partition"), col("offset")))
        
        preprocessing_model = PipelineModel.load(SparkConfig.get_preprocessing_model_path())
        feature_transaction_df = preprocessing_model.transform(processed_transaction_df)
        
        random_forest_model = RandomForestClassificationModel.load(SparkConfig.get_model_path())
        prediction_df = random_forest_model.transform(feature_transaction_df).withColumnRenamed("prediction", "is_fraud")
        
        fraud_prediction_df = prediction_df.filter(col("is_fraud") == 1.0)
        non_fraud_prediction_df = prediction_df.filter(col("is_fraud") != 1.0)
        
        fraud_query = CassandraDriver.save_foreach(
            fraud_prediction_df, 
            CassandraConfig.get_keyspace(), 
            CassandraConfig.get_fraud_table(),
            "fraudQuery", 
            "append"
        )
        
        non_fraud_query = CassandraDriver.save_foreach(
            non_fraud_prediction_df, 
            CassandraConfig.get_keyspace(), 
            CassandraConfig.get_non_fraud_table(),
            "nonFraudQuery", 
            "append"
        )
        
        GracefulShutdown.handle_graceful_shutdown(1000, [fraud_query, non_fraud_query])
        
    except Exception as e:
        logger.error(f"Error in fraud detection job: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
