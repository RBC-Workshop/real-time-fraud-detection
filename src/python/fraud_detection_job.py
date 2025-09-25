"""
Main PySpark Structured Streaming job for real-time fraud detection.

Orchestrates the complete fraud detection pipeline equivalent to
StructuredStreamingFraudDetection.scala with all transformations and outputs.
"""

import logging
import sys
import os
import signal
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, round as spark_round, to_timestamp, datediff, 
    current_date, to_date, broadcast, max as spark_max
)
from pyspark.sql.types import DoubleType, IntegerType

sys.path.append(os.path.join(os.path.dirname(__file__), '../../Fraud Detection'))

from datamantra.data.data_reader import DataReader
from config import Config
from kafka_source import KafkaSource
from ml_pipeline import MLPipeline
from cassandra_driver import CassandraDriver
from graceful_shutdown import GracefulShutdown
from utils import distance_udf


class FraudDetectionJob:
    """Main fraud detection streaming job."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark_session = None
        self.streaming_queries = []
        self.ml_pipeline = None
    
    def initialize_spark(self) -> None:
        """Initialize Spark session with proper configuration."""
        spark_config = Config.get_spark_config()
        
        self.spark_session = SparkSession.builder \
            .appName("PySpark Fraud Detection Streaming") \
            .config(conf=spark_config.spark_conf) \
            .getOrCreate()
        
        self.spark_session.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")
        
        self.logger.info("Spark session initialized successfully")
    
    def load_customer_data(self):
        """Load and prepare customer data with age calculation."""
        cassandra_config = Config.get_cassandra_config()
        
        customer_df = DataReader.read_from_cassandra(
            cassandra_config.keyspace, 
            cassandra_config.customer, 
            self.spark_session
        )
        
        customer_age_df = customer_df.withColumn(
            "age", 
            (datediff(current_date(), to_date(col("dob"))) / 365).cast(IntegerType())
        )
        
        customer_age_df.cache()
        
        return customer_age_df
    
    def setup_streaming_pipeline(self):
        """Set up the complete streaming pipeline."""
        customer_age_df = self.load_customer_data()
        
        self.ml_pipeline = MLPipeline()
        
        raw_stream = KafkaSource.read_stream(self.spark_session)
        
        transaction_stream = raw_stream \
            .selectExpr("transaction.*", "partition", "offset") \
            .withColumn("amt", col("amt").cast(DoubleType())) \
            .withColumn("merch_lat", col("merch_lat").cast(DoubleType())) \
            .withColumn("merch_long", col("merch_long").cast(DoubleType())) \
            .drop("first") \
            .drop("last")
        
        processed_transaction_df = transaction_stream \
            .join(broadcast(customer_age_df), ["cc_num"]) \
            .withColumn(
                "distance", 
                spark_round(distance_udf(
                    col("lat"), col("long"), 
                    col("merch_lat"), col("merch_long")
                ), 2)
            ) \
            .select(
                col("cc_num"), col("trans_num"),
                to_timestamp(col("trans_time"), "yyyy-MM-dd HH:mm:ss").alias("trans_time"),
                col("category"), col("merchant"), col("amt"),
                col("merch_lat"), col("merch_long"), col("distance"),
                col("age"), col("partition"), col("offset")
            )
        
        prediction_df = self.ml_pipeline.transform(processed_transaction_df)
        
        fraud_prediction_df = prediction_df.filter(col("is_fraud") == 1.0)
        non_fraud_prediction_df = prediction_df.filter(col("is_fraud") != 1.0)
        
        return fraud_prediction_df, non_fraud_prediction_df
    
    def start_streaming_queries(self, fraud_df, non_fraud_df):
        """Start streaming queries for fraud and non-fraud outputs."""
        cassandra_config = Config.get_cassandra_config()
        
        fraud_query = CassandraDriver.save_foreach(
            fraud_df, 
            cassandra_config.keyspace,
            cassandra_config.fraud_transaction_table,
            "fraudQuery",
            "append"
        )
        
        non_fraud_query = CassandraDriver.save_foreach(
            non_fraud_df,
            cassandra_config.keyspace, 
            cassandra_config.non_fraud_transaction_table,
            "nonFraudQuery",
            "append"
        )
        
        self.streaming_queries = [fraud_query, non_fraud_query]
        return self.streaming_queries
    
    def setup_graceful_shutdown(self, spark_session: SparkSession):
        """Set up graceful shutdown handling with file marker support."""
        graceful_shutdown = GracefulShutdown(self.streaming_queries, check_interval=1000)
        graceful_shutdown.handle_graceful_shutdown(spark_session)
    
    def run(self, args=None):
        """Run the complete fraud detection streaming job."""
        try:
            Config.initialize(args)
            
            self.initialize_spark()
            
            fraud_df, non_fraud_df = self.setup_streaming_pipeline()
            
            queries = self.start_streaming_queries(fraud_df, non_fraud_df)
            
            self.logger.info("Fraud detection streaming job started successfully")
            
            self.setup_graceful_shutdown(self.spark_session)
                
        except Exception as e:
            self.logger.error(f"Fraud detection job failed: {str(e)}")
            raise


if __name__ == "__main__":
    job = FraudDetectionJob()
    job.run(sys.argv[1:])
