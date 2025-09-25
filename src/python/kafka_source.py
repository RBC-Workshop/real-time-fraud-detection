"""
Kafka source for PySpark Structured Streaming.

Provides Kafka stream reading functionality equivalent to KafkaSource.scala,
with JSON schema parsing and DataFrame creation.
"""

import logging
import sys
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType

sys.path.append(os.path.join(os.path.dirname(__file__), '../../Fraud Detection'))

from datamantra.schema.schemas import kafka_transaction_schema, kafka_transaction_structure_name
from config import Config


class KafkaSource:
    """Kafka source for structured streaming equivalent to KafkaSource.scala."""
    
    logger = logging.getLogger(__name__)
    
    @classmethod
    def read_stream(cls, spark_session: SparkSession, 
                   starting_option: str = "startingOffsets", 
                   partitions_and_offsets: str = "earliest") -> DataFrame:
        """
        Read stream from Kafka using Structured Streaming.
        
        Equivalent to KafkaSource.readStream() method in Scala.
        Creates DataFrame with parsed JSON transaction data.
        """
        cls.logger.info("Reading from Kafka")
        
        kafka_config = Config.get_kafka_config()
        
        kafka_df = spark_session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config["bootstrap.servers"]) \
            .option("subscribe", kafka_config["topic"]) \
            .option("enable.auto.commit", kafka_config["enable.auto.commit"]) \
            .option("group.id", kafka_config["group.id"]) \
            .load()
        
        parsed_df = kafka_df.withColumn(
            kafka_transaction_structure_name,
            from_json(col("value").cast(StringType()), kafka_transaction_schema)
        )
        
        return parsed_df
