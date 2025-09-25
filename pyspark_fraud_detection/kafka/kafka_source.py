"""Kafka source for reading streaming transaction data."""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType
from ..config.kafka_config import KafkaConfig
from ..creditcard.schema import Schema

logger = logging.getLogger(__name__)


class KafkaSource:
    """Kafka source for structured streaming."""
    
    @staticmethod
    def read_stream(spark: SparkSession, starting_option: str = "startingOffsets", 
                   partitions_and_offsets: str = "earliest") -> DataFrame:
        """
        Read stream from Kafka using Structured Streaming.
        
        Args:
            spark: SparkSession instance
            starting_option: Starting offset option
            partitions_and_offsets: Partition and offset configuration
            
        Returns:
            DataFrame with transaction data from Kafka
        """
        logger.info("Reading from Kafka")
        kafka_params = KafkaConfig.get_kafka_params()
        
        return (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_params["bootstrap.servers"])
                .option("subscribe", kafka_params["topic"])
                .option("enable.auto.commit", kafka_params["enable.auto.commit"])
                .option("group.id", kafka_params["group.id"])
                .load()
                .withColumn(Schema.KAFKA_TRANSACTION_STRUCTURE_NAME,
                           from_json(col("value").cast(StringType()), 
                                   Schema.KAFKA_TRANSACTION_SCHEMA))
                .select("transaction.*", "partition", "offset"))
