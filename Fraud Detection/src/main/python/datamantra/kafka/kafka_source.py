"""
Kafka Source Module

Migrated from: com.datamantra.kafka.KafkaSource.scala
Provides Kafka streaming source using Structured Streaming
"""
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType

from .kafka_config import KafkaConfig
from ..schema.schema import Schema


class KafkaSource:
    """
    Kafka source for Structured Streaming
    """
    
    @staticmethod
    def read_stream(spark, starting_option="startingOffsets", partitions_and_offsets="earliest"):
        """
        Read stream from Kafka using Structured Streaming
        
        Args:
            spark: SparkSession instance
            starting_option: Starting offset option (default: "startingOffsets")
            partitions_and_offsets: Offset specification (default: "earliest")
            
        Returns:
            DataFrame with parsed transaction data including partition and offset
        """
        return (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KafkaConfig.kafka_params["bootstrap.servers"])
            .option("subscribe", KafkaConfig.kafka_params["topic"])
            .option("enable.auto.commit", KafkaConfig.kafka_params["enable.auto.commit"])
            .option("group.id", KafkaConfig.kafka_params["group.id"])
            .load()
            .withColumn(
                Schema.kafka_transaction_structure_name,
                from_json(col("value").cast(StringType()), Schema.kafka_transaction_schema)
            )
            .selectExpr(
                f"{Schema.kafka_transaction_structure_name}.*",
                "partition",
                "offset",
                "timestamp"
            )
        )
