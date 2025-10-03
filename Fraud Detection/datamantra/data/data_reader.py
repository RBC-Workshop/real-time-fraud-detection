"""
Data reading functionality for the fraud detection system.

This module provides data loading capabilities equivalent to DataReader.scala,
including CSV reading with schema validation and Cassandra connectivity.
"""

import logging
from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.rdd import RDD


class DataReader:
    """Data reader for various data sources."""
    
    logger = logging.getLogger(__name__)
    
    @staticmethod
    def read(transaction_datasource: str, schema: StructType, spark_session: SparkSession) -> DataFrame:
        """
        Read CSV data with schema validation.
        
        Args:
            transaction_datasource: Path to the CSV file
            schema: Spark SQL schema for validation
            spark_session: Spark session instance
            
        Returns:
            DataFrame with loaded and validated data
        """
        return spark_session.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(transaction_datasource)
    
    @staticmethod
    def read_from_cassandra(keyspace: str, table: str, spark_session: SparkSession) -> DataFrame:
        """
        Read data from Cassandra table.
        
        Args:
            keyspace: Cassandra keyspace name
            table: Cassandra table name
            spark_session: Spark session instance
            
        Returns:
            DataFrame with data from Cassandra
        """
        return spark_session.read \
            .format("org.apache.spark.sql.cassandra") \
            .options({"keyspace": keyspace, "table": table, "pushdown": "true"}) \
            .load()
    
    @staticmethod
    def get_offset(rdd: RDD, spark_session: SparkSession) -> DataFrame:
        """
        Extract Kafka offset information from RDD.
        
        Args:
            rdd: RDD containing Kafka data with offset ranges
            spark_session: Spark session instance
            
        Returns:
            DataFrame with partition and offset information
        """
        try:
            from pyspark.streaming.kafka010 import HasOffsetRanges
            
            offset_ranges = rdd.offsetRanges
            offset_data = [(offset_range.partition, offset_range.untilOffset) 
                          for offset_range in offset_ranges]
            
            return spark_session.createDataFrame(offset_data, ["partition", "offset"])
        except ImportError:
            raise ImportError("pyspark.streaming.kafka010 module is required for Kafka offset handling")
