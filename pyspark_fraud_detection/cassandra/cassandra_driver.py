"""Cassandra driver for data operations and streaming queries."""

import logging
from typing import List, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import max as spark_max
from cassandra.cluster import Cluster
from ..config.cassandra_config import CassandraConfig
from .foreach_sink.cassandra_sink_foreach import CassandraSinkForeach

logger = logging.getLogger(__name__)


class CassandraDriver:
    """Driver for Cassandra operations and streaming queries."""
    
    @staticmethod
    def debug_stream(df: DataFrame, mode: str = "append"):
        """
        Debug streaming DataFrame by printing to console.
        
        Args:
            df: DataFrame to debug
            mode: Output mode
            
        Returns:
            Streaming query
        """
        return (df.writeStream
                .format("console")
                .option("truncate", "false")
                .option("numRows", "100")
                .outputMode(mode)
                .start())
    
    @staticmethod
    def save_foreach(df: DataFrame, db: str, table: str, 
                    query_name: str, mode: str):
        """
        Save DataFrame using foreach writer to Cassandra.
        
        Args:
            df: DataFrame to save
            db: Database/keyspace name
            table: Table name
            query_name: Name for the streaming query
            mode: Output mode
            
        Returns:
            Streaming query
        """
        print("Calling save_foreach")
        return (df.writeStream
                .queryName(query_name)
                .outputMode(mode)
                .foreach(CassandraSinkForeach(db, table))
                .start())
    
    @staticmethod
    def read_offset(keyspace: str, table: str, spark: SparkSession) -> Tuple[str, str]:
        """
        Read offset information from Cassandra for Structured Streaming.
        
        Args:
            keyspace: Cassandra keyspace
            table: Table containing offset information
            spark: SparkSession instance
            
        Returns:
            Tuple of (starting_option, partitions_and_offsets)
        """
        try:
            df = (spark.read
                  .format("org.apache.spark.sql.cassandra")
                  .option("keyspace", keyspace)
                  .option("table", table)
                  .option("pushdown", "true")
                  .load()
                  .select("partition", "offset"))
            
            if df.rdd.isEmpty():
                return ("startingOffsets", "earliest")
            else:
                return ("startingOffsets", CassandraDriver._transform_kafka_metadata_to_json(df.collect()))
        except Exception as e:
            logger.error(f"Error reading offset: {e}")
            return ("startingOffsets", "earliest")
    
    @staticmethod
    def _transform_kafka_metadata_to_json(rows: List) -> str:
        """
        Transform Kafka metadata array to JSON format.
        
        Args:
            rows: List of rows containing partition and offset information
            
        Returns:
            JSON string for Kafka offset configuration
        """
        partition_offset = ""
        for row in rows:
            partition_offset += f'"{row["partition"]}":{row["offset"]}, '
        
        if partition_offset:
            partition_offset = partition_offset[:-2]  # Remove trailing comma and space
        
        partition_and_offset = f'{{"creditTransaction":{{{partition_offset}}}}}'
        print(f"Offset: {partition_and_offset}")
        return partition_and_offset
    
    @staticmethod
    def save_offset(keyspace: str, table: str, df: DataFrame, spark: SparkSession):
        """
        Save offset information to Cassandra for Structured Streaming.
        
        Args:
            keyspace: Cassandra keyspace
            table: Table to save offset information
            df: DataFrame containing offset data
            spark: SparkSession instance
        """
        (df.write
         .format("org.apache.spark.sql.cassandra")
         .options({"keyspace": keyspace, "table": table})
         .save())
