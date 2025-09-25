"""Data reader utilities for Cassandra and other sources."""

import logging
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


class DataReader:
    """Utility class for reading data from various sources."""
    
    @staticmethod
    def read_from_cassandra(keyspace: str, table: str, spark: SparkSession) -> DataFrame:
        """
        Read data from Cassandra table.
        
        Args:
            keyspace: Cassandra keyspace
            table: Table name
            spark: SparkSession instance
            
        Returns:
            DataFrame with data from Cassandra
        """
        return (spark.read
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", keyspace)
                .option("table", table)
                .load())
