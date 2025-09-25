"""
Cassandra driver for PySpark Structured Streaming output.

Provides foreach sink functionality equivalent to CassandraSinkForeach.scala
for writing fraud detection results to Cassandra tables.
"""

import logging
import sys
import os
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

sys.path.append(os.path.join(os.path.dirname(__file__), '../../Fraud Detection'))

from datamantra.schema.enums import TransactionCassandra
from config import Config

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
except ImportError:
    raise ImportError("cassandra-driver package is required. Install with: pip install cassandra-driver")


class CassandraForeachWriter:
    """Foreach writer for Cassandra equivalent to CassandraSinkForeach.scala."""
    
    def __init__(self, keyspace: str, table: str):
        self.keyspace = keyspace
        self.table = table
        self.session = None
        self.logger = logging.getLogger(__name__)
    
    def open(self, partition_id: int, epoch_id: int) -> bool:
        """Open Cassandra connection for this partition."""
        try:
            cassandra_config = Config.get_cassandra_config()
            cluster = Cluster([cassandra_config.cassandra_host])
            self.session = cluster.connect()
            return True
        except Exception as e:
            self.logger.error(f"Failed to open Cassandra connection: {str(e)}")
            return False
    
    def process(self, row) -> None:
        """Process a single row and write to Cassandra."""
        if not self.session:
            return
        
        try:
            cassandra_config = Config.get_cassandra_config()
            
            if (self.table == cassandra_config.fraud_transaction_table or 
                self.table == cassandra_config.non_fraud_transaction_table):
                
                cql = f"""
                INSERT INTO {self.keyspace}.{self.table} (
                    {TransactionCassandra.cc_num},
                    {TransactionCassandra.trans_time},
                    {TransactionCassandra.trans_num},
                    {TransactionCassandra.category},
                    {TransactionCassandra.merchant},
                    {TransactionCassandra.amt},
                    {TransactionCassandra.merch_lat},
                    {TransactionCassandra.merch_long},
                    {TransactionCassandra.distance},
                    {TransactionCassandra.age},
                    {TransactionCassandra.is_fraud}
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                self.session.execute(cql, (
                    row[TransactionCassandra.cc_num],
                    row[TransactionCassandra.trans_time],
                    row[TransactionCassandra.trans_num],
                    row[TransactionCassandra.category],
                    row[TransactionCassandra.merchant],
                    row[TransactionCassandra.amt],
                    row[TransactionCassandra.merch_lat],
                    row[TransactionCassandra.merch_long],
                    row[TransactionCassandra.distance],
                    row[TransactionCassandra.age],
                    row[TransactionCassandra.is_fraud]
                ))
                
        except Exception as e:
            self.logger.error(f"Failed to process row: {str(e)}")
    
    def close(self, error: Optional[Exception]) -> None:
        """Close Cassandra connection."""
        if self.session:
            self.session.shutdown()


class CassandraDriver:
    """Cassandra driver equivalent to CassandraDriver.scala."""
    
    logger = logging.getLogger(__name__)
    
    @classmethod
    def save_foreach(cls, df: DataFrame, keyspace: str, table: str, 
                    query_name: str, mode: str = "append") -> StreamingQuery:
        """
        Save DataFrame using foreach sink equivalent to CassandraDriver.saveForeach().
        
        Creates a streaming query that writes to Cassandra using foreach writer.
        """
        cls.logger.info(f"Creating foreach sink for {keyspace}.{table}")
        
        return df.writeStream \
            .queryName(query_name) \
            .outputMode(mode) \
            .foreach(CassandraForeachWriter(keyspace, table)) \
            .start()
