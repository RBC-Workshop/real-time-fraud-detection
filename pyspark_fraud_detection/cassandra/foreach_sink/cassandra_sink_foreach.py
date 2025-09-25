"""Cassandra ForeachWriter implementation for streaming data."""

import logging
from typing import Optional
from pyspark.sql import Row
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from ..cassandra_config import CassandraConfig
from ...creditcard.enums import TransactionColumns

logger = logging.getLogger(__name__)


class CassandraSinkForeach:
    """ForeachWriter implementation for writing to Cassandra."""
    
    def __init__(self, db_name: str, table_name: str):
        """
        Initialize Cassandra sink.
        
        Args:
            db_name: Database/keyspace name
            table_name: Table name to write to
        """
        self.db_name = db_name
        self.table_name = table_name
        self.session = None
        self.cluster = None
    
    def open(self, partition_id: int, epoch_id: int) -> bool:
        """
        Open connection to Cassandra.
        
        Args:
            partition_id: Partition ID
            epoch_id: Epoch ID
            
        Returns:
            True if connection successful
        """
        try:
            self.cluster = Cluster([CassandraConfig.get_host()])
            self.session = self.cluster.connect()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            return False
    
    def process(self, row: Row):
        """
        Process a single row and write to Cassandra.
        
        Args:
            row: Row to process
        """
        try:
            if self.session is None:
                logger.error("Cassandra session is not initialized")
                return
                
            if self.table_name in [CassandraConfig.get_fraud_table(), 
                                  CassandraConfig.get_non_fraud_table()]:
                cql = self._build_transaction_cql(row)
                print(f"Saving record: {row}")
                self.session.execute(cql)
            elif self.table_name == CassandraConfig.kafka_offset_table:
                cql = self._build_offset_cql(row)
                print(f"Saving offset to kafka: {row}")
                self.session.execute(cql)
        except Exception as e:
            logger.error(f"Error processing row: {e}")
    
    def close(self, error: Optional[Exception]):
        """
        Close Cassandra connection.
        
        Args:
            error: Any error that occurred during processing
        """
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
    
    def _build_transaction_cql(self, row: Row) -> str:
        """
        Build CQL statement for transaction data.
        
        Args:
            row: Row containing transaction data
            
        Returns:
            CQL insert statement
        """
        return f"""
        INSERT INTO {self.db_name}.{self.table_name} (
            {TransactionColumns.CC_NUM},
            {TransactionColumns.TRANS_TIME},
            {TransactionColumns.TRANS_NUM},
            {TransactionColumns.CATEGORY},
            {TransactionColumns.MERCHANT},
            {TransactionColumns.AMT},
            {TransactionColumns.MERCH_LAT},
            {TransactionColumns.MERCH_LONG},
            {TransactionColumns.DISTANCE},
            {TransactionColumns.AGE},
            {TransactionColumns.IS_FRAUD}
        )
        VALUES (
            '{row[TransactionColumns.CC_NUM]}',
            '{row[TransactionColumns.TRANS_TIME]}',
            '{row[TransactionColumns.TRANS_NUM]}',
            '{row[TransactionColumns.CATEGORY]}',
            '{row[TransactionColumns.MERCHANT]}',
            {row[TransactionColumns.AMT]},
            {row[TransactionColumns.MERCH_LAT]},
            {row[TransactionColumns.MERCH_LONG]},
            {row[TransactionColumns.DISTANCE]},
            {row[TransactionColumns.AGE]},
            {row[TransactionColumns.IS_FRAUD]}
        )"""
    
    def _build_offset_cql(self, row: Row) -> str:
        """
        Build CQL statement for Kafka offset data.
        
        Args:
            row: Row containing offset data
            
        Returns:
            CQL insert statement
        """
        return f"""
        INSERT INTO {self.db_name}.{self.table_name} (
            {TransactionColumns.KAFKA_PARTITION},
            {TransactionColumns.KAFKA_OFFSET}
        )
        VALUES (
            {row[TransactionColumns.KAFKA_PARTITION]},
            {row[TransactionColumns.KAFKA_OFFSET]}
        )"""
