"""
Kafka offset repository for atomic offset persistence.

Provides prepared statement patterns for Kafka offset management equivalent to
KafkaOffsetRepository.scala with efficient batch operations.
"""

import logging
from typing import Tuple
from cassandra.cluster import PreparedStatement, Session
from datamantra.schema.enums import TransactionCassandra


class KafkaOffsetRepository:
    """Repository for Kafka offset persistence operations."""
    
    logger = logging.getLogger(__name__)
    
    @staticmethod
    def cql_offset_prepare(keyspace: str, table: str) -> str:
        """
        Generate CQL prepared statement for offset insertion.
        
        Equivalent to KafkaOffsetRepository.cqlOffsetPrepare() in Scala.
        Returns parameterized CQL for atomic offset persistence.
        """
        return f"""
            INSERT INTO {keyspace}.{table} (
                {TransactionCassandra.kafka_partition},
                {TransactionCassandra.kafka_offset}
            )
            VALUES (?, ?)
        """
    
    @staticmethod
    def cql_offset_bind(prepared: PreparedStatement, partition_offset: Tuple[int, int]) -> PreparedStatement:
        """
        Bind partition and offset values to prepared statement.
        
        Equivalent to KafkaOffsetRepository.cqlOffsetBind() in Scala.
        
        Args:
            prepared: Cassandra prepared statement
            partition_offset: Tuple of (partition_id, offset)
            
        Returns:
            Bound statement ready for execution
        """
        partition, offset = partition_offset
        bound = prepared.bind([partition, offset])
        return bound
