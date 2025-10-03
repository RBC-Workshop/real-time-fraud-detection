"""
Credit card transaction repository for atomic transaction persistence.

Provides prepared statement patterns for fraud/non-fraud transaction writes
equivalent to CreditcardTransactionRepository.scala.
"""

import logging
from pyspark.sql import Row
from cassandra.cluster import PreparedStatement
from datamantra.schema.enums import TransactionCassandra


class CreditcardTransactionRepository:
    """Repository for credit card transaction persistence operations."""
    
    logger = logging.getLogger(__name__)
    
    @staticmethod
    def cql_transaction_prepare(keyspace: str, table: str) -> str:
        """
        Generate CQL prepared statement for transaction insertion.
        
        Equivalent to CreditcardTransactionRepository.cqlTransactionPrepare() in Scala.
        Returns parameterized CQL for atomic transaction persistence.
        """
        return f"""
            INSERT INTO {keyspace}.{table} (
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
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
    @staticmethod
    def cql_transaction_bind(prepared: PreparedStatement, record: Row) -> PreparedStatement:
        """
        Bind transaction values to prepared statement.
        
        Equivalent to CreditcardTransactionRepository.cqlTransactionBind() in Scala.
        
        Args:
            prepared: Cassandra prepared statement
            record: Spark Row containing transaction data
            
        Returns:
            Bound statement ready for execution
        """
        bound = prepared.bind([
            record[TransactionCassandra.cc_num],
            record[TransactionCassandra.trans_time],
            record[TransactionCassandra.trans_num],
            record[TransactionCassandra.category],
            record[TransactionCassandra.merchant],
            record[TransactionCassandra.amt],
            record[TransactionCassandra.merch_lat],
            record[TransactionCassandra.merch_long],
            record[TransactionCassandra.distance],
            record[TransactionCassandra.age],
            record[TransactionCassandra.is_fraud]
        ])
        return bound
