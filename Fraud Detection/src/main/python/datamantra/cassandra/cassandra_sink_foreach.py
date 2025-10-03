"""
Cassandra Sink ForeachWriter Module

Migrated from: com.datamantra.cassandra.foreachSink.CassandraSinkForeach.scala
Provides custom ForeachWriter for writing streaming data to Cassandra
"""
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

from .cassandra_config import CassandraConfig
from ..schema.schema import Enums


class CassandraSinkForeach:
    """
    ForeachWriter implementation for writing streaming data to Cassandra
    
    This class writes transaction records to Cassandra tables during streaming.
    It maintains a connection per partition and uses prepared statements for efficiency.
    """
    
    def __init__(self, keyspace, table):
        """
        Initialize the Cassandra sink
        
        Args:
            keyspace: Cassandra keyspace name
            table: Cassandra table name
        """
        self.keyspace = keyspace
        self.table = table
        self.cluster = None
        self.session = None
        self.prepared_stmt = None
    
    def open(self, partition_id, epoch_id):
        """
        Open connection for a partition
        
        Args:
            partition_id: Partition identifier
            epoch_id: Epoch identifier
            
        Returns:
            True if connection is successful
        """
        try:
            self.cluster = Cluster([CassandraConfig.cassandra_host])
            self.session = self.cluster.connect(self.keyspace)
            
            if self.table in [CassandraConfig.fraud_transaction_table, 
                             CassandraConfig.non_fraud_transaction_table]:
                self.prepared_stmt = self.session.prepare(
                    f"""
                    INSERT INTO {self.keyspace}.{self.table} (
                        {Enums.TransactionCassandra.cc_num},
                        {Enums.TransactionCassandra.trans_time},
                        {Enums.TransactionCassandra.trans_num},
                        {Enums.TransactionCassandra.category},
                        {Enums.TransactionCassandra.merchant},
                        {Enums.TransactionCassandra.amt},
                        {Enums.TransactionCassandra.merch_lat},
                        {Enums.TransactionCassandra.merch_long},
                        {Enums.TransactionCassandra.distance},
                        {Enums.TransactionCassandra.age},
                        {Enums.TransactionCassandra.is_fraud}
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                )
            elif self.table == CassandraConfig.kafka_offset_table:
                self.prepared_stmt = self.session.prepare(
                    f"""
                    INSERT INTO {self.keyspace}.{self.table} (
                        {Enums.TransactionCassandra.kafka_partition},
                        {Enums.TransactionCassandra.kafka_offset}
                    )
                    VALUES (?, ?)
                    """
                )
            return True
        except Exception as e:
            print(f"Error opening connection: {e}")
            return False
    
    def process(self, row):
        """
        Process a single row and write to Cassandra
        
        Args:
            row: Row to process
        """
        try:
            if self.table in [CassandraConfig.fraud_transaction_table,
                             CassandraConfig.non_fraud_transaction_table]:
                print(f"Saving record: {row}")
                self.session.execute(
                    self.prepared_stmt,
                    (
                        row[Enums.TransactionCassandra.cc_num],
                        row[Enums.TransactionCassandra.trans_time],
                        row[Enums.TransactionCassandra.trans_num],
                        row[Enums.TransactionCassandra.category],
                        row[Enums.TransactionCassandra.merchant],
                        float(row[Enums.TransactionCassandra.amt]),
                        float(row[Enums.TransactionCassandra.merch_lat]),
                        float(row[Enums.TransactionCassandra.merch_long]),
                        float(row[Enums.TransactionCassandra.distance]),
                        float(row[Enums.TransactionCassandra.age]),
                        float(row[Enums.TransactionCassandra.is_fraud])
                    )
                )
            elif self.table == CassandraConfig.kafka_offset_table:
                print(f"Saving offset to kafka: {row}")
                self.session.execute(
                    self.prepared_stmt,
                    (
                        int(row[Enums.TransactionCassandra.kafka_partition]),
                        int(row[Enums.TransactionCassandra.kafka_offset])
                    )
                )
        except Exception as e:
            print(f"Error processing row: {e}")
    
    def close(self, error):
        """
        Close the connection
        
        Args:
            error: Error object if any error occurred
        """
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
