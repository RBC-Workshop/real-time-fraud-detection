"""
Cassandra driver for PySpark Structured Streaming output.

Provides foreach sink functionality equivalent to CassandraSinkForeach.scala
for writing fraud detection results to Cassandra tables.
"""

import logging
import sys
import os
import time
import random
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

sys.path.append(os.path.join(os.path.dirname(__file__), '../../Fraud Detection'))

from datamantra.schema.enums import TransactionCassandra
from config import Config

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra import ConsistencyLevel
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
        """Open Cassandra connection for this partition with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                cassandra_config = Config.get_cassandra_config()
                cluster = Cluster([cassandra_config.cassandra_host])
                self.session = cluster.connect()
                self.session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
                return True
            except Exception as e:
                self.logger.error(f"Failed to open Cassandra connection (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt + random.uniform(0, 1))
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
                
                self._execute_with_retry(cql, (
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
                
            elif self.table == cassandra_config.kafka_offset_table:
                cql = f"""
                INSERT INTO {self.keyspace}.{self.table} (
                    {TransactionCassandra.kafka_partition},
                    {TransactionCassandra.kafka_offset}
                ) VALUES (?, ?)
                """
                
                self._execute_with_retry(cql, (
                    row[TransactionCassandra.kafka_partition],
                    row[TransactionCassandra.kafka_offset]
                ))
                
        except Exception as e:
            self.logger.error(f"Failed to process row: {str(e)}")
    
    def _execute_with_retry(self, cql: str, params: tuple, max_retries: int = 3) -> None:
        """Execute CQL with exponential backoff retry."""
        if not self.session:
            raise RuntimeError("Cassandra session not initialized")
            
        for attempt in range(max_retries):
            try:
                self.session.execute(cql, params)
                return
            except Exception as e:
                self.logger.error(f"CQL execution failed (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt + random.uniform(0, 1))
                else:
                    raise e
    
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
    
    @classmethod
    def read_offset(cls, keyspace: str, table: str, spark_session) -> tuple:
        """
        Read offset from Cassandra for Structured Streaming.
        
        Equivalent to CassandraDriver.readOffset() in Scala.
        Returns tuple of (startingOption, partitionsAndOffsets).
        """
        cls.logger.info(f"Reading offset from {keyspace}.{table}")
        
        try:
            df = spark_session.read \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", keyspace) \
                .option("table", table) \
                .option("pushdown", "true") \
                .load() \
                .select("partition", "offset")
            
            if df.rdd.isEmpty():
                return ("startingOffsets", "earliest")
            else:
                return ("startingOffsets", cls._transform_kafka_metadata_to_json(df.collect()))
                
        except Exception as e:
            cls.logger.error(f"Failed to read offset: {str(e)}")
            return ("startingOffsets", "earliest")
    
    @classmethod
    def _transform_kafka_metadata_to_json(cls, rows) -> str:
        """
        Transform Kafka metadata array to JSON format.
        
        Equivalent to CassandraDriver.transformKafkaMetadataArrayToJson() in Scala.
        Returns JSON like: {"creditTransaction":{"0":23,"1":-1}}
        """
        partition_offset = ""
        for row in rows:
            partition = row["partition"]
            offset = row["offset"]
            partition_offset += f'"{partition}":{offset}, '
        
        if partition_offset:
            partition_offset = partition_offset[:-2]
        
        partition_and_offset = f'{{"creditTransaction":{{{partition_offset}}}}}'
        cls.logger.info(f"Transformed offset: {partition_and_offset}")
        
        return partition_and_offset
    
    @classmethod
    def save_offset(cls, keyspace: str, table: str, df, spark_session) -> None:
        """
        Save offset to Cassandra for Structured Streaming.
        
        Equivalent to CassandraDriver.saveOffset() in Scala.
        """
        cls.logger.info(f"Saving offset to {keyspace}.{table}")
        
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options({"keyspace": keyspace, "table": table}) \
            .save()
    
    @classmethod
    def read_offset_dstream(cls, keyspace: str, table: str, topic: str, spark_session) -> dict:
        """
        Read offset from Cassandra for DStream processing.
        
        Equivalent to the DStream-specific CassandraDriver.readOffset() in Scala
        that returns Map[TopicPartition, Long].
        
        Args:
            keyspace: Cassandra keyspace name
            table: Kafka offset table name
            topic: Kafka topic name
            spark_session: Spark session instance
            
        Returns:
            dict mapping TopicPartition to offset, or None if no offsets exist.
            Format: {TopicPartition(topic, partition): offset}
        """
        cls.logger.info(f"Reading DStream offset from {keyspace}.{table} for topic {topic}")
        
        try:
            from pyspark.streaming.kafka import TopicAndPartition
            
            df = spark_session.read \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", keyspace) \
                .option("table", table) \
                .option("pushdown", "true") \
                .load() \
                .select("partition", "offset")
            
            if df.rdd.isEmpty():
                cls.logger.info("No offset found. Will read from earliest")
                return None
            else:
                from_offsets = {}
                for row in df.collect():
                    partition = row["partition"]
                    offset = row["offset"]
                    topic_partition = TopicAndPartition(topic, partition)
                    from_offsets[topic_partition] = offset
                    cls.logger.info(f"Loaded offset: partition={partition}, offset={offset}")
                
                return from_offsets
                
        except Exception as e:
            cls.logger.error(f"Failed to read DStream offset: {str(e)}")
            return None


class CassandraConnectionPool:
    """Connection pool for Cassandra sessions."""
    
    _instance = None
    _cluster = None
    _session = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_session(self):
        """Get or create Cassandra session."""
        if self._session is None:
            cassandra_config = Config.get_cassandra_config()
            self._cluster = Cluster([cassandra_config.cassandra_host])
            self._session = self._cluster.connect()
            self._session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
        return self._session
    
    def close(self):
        """Close connection pool."""
        if self._session:
            self._session.shutdown()
        if self._cluster:
            self._cluster.shutdown()
