"""
Main PySpark DStream job for real-time fraud detection.

Orchestrates the complete DStream-based fraud detection pipeline equivalent to
DstreamFraudDetection.scala with manual Kafka offset management and exactly-once semantics.
"""

import logging
import sys
import os
from typing import Optional, Dict
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, lit, round as spark_round, to_timestamp, datediff, 
    current_date, to_date, broadcast, from_json, udf
)
from pyspark.sql.types import DoubleType, IntegerType, TimestampType, StringType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, LocationStrategies, ConsumerStrategies

sys.path.append(os.path.join(os.path.dirname(__file__), '../../Fraud Detection'))

from datamantra.data.data_reader import DataReader
from datamantra.schema.schemas import kafka_transaction_schema, kafka_transaction_structure_name
from datamantra.schema.enums import TransactionCassandra
from config import Config
from ml_pipeline import MLPipeline
from cassandra_driver import CassandraDriver
from graceful_shutdown import DStreamGracefulShutdown
from utils import get_distance
from kafka_offset_repository import KafkaOffsetRepository
from creditcard_transaction_repository import CreditcardTransactionRepository

try:
    from cassandra.cluster import Cluster
    from cassandra import ConsistencyLevel
except ImportError:
    raise ImportError("cassandra-driver package is required. Install with: pip install cassandra-driver")


class DStreamFraudDetection:
    """Main DStream-based fraud detection job with manual offset management."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark_session: Optional[SparkSession] = None
        self.streaming_context: Optional[StreamingContext] = None
        self.ml_pipeline: Optional[MLPipeline] = None
        self.customer_age_df = None
        self.broadcast_config = None
    
    def initialize_spark(self) -> None:
        """Initialize Spark session with proper configuration."""
        spark_config = Config.get_spark_config()
        
        self.spark_session = SparkSession.builder \
            .appName("PySpark DStream Fraud Detection") \
            .config(conf=spark_config.spark_conf) \
            .getOrCreate()
        
        self.spark_session.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")
        
        self.logger.info("Spark session initialized successfully")
    
    def load_customer_data(self):
        """Load and prepare customer data with age calculation."""
        cassandra_config = Config.get_cassandra_config()
        
        customer_df = DataReader.read_from_cassandra(
            cassandra_config.keyspace, 
            cassandra_config.customer, 
            self.spark_session
        )
        
        self.customer_age_df = customer_df.withColumn(
            "age", 
            (datediff(current_date(), to_date(col("dob"))) / 365).cast(IntegerType())
        )
        
        self.customer_age_df.cache()
        
        self.logger.info("Customer data loaded and cached")
    
    def load_ml_models(self):
        """Load preprocessing and RandomForest ML models."""
        self.ml_pipeline = MLPipeline()
        self.logger.info("ML models loaded successfully")
    
    def create_streaming_context(self) -> StreamingContext:
        """
        Create StreamingContext with configured batch interval.
        
        Equivalent to DstreamFraudDetection main initialization in Scala.
        """
        spark_config = Config.get_spark_config()
        batch_interval_ms = spark_config.batch_interval
        
        batch_interval_sec = batch_interval_ms / 1000.0
        
        ssc = StreamingContext(self.spark_session.sparkContext, batch_interval_sec)
        
        self.logger.info(f"StreamingContext created with batch interval: {batch_interval_sec}s")
        
        return ssc
    
    def setup_kafka_stream(self, ssc: StreamingContext):
        """
        Set up Kafka DStream with manual offset management.
        
        Equivalent to DstreamFraudDetection Kafka stream setup in Scala.
        Implements conditional stream creation based on stored offsets.
        """
        kafka_config = Config.get_kafka_config()
        cassandra_config = Config.get_cassandra_config()
        
        topics = {kafka_config["topic"]}
        kafka_params = {
            "bootstrap.servers": kafka_config["bootstrap.servers"],
            "group.id": kafka_config["group.id"],
            "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
            "auto.offset.reset": kafka_config["auto.offset.reset"],
            "enable.auto.commit": kafka_config["enable.auto.commit"]
        }
        
        stored_offsets = CassandraDriver.read_offset_dstream(
            cassandra_config.keyspace,
            cassandra_config.kafka_offset_table,
            kafka_config["topic"],
            self.spark_session
        )
        
        if stored_offsets is None:
            self.logger.info("No stored offsets found. Creating stream from earliest")
            stream = KafkaUtils.createDirectStream(
                ssc,
                topics,
                kafka_params,
                locationStrategy=LocationStrategies.PreferConsistent
            )
        else:
            self.logger.info(f"Resuming from stored offsets: {stored_offsets}")
            stream = KafkaUtils.createDirectStream(
                ssc,
                topics,
                kafka_params,
                fromOffsets=stored_offsets,
                locationStrategy=LocationStrategies.PreferConsistent
            )
        
        return stream
    
    def process_stream(self, ssc: StreamingContext, stream):
        """
        Process DStream with foreachRDD transformation pipeline.
        
        Equivalent to the main processing logic in DstreamFraudDetection.scala.
        Implements:
        - JSON parsing and schema validation
        - Customer data enrichment
        - ML prediction
        - Fraud/non-fraud classification
        - Atomic Cassandra writes with offset persistence
        """
        cassandra_config = Config.get_cassandra_config()
        
        self.broadcast_config = self.spark_session.sparkContext.broadcast({
            "keyspace": cassandra_config.keyspace,
            "fraud_table": cassandra_config.fraud_transaction_table,
            "non_fraud_table": cassandra_config.non_fraud_transaction_table,
            "kafka_offset_table": cassandra_config.kafka_offset_table,
            "cassandra_host": cassandra_config.cassandra_host
        })
        
        transaction_stream = stream.map(lambda cr: (cr.value, cr.partition, cr.offset))
        
        def process_rdd(rdd):
            """Process each RDD batch with complete fraud detection pipeline."""
            if rdd.isEmpty():
                self.logger.info("Did not receive any data")
                return
            
            kafka_transaction_df = rdd.toDF(["transaction", "partition", "offset"]) \
                .withColumn(
                    kafka_transaction_structure_name,
                    from_json(col("transaction"), kafka_transaction_schema)
                ) \
                .select("transaction.*", "partition", "offset") \
                .withColumn("amt", col("amt").cast(DoubleType())) \
                .withColumn("merch_lat", col("merch_lat").cast(DoubleType())) \
                .withColumn("merch_long", col("merch_long").cast(DoubleType())) \
                .withColumn("trans_time", col("trans_time").cast(TimestampType()))
            
            distance_udf_func = udf(get_distance, DoubleType())
            
            processed_transaction_df = kafka_transaction_df \
                .join(broadcast(self.customer_age_df), ["cc_num"]) \
                .withColumn(
                    "distance",
                    spark_round(distance_udf_func(
                        col("lat"), col("long"),
                        col("merch_lat"), col("merch_long")
                    ), 2)
                )
            
            feature_transaction_df = self.ml_pipeline.preprocessing_model.transform(processed_transaction_df)
            prediction_df = self.ml_pipeline.random_forest_model.transform(feature_transaction_df) \
                .withColumnRenamed("prediction", "is_fraud")
            
            def process_partition(partition_records):
                """
                Process partition with atomic writes to Cassandra.
                
                Implements exactly-once semantics through:
                1. Idempotent writes (cc_num + trans_time is primary key)
                2. Offset persistence after data writes
                3. Prepared statements for efficiency
                """
                config = self.broadcast_config.value
                keyspace = config["keyspace"]
                fraud_table = config["fraud_table"]
                non_fraud_table = config["non_fraud_table"]
                kafka_offset_table = config["kafka_offset_table"]
                cassandra_host = config["cassandra_host"]
                
                cluster = Cluster([cassandra_host])
                session = cluster.connect()
                session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
                
                try:
                    fraud_prepared = session.prepare(
                        CreditcardTransactionRepository.cql_transaction_prepare(keyspace, fraud_table)
                    )
                    non_fraud_prepared = session.prepare(
                        CreditcardTransactionRepository.cql_transaction_prepare(keyspace, non_fraud_table)
                    )
                    offset_prepared = session.prepare(
                        KafkaOffsetRepository.cql_offset_prepare(keyspace, kafka_offset_table)
                    )
                    
                    partition_offsets = {}
                    
                    for record in partition_records:
                        is_fraud = record[TransactionCassandra.is_fraud]
                        
                        if is_fraud == 1.0:
                            bound = CreditcardTransactionRepository.cql_transaction_bind(fraud_prepared, record)
                            session.execute(bound)
                        elif is_fraud == 0.0:
                            bound = CreditcardTransactionRepository.cql_transaction_bind(non_fraud_prepared, record)
                            session.execute(bound)
                        
                        kafka_partition = record[TransactionCassandra.kafka_partition]
                        offset = record[TransactionCassandra.kafka_offset]
                        
                        if kafka_partition not in partition_offsets:
                            partition_offsets[kafka_partition] = offset
                        else:
                            partition_offsets[kafka_partition] = max(partition_offsets[kafka_partition], offset)
                    
                    for partition, offset in partition_offsets.items():
                        bound = KafkaOffsetRepository.cql_offset_bind(offset_prepared, (partition, offset))
                        session.execute(bound)
                    
                finally:
                    session.shutdown()
                    cluster.shutdown()
            
            prediction_df.foreachPartition(process_partition)
        
        transaction_stream.foreachRDD(process_rdd)
    
    def run(self, args=None):
        """
        Run the complete DStream fraud detection job.
        
        Main entry point equivalent to DstreamFraudDetection.main() in Scala.
        """
        try:
            Config.initialize(args)
            
            self.initialize_spark()
            self.load_customer_data()
            self.load_ml_models()
            
            self.streaming_context = self.create_streaming_context()
            
            kafka_stream = self.setup_kafka_stream(self.streaming_context)
            
            self.process_stream(self.streaming_context, kafka_stream)
            
            self.streaming_context.start()
            self.logger.info("DStream fraud detection job started successfully")
            
            graceful_shutdown = DStreamGracefulShutdown(self.streaming_context, check_interval=1000)
            graceful_shutdown.handle_graceful_shutdown(self.spark_session)
            
        except Exception as e:
            self.logger.error(f"DStream fraud detection job failed: {str(e)}")
            raise


if __name__ == "__main__":
    job = DStreamFraudDetection()
    job.run(sys.argv[1:])
