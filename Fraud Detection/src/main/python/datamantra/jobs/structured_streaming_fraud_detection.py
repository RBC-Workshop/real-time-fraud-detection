"""
Structured Streaming Fraud Detection Job

Migrated from: com.datamantra.spark.jobs.RealTimeFraudDetection.StructuredStreamingFraudDetection.scala

This job performs real-time fraud detection on credit card transactions using:
- Kafka as the streaming source
- Spark Structured Streaming for processing
- ML models (preprocessing pipeline and RandomForest) for prediction
- Cassandra as the sink for storing predictions
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, broadcast, udf, round as spark_round, 
    datediff, current_date, to_date, to_timestamp
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel

from datamantra.config.config import Config
from datamantra.cassandra.cassandra_config import CassandraConfig
from datamantra.cassandra.cassandra_sink_foreach import CassandraSinkForeach
from datamantra.kafka.kafka_source import KafkaSource
from datamantra.spark.spark_config import SparkConfig
from datamantra.spark.graceful_shutdown import GracefulShutdown
from datamantra.utils.utils import Utils


def main():
    """
    Main function for Structured Streaming Fraud Detection job
    """
    args = sys.argv[1:]
    Config.parse_args(args)
    
    spark = SparkSession.builder \
        .appName("PySpark Structured Streaming Fraud Detection") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", SparkConfig.checkpoint_location) \
        .config("spark.cassandra.connection.host", CassandraConfig.cassandra_host) \
        .getOrCreate()
    
    print("Reading customer data from Cassandra")
    customer_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace=CassandraConfig.keyspace, table=CassandraConfig.customer) \
        .load()
    
    customer_age_df = customer_df.withColumn(
        "age",
        (datediff(current_date(), to_date(col("dob"))) / 365).cast(IntegerType())
    )
    customer_age_df.cache()
    
    print("Reading stream from Kafka")
    raw_stream = KafkaSource.read_stream(spark)
    
    transaction_stream = raw_stream \
        .selectExpr("transaction.*", "partition", "offset") \
        .withColumn("amt", lit(col("amt")).cast(DoubleType())) \
        .withColumn("merch_lat", lit(col("merch_lat")).cast(DoubleType())) \
        .withColumn("merch_long", lit(col("merch_long")).cast(DoubleType())) \
        .drop("first") \
        .drop("last")
    
    distance_udf = udf(Utils.get_distance, DoubleType())
    
    spark.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")
    
    processed_transaction_df = transaction_stream \
        .join(broadcast(customer_age_df), "cc_num") \
        .withColumn(
            "distance",
            lit(spark_round(
                distance_udf(
                    col("lat"),
                    col("long"),
                    col("merch_lat"),
                    col("merch_long")
                ),
                2
            ))
        ) \
        .select(
            col("cc_num"),
            col("trans_num"),
            to_timestamp(col("trans_time"), "yyyy-MM-dd HH:mm:ss").alias("trans_time"),
            col("category"),
            col("merchant"),
            col("amt"),
            col("merch_lat"),
            col("merch_long"),
            col("distance"),
            col("age"),
            col("partition"),
            col("offset")
        )
    
    print(f"Loading preprocessing model from {SparkConfig.preprocessing_model_path}")
    preprocessing_model = PipelineModel.load(SparkConfig.preprocessing_model_path)
    feature_transaction_df = preprocessing_model.transform(processed_transaction_df)
    
    print(f"Loading RandomForest model from {SparkConfig.model_path}")
    random_forest_model = RandomForestClassificationModel.load(SparkConfig.model_path)
    prediction_df = random_forest_model.transform(feature_transaction_df) \
        .withColumnRenamed("prediction", "is_fraud")
    
    fraud_prediction_df = prediction_df.filter(col("is_fraud") == 1.0)
    non_fraud_prediction_df = prediction_df.filter(col("is_fraud") != 1.0)
    
    print("Starting streaming query for fraud transactions")
    fraud_query = fraud_prediction_df \
        .writeStream \
        .foreach(CassandraSinkForeach(CassandraConfig.keyspace, 
                                      CassandraConfig.fraud_transaction_table)) \
        .queryName("fraudQuery") \
        .outputMode("append") \
        .start()
    
    print("Starting streaming query for non-fraud transactions")
    non_fraud_query = non_fraud_prediction_df \
        .writeStream \
        .foreach(CassandraSinkForeach(CassandraConfig.keyspace,
                                       CassandraConfig.non_fraud_transaction_table)) \
        .queryName("nonFraudQuery") \
        .outputMode("append") \
        .start()
    
    print("Handling graceful shutdown")
    GracefulShutdown.handle_graceful_shutdown(1000, [fraud_query, non_fraud_query], spark)


if __name__ == "__main__":
    main()
