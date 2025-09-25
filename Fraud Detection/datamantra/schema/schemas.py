"""
Spark SQL schema definitions for transaction and customer data.

This module contains StructType schema definitions equivalent to Schema.scala,
maintaining exact field types and nullability settings.
"""

from pyspark.sql.types import StructType, StringType, DoubleType, LongType, TimestampType
from .enums import TransactionKafka, Customer


transaction_structure_name = "transaction"

transaction_schema = StructType() \
    .add(TransactionKafka.cc_num, StringType(), True) \
    .add(TransactionKafka.first, StringType(), True) \
    .add(TransactionKafka.last, StringType(), True) \
    .add(TransactionKafka.trans_num, StringType(), True) \
    .add(TransactionKafka.trans_date, StringType(), True) \
    .add(TransactionKafka.trans_time, StringType(), True) \
    .add(TransactionKafka.unix_time, LongType(), True) \
    .add(TransactionKafka.category, StringType(), True) \
    .add(TransactionKafka.merchant, StringType(), True) \
    .add(TransactionKafka.amt, DoubleType(), True) \
    .add(TransactionKafka.merch_lat, DoubleType(), True) \
    .add(TransactionKafka.merch_long, DoubleType(), True)

fraud_checked_transaction_schema = transaction_schema.add(TransactionKafka.is_fraud, DoubleType(), True)

customer_structure_name = "customer"
customer_schema = StructType() \
    .add(Customer.cc_num, StringType(), True) \
    .add(Customer.first, StringType(), True) \
    .add(Customer.last, StringType(), True) \
    .add(Customer.gender, StringType(), True) \
    .add(Customer.street, StringType(), True) \
    .add(Customer.city, StringType(), True) \
    .add(Customer.state, StringType(), True) \
    .add(Customer.zip, StringType(), True) \
    .add(Customer.lat, DoubleType(), True) \
    .add(Customer.long, DoubleType(), True) \
    .add(Customer.job, StringType(), True) \
    .add(Customer.dob, TimestampType(), True)

kafka_transaction_structure_name = transaction_structure_name
kafka_transaction_schema = StructType() \
    .add(TransactionKafka.cc_num, StringType(), True) \
    .add(TransactionKafka.first, StringType(), True) \
    .add(TransactionKafka.last, StringType(), True) \
    .add(TransactionKafka.trans_num, StringType(), True) \
    .add(TransactionKafka.trans_time, TimestampType(), True) \
    .add(TransactionKafka.category, StringType(), True) \
    .add(TransactionKafka.merchant, StringType(), True) \
    .add(TransactionKafka.amt, StringType(), True) \
    .add(TransactionKafka.merch_lat, StringType(), True) \
    .add(TransactionKafka.merch_long, StringType(), True)
