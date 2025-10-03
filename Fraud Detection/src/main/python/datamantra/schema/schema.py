"""
Schema Definitions Module

Migrated from: com.datamantra.creditcard.Schema.scala
Defines data schemas for transactions and customers
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, TimestampType, IntegerType
)


class Enums:
    """Field name constants matching Scala Enums"""
    
    class TransactionKafka:
        cc_num = "cc_num"
        first = "first"
        last = "last"
        trans_num = "trans_num"
        trans_date = "trans_date"
        trans_time = "trans_time"
        unix_time = "unix_time"
        category = "category"
        merchant = "merchant"
        amt = "amt"
        merch_lat = "merch_lat"
        merch_long = "merch_long"
        distance = "distance"
        age = "age"
        is_fraud = "is_fraud"
        kafka_partition = "partition"
        kafka_offset = "offset"
    
    class Customer:
        cc_num = "cc_num"
        first = "first"
        last = "last"
        gender = "gender"
        street = "street"
        city = "city"
        state = "state"
        zip = "zip"
        lat = "lat"
        long = "long"
        job = "job"
        dob = "dob"
    
    TransactionCassandra = TransactionKafka


class Schema:
    """Schema definitions for data structures"""
    
    transaction_structure_name = "transaction"
    
    transaction_schema = StructType([
        StructField(Enums.TransactionKafka.cc_num, StringType(), True),
        StructField(Enums.TransactionKafka.first, StringType(), True),
        StructField(Enums.TransactionKafka.last, StringType(), True),
        StructField(Enums.TransactionKafka.trans_num, StringType(), True),
        StructField(Enums.TransactionKafka.trans_date, StringType(), True),
        StructField(Enums.TransactionKafka.trans_time, StringType(), True),
        StructField(Enums.TransactionKafka.unix_time, LongType(), True),
        StructField(Enums.TransactionKafka.category, StringType(), True),
        StructField(Enums.TransactionKafka.merchant, StringType(), True),
        StructField(Enums.TransactionKafka.amt, DoubleType(), True),
        StructField(Enums.TransactionKafka.merch_lat, DoubleType(), True),
        StructField(Enums.TransactionKafka.merch_long, DoubleType(), True),
    ])
    
    fraud_checked_transaction_schema = StructType(
        transaction_schema.fields + [
            StructField(Enums.TransactionKafka.is_fraud, DoubleType(), True)
        ]
    )
    
    customer_structure_name = "customer"
    customer_schema = StructType([
        StructField(Enums.Customer.cc_num, StringType(), True),
        StructField(Enums.Customer.first, StringType(), True),
        StructField(Enums.Customer.last, StringType(), True),
        StructField(Enums.Customer.gender, StringType(), True),
        StructField(Enums.Customer.street, StringType(), True),
        StructField(Enums.Customer.city, StringType(), True),
        StructField(Enums.Customer.state, StringType(), True),
        StructField(Enums.Customer.zip, StringType(), True),
        StructField(Enums.Customer.lat, DoubleType(), True),
        StructField(Enums.Customer.long, DoubleType(), True),
        StructField(Enums.Customer.job, StringType(), True),
        StructField(Enums.Customer.dob, TimestampType(), True),
    ])
    
    kafka_transaction_structure_name = transaction_structure_name
    kafka_transaction_schema = StructType([
        StructField(Enums.TransactionKafka.cc_num, StringType(), True),
        StructField(Enums.TransactionKafka.first, StringType(), True),
        StructField(Enums.TransactionKafka.last, StringType(), True),
        StructField(Enums.TransactionKafka.trans_num, StringType(), True),
        StructField(Enums.TransactionKafka.trans_time, TimestampType(), True),
        StructField(Enums.TransactionKafka.category, StringType(), True),
        StructField(Enums.TransactionKafka.merchant, StringType(), True),
        StructField(Enums.TransactionKafka.amt, StringType(), True),
        StructField(Enums.TransactionKafka.merch_lat, StringType(), True),
        StructField(Enums.TransactionKafka.merch_long, StringType(), True),
    ])
