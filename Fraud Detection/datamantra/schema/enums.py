"""
Enumerations for field names in transaction and customer data structures.

This module provides field name constants for consistent data access across
the fraud detection system, equivalent to CreditcardEnum.scala.
"""


class TransactionKafka:
    """Field names for transaction data from Kafka."""
    
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
    """Field names for customer data."""
    
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
