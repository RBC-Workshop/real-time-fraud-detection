"""Schema definitions for transaction and customer data."""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, TimestampType, IntegerType
)
from .enums import TransactionColumns, CustomerColumns


class Schema:
    """Schema definitions for various data structures."""
    
    TRANSACTION_STRUCTURE_NAME = "transaction"
    
    TRANSACTION_SCHEMA = StructType([
        StructField(TransactionColumns.CC_NUM, StringType(), True),
        StructField(TransactionColumns.FIRST, StringType(), True),
        StructField(TransactionColumns.LAST, StringType(), True),
        StructField(TransactionColumns.TRANS_NUM, StringType(), True),
        StructField(TransactionColumns.TRANS_DATE, StringType(), True),
        StructField(TransactionColumns.TRANS_TIME, StringType(), True),
        StructField(TransactionColumns.UNIX_TIME, LongType(), True),
        StructField(TransactionColumns.CATEGORY, StringType(), True),
        StructField(TransactionColumns.MERCHANT, StringType(), True),
        StructField(TransactionColumns.AMT, DoubleType(), True),
        StructField(TransactionColumns.MERCH_LAT, DoubleType(), True),
        StructField(TransactionColumns.MERCH_LONG, DoubleType(), True)
    ])
    
    FRAUD_CHECKED_TRANSACTION_SCHEMA = StructType(
        TRANSACTION_SCHEMA.fields + [
            StructField(TransactionColumns.IS_FRAUD, DoubleType(), True)
        ]
    )
    
    CUSTOMER_STRUCTURE_NAME = "customer"
    CUSTOMER_SCHEMA = StructType([
        StructField(CustomerColumns.CC_NUM, StringType(), True),
        StructField(CustomerColumns.FIRST, StringType(), True),
        StructField(CustomerColumns.LAST, StringType(), True),
        StructField(CustomerColumns.GENDER, StringType(), True),
        StructField(CustomerColumns.STREET, StringType(), True),
        StructField(CustomerColumns.CITY, StringType(), True),
        StructField(CustomerColumns.STATE, StringType(), True),
        StructField(CustomerColumns.ZIP, StringType(), True),
        StructField(CustomerColumns.LAT, DoubleType(), True),
        StructField(CustomerColumns.LONG, DoubleType(), True),
        StructField(CustomerColumns.JOB, StringType(), True),
        StructField(CustomerColumns.DOB, TimestampType(), True)
    ])
    
    KAFKA_TRANSACTION_STRUCTURE_NAME = TRANSACTION_STRUCTURE_NAME
    KAFKA_TRANSACTION_SCHEMA = StructType([
        StructField(TransactionColumns.CC_NUM, StringType(), True),
        StructField(TransactionColumns.FIRST, StringType(), True),
        StructField(TransactionColumns.LAST, StringType(), True),
        StructField(TransactionColumns.TRANS_NUM, StringType(), True),
        StructField(TransactionColumns.TRANS_TIME, TimestampType(), True),
        StructField(TransactionColumns.CATEGORY, StringType(), True),
        StructField(TransactionColumns.MERCHANT, StringType(), True),
        StructField(TransactionColumns.AMT, StringType(), True),
        StructField(TransactionColumns.MERCH_LAT, StringType(), True),
        StructField(TransactionColumns.MERCH_LONG, StringType(), True)
    ])
