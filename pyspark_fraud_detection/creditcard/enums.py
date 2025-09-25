"""Enum definitions for transaction and customer fields."""


class TransactionColumns:
    """Column names for transaction data."""
    
    CC_NUM = "cc_num"
    FIRST = "first"
    LAST = "last"
    TRANS_NUM = "trans_num"
    TRANS_DATE = "trans_date"
    TRANS_TIME = "trans_time"
    UNIX_TIME = "unix_time"
    CATEGORY = "category"
    MERCHANT = "merchant"
    AMT = "amt"
    MERCH_LAT = "merch_lat"
    MERCH_LONG = "merch_long"
    DISTANCE = "distance"
    AGE = "age"
    IS_FRAUD = "is_fraud"
    KAFKA_PARTITION = "partition"
    KAFKA_OFFSET = "offset"


class CustomerColumns:
    """Column names for customer data."""
    
    CC_NUM = "cc_num"
    FIRST = "first"
    LAST = "last"
    GENDER = "gender"
    STREET = "street"
    CITY = "city"
    STATE = "state"
    ZIP = "zip"
    LAT = "lat"
    LONG = "long"
    JOB = "job"
    DOB = "dob"


TransactionCassandraColumns = TransactionColumns
