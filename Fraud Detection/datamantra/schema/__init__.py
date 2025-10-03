"""
Schema module for DataMantra fraud detection system.

This module contains schema definitions and enumerations for transaction and customer data.
"""

from .schemas import (
    transaction_schema,
    fraud_checked_transaction_schema,
    customer_schema,
    kafka_transaction_schema,
    transaction_structure_name,
    customer_structure_name,
    kafka_transaction_structure_name
)
from .enums import TransactionKafka, Customer, TransactionCassandra

__all__ = [
    "transaction_schema",
    "fraud_checked_transaction_schema", 
    "customer_schema",
    "kafka_transaction_schema",
    "transaction_structure_name",
    "customer_structure_name",
    "kafka_transaction_structure_name",
    "TransactionKafka",
    "Customer",
    "TransactionCassandra"
]
