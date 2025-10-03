"""
Cassandra integration module
"""
from .cassandra_config import CassandraConfig
from .cassandra_sink_foreach import CassandraSinkForeach

__all__ = ['CassandraConfig', 'CassandraSinkForeach']
