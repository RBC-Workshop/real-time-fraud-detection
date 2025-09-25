"""Utility functions for distance calculation and other operations."""

import math
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


def get_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two points using Haversine formula.
    
    Args:
        lat1: Latitude of first point
        lon1: Longitude of first point  
        lat2: Latitude of second point
        lon2: Longitude of second point
        
    Returns:
        Distance in kilometers
    """
    r = 6371  # Earth radius in km
    lat_distance = math.radians(lat2 - lat1)
    lon_distance = math.radians(lon2 - lon1)
    a = (math.sin(lat_distance / 2) * math.sin(lat_distance / 2) + 
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
         math.sin(lon_distance / 2) * math.sin(lon_distance / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = r * c
    return distance


distance_udf = udf(get_distance, DoubleType())
