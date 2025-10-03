"""
Utility functions for the fraud detection system.

This module provides utility functions equivalent to Utils.scala,
including geographic distance calculations using the Haversine formula.
"""

import math


def get_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the distance between two geographic points using the Haversine formula.
    
    Args:
        lat1: Latitude of first point in degrees
        lon1: Longitude of first point in degrees  
        lat2: Latitude of second point in degrees
        lon2: Longitude of second point in degrees
        
    Returns:
        Distance between the two points in kilometers
    """
    r = 6371  # Earth radius in kilometers
    lat_distance = math.radians(lat2 - lat1)
    lon_distance = math.radians(lon2 - lon1)
    a = (math.sin(lat_distance / 2) * math.sin(lat_distance / 2) + 
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
         math.sin(lon_distance / 2) * math.sin(lon_distance / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = r * c
    return distance
