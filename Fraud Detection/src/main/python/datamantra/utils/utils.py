"""
Utility Functions Module

Migrated from: com.datamantra.utils.Utils.scala
Provides utility functions for data processing
"""
import math


class Utils:
    """Utility functions"""
    
    @staticmethod
    def get_distance(lat1, lon1, lat2, lon2):
        """
        Calculate distance between two geographic coordinates using Haversine formula
        
        Args:
            lat1: Latitude of first point
            lon1: Longitude of first point
            lat2: Latitude of second point
            lon2: Longitude of second point
            
        Returns:
            Distance in kilometers
        """
        r = 6371
        lat_distance = math.radians(lat2 - lat1)
        lon_distance = math.radians(lon2 - lon1)
        a = (math.sin(lat_distance / 2) * math.sin(lat_distance / 2) +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(lon_distance / 2) * math.sin(lon_distance / 2))
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = r * c
        return distance
