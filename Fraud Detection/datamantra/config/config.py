"""
Main configuration management for the fraud detection system.

This module provides configuration loading functionality equivalent to Config.scala,
supporting both local and cluster deployment modes with HOCON format parsing.
"""

import os
import logging
from typing import List, Optional
from pyhocon import ConfigFactory
from pyhocon.config_tree import ConfigTree

from .spark_config import SparkConfig
from .cassandra_config import CassandraConfig


class Config:
    """Main configuration manager for the fraud detection system."""
    
    logger = logging.getLogger(__name__)
    
    application_conf: Optional[ConfigTree] = None
    run_mode: str = "local"
    local_project_dir: str = ""
    
    @classmethod
    def parse_args(cls, args: List[str]) -> None:
        """
        Parse configuration from command line arguments.
        
        Args:
            args: Command line arguments, first argument should be config file path
        """
        if len(args) == 0:
            cls.default_setting()
        else:
            cls.application_conf = ConfigFactory.parse_file(args[0])
            if cls.application_conf:
                cls.run_mode = cls.application_conf.get("config.mode", "local")
                if cls.run_mode == "local":
                    home_dir = os.path.expanduser("~")
                    cls.local_project_dir = f"file:///{home_dir}/frauddetection/"
                cls.load_config()
            else:
                cls.logger.error(f"Failed to load configuration from {args[0]}")
                cls.default_setting()
    
    @classmethod
    def load_config(cls) -> None:
        """Load configuration for all subsystems."""
        CassandraConfig.load()
        SparkConfig.load()
    
    @classmethod
    def default_setting(cls) -> None:
        """Set default configuration values for local development."""
        CassandraConfig.default_setting()
        SparkConfig.default_setting()
