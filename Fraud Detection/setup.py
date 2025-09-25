"""
Setup configuration for the DataMantra fraud detection package.

This setup script configures the datamantra package for installation
and distribution, supporting both development and production deployment.
"""

from setuptools import setup, find_packages

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="datamantra",
    version="1.0.0",
    author="DataMantra Team",
    author_email="team@datamantra.com",
    description="Python/PySpark fraud detection infrastructure package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/RBC-Workshop/real-time-fraud-detection",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.812",
        ],
    },
    include_package_data=True,
    package_data={
        "datamantra": ["*.conf"],
    },
    entry_points={
        "console_scripts": [
            "datamantra-config=datamantra.config.config:main",
        ],
    },
)
