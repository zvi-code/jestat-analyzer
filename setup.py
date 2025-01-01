# setup.py
from setuptools import setup, find_packages

setup(
    name="je-analyzer",
    version="1.0.0",
    packages=find_packages(include=['src', 'src.*']),  # Changed this line
    install_requires=[],
    extras_require={
        'test': [
            'pytest>=6.0',
            'pytest-cov>=2.0',
            'pytest-mock>=3.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'je-analyze=src.cli:main',
        ],
    },
)