# tests/conftest.py
import pytest
import sqlite3
import os
import json
from pathlib import Path

@pytest.fixture
def test_db_path(tmp_path):
    """Provide a temporary database path"""
    return str(tmp_path / "test.db")

@pytest.fixture
def sample_db(test_db_path):
    """Create a sample database with test data"""
    conn = sqlite3.connect(test_db_path)
    cur = conn.cursor()
    
    # Create tables
    cur.execute("""
        CREATE TABLE je_metadata (
            id INTEGER PRIMARY KEY,
            timestamp TEXT,
            section TEXT,
            table_name TEXT
        )
    """)
    
    cur.execute("""
        CREATE TABLE arenas_0_overall (
            metadata_id INTEGER,
            timestamp TEXT,
            primary_0 TEXT,
            allocated_0 TEXT,
            nmalloc_1 TEXT,
            ndalloc_3 TEXT,
            rps_2 TEXT,
            rps_4 TEXT
        )
    """)
    
    # Insert sample data - now with two distinct rows
    cur.execute("INSERT INTO je_metadata VALUES (1, '123456789', 'arena', 'arenas_0.overall')")
    cur.execute("""
        INSERT INTO arenas_0_overall VALUES 
        (1, '123456789', '0', '1000', '500', '300', '50', '30'),
        (1, '123456789', '1', '2000', '1000', '600', '100', '60')
    """)
    
    conn.commit()
    conn.close()
    return test_db_path
