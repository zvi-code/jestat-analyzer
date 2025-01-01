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

    # Add bins table for fragmentation analysis
    cur.execute("""
        CREATE TABLE bins (
            metadata_id INTEGER,
            timestamp TEXT,
            curregs TEXT,
            curslabs TEXT,
            nonfull_slabs TEXT,
            util TEXT
        )
    """)

    # Insert sample data
    cur.execute("INSERT INTO je_metadata VALUES (1, '123456789', 'arena', 'arenas_0.overall')")
    cur.execute("INSERT INTO je_metadata VALUES (2, '123456790', 'arena', 'arenas_0.overall')")
    
    # Insert arena data with multiple timestamps
    cur.execute("""
        INSERT INTO arenas_0_overall VALUES 
        (1, '123456789', '0', '1000', '500', '300', '50', '30'),
        (1, '123456789', '1', '2000', '1000', '600', '100', '60'),
        (2, '123456790', '0', '1500', '600', '400', '60', '40'),
        (2, '123456790', '1', '2500', '1200', '700', '120', '70')
    """)

    # Insert bins data
    cur.execute("""
        INSERT INTO bins VALUES
        (1, '123456789', '100', '10', '2', '80'),
        (1, '123456789', '200', '20', '4', '85'),
        (2, '123456790', '150', '15', '3', '82'),
        (2, '123456790', '250', '25', '5', '87')
    """)

    cur.execute("""
        CREATE TABLE bins_v1 (
            timestamp INTEGER,
            metadata_id INTEGER,
            bins_0 INTEGER,
            size_1 INTEGER,
            ind_2 INTEGER,
            allocated_3 INTEGER,
            nmalloc_4 INTEGER,
            rps_5 INTEGER,
            ndalloc_6 INTEGER,
            rps_7 INTEGER,
            nrequests_8 INTEGER,
            rps_9 INTEGER,
            nshards_10 INTEGER,
            curregs_11 INTEGER,
            curslabs_12 INTEGER,
            nonfull_slabs_13 INTEGER,
            regs_14 INTEGER,
            pgs_15 INTEGER,
            util_16 REAL,
            nfills_17 INTEGER,
            rps_18 INTEGER,
            nflushes_19 INTEGER,
            rps_20 INTEGER,
            nslabs_21 INTEGER,
            nreslabs_22 INTEGER,
            rps_23 INTEGER,
            n_lock_ops_24 INTEGER,
            rps_25 INTEGER,
            n_waiting_26 INTEGER,
            rps_27 INTEGER,
            n_spin_acq_28 INTEGER,
            rps_29 INTEGER,
            n_owner_switch_30 INTEGER,
            rps_31 INTEGER,
            total_wait_ns_32 INTEGER,
            rps_33 INTEGER,
            max_wait_ns_34 INTEGER,
            max_n_thds_35 INTEGER
        )
    """)

    # Insert sample data
    sample_data = [
        (0, 7, 0, 8, 0, 9144, 3212, 50, 2069, 32, 12866, 204, 1, 1143, 9, 2, 512, 1, 0.248, 34, 0, 37, 0, 11, 2, 0, 7693, 122, 0, 0, 0, 0, 91, 1, 0, 0, 0, 0),
        (0, 7, 1, 16, 1, 194336, 15690, 249, 3544, 56, 19194, 304, 1, 12146, 54, 7, 256, 1, 0.878, 170, 2, 63, 1, 70, 10, 0, 7920, 125, 0, 0, 0, 0, 156, 2, 0, 0, 0, 0),
        # ... add more sample rows as needed ...
    ]

    cur.executemany("""
        INSERT INTO bins_v1 VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, sample_data)

    conn.commit()
    conn.close()
    return test_db_path