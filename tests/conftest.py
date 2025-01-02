# tests/conftest.py
import pytest
import sqlite3
import os
import json
from pathlib import Path
from constants import *

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
    
    cur.execute(f"""
        CREATE TABLE merged_arena_stats{SECTION_TABLE_CON}overall (
            metadata_id INTEGER,
            timestamp TEXT,
            {COL_HEADER_FILLER} TEXT,
            allocated TEXT,
            nmalloc TEXT,
            ndalloc TEXT,
            rps_nmalloc TEXT,
            rps_ndalloc TEXT
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
    cur.execute("""
        CREATE TABLE 'stats-merged_arena_stats__bins_v1' (
            timestamp INTEGER,
            metadata_id INTEGER,
            bins INTEGER,
            size INTEGER,
            ind INTEGER,
            allocated INTEGER,
            nmalloc INTEGER,
            rps_nmalloc INTEGER,
            ndalloc INTEGER,
            rps_ndalloc INTEGER,
            nrequests INTEGER,
            rps_nrequests INTEGER,
            nshards INTEGER,
            curregs INTEGER,
            curslabs INTEGER,
            nonfull_slabs INTEGER,
            regs INTEGER,
            pgs INTEGER,
            util REAL,
            nfills INTEGER,
            rps_nfills INTEGER,
            nflushes INTEGER,
            rps_nflushes INTEGER,
            nslabs INTEGER,
            nreslabs INTEGER,
            rps_nreslabs INTEGER,
            n_lock_ops INTEGER,
            rps_n_lock_ops INTEGER,
            n_waiting INTEGER,
            rps_n_waiting INTEGER,
            n_spin_acq INTEGER,
            rps_n_spin_acq INTEGER,
            n_owner_switch INTEGER,
            rps_n_owner_switch INTEGER,
            total_wait_ns INTEGER,
            rps_total_wait_ns INTEGER,
            max_wait_ns INTEGER,
            max_n_thds INTEGER
        )
    """)
    # Insert sample data
    cur.execute(f"INSERT INTO je_metadata VALUES (1, '123456789', 'arena', 'merged_arena_stats{SECTION_TABLE_CON}overall')")
    cur.execute("INSERT INTO je_metadata VALUES (2, '123456790', 'bins', 'stats-merged_arena_stats__bins_v1')")
    cur.execute(f"INSERT INTO je_metadata VALUES (3, '123456790', 'arena', 'merged_arena_stats{SECTION_TABLE_CON}overall')")
    cur.execute("INSERT INTO je_metadata VALUES (4, '123456789', 'bins', 'bins')")
    cur.execute("INSERT INTO je_metadata VALUES (5, '123456790', 'bins', 'bins')")
    cur.execute("INSERT INTO je_metadata VALUES (6, '123456789', 'bins', 'bins_v1')")
    cur.execute("INSERT INTO je_metadata VALUES (7, '123456790', 'bins', 'bins_v1')")
    cur.execute("INSERT INTO je_metadata VALUES (8, '123456789', 'bins', 'stats-merged_arena_stats__bins_v1')")
    cur.execute("INSERT INTO je_metadata VALUES (9, '123456790', 'bins', 'stats-merged_arena_stats__bins_v1')")
    # Insert arena data with multiple timestamps
    cur.execute(f"""
        INSERT INTO merged_arena_stats{SECTION_TABLE_CON}overall VALUES 
        (1, '123456789', '0', '1000', '500', '300', '50', '30'),
        (1, '123456789', '1', '2000', '1000', '600', '100', '60'),
        (3, '123456790', '0', '1500', '600', '400', '60', '40'),
        (3, '123456790', '1', '2500', '1200', '700', '120', '70')
    """)

    # Insert bins data
    cur.execute("""
        INSERT INTO bins VALUES
        (4, '123456789', '100', '10', '2', '80'),
        (4, '123456789', '200', '20', '4', '85'),
        (5, '123456790', '150', '15', '3', '82'),
        (5, '123456790', '250', '25', '5', '87')
    """)

    cur.execute("""
        CREATE TABLE bins_v1 (
            timestamp INTEGER,
            metadata_id INTEGER,
            bins INTEGER,
            size INTEGER,
            ind INTEGER,
            allocated INTEGER,
            nmalloc INTEGER,
            rps_nmalloc INTEGER,
            ndalloc INTEGER,
            rps_ndalloc INTEGER,
            nrequests INTEGER,
            rps_9 INTEGER,
            nshards INTEGER,
            curregs INTEGER,
            curslabs INTEGER,
            nonfull_slabs INTEGER,
            regs INTEGER,
            pgs INTEGER,
            util REAL,
            nfills INTEGER,
            rps_nfills INTEGER,
            nflushes INTEGER,
            rps_nflushes INTEGER,
            nslabs INTEGER,
            nreslabs INTEGER,
            rps_nreslabs INTEGER,
            n_lock_ops INTEGER,
            rps_n_lock_ops INTEGER,
            n_waiting INTEGER,
            rps_n_waiting INTEGER,
            n_spin_acq INTEGER,
            rps_n_spin_acq INTEGER,
            n_owner_switch INTEGER,
            rps_n_owner_switch INTEGER,
            total_wait_ns INTEGER,
            rps_total_wait_ns INTEGER,
            max_wait_ns INTEGER,
            max_n_thds INTEGER
        )
    """)

    # Insert sample data
    sample_data = [
        (123456789, 7, 0, 8, 0, 9144, 3212, 50, 2069, 32, 12866, 204, 1, 1143, 9, 2, 512, 1, 0.248, 34, 0, 37, 0, 11, 2, 0, 7693, 122, 0, 0, 0, 0, 91, 1, 0, 0, 0, 0),
        (123456790, 6, 1, 16, 1, 194336, 15690, 249, 3544, 56, 19194, 304, 1, 12146, 54, 7, 256, 1, 0.878, 170, 2, 63, 1, 70, 10, 0, 7920, 125, 0, 0, 0, 0, 156, 2, 0, 0, 0, 0),
        # ... add more sample rows as needed ...
    ]

    cur.executemany("""
        INSERT INTO bins_v1 VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, sample_data)
    # conn.commit()
    

    # Insert sample data
    sample_data = [
        (0, 7, 0, 8, 0, 9144, 3212, 50, 2069, 32, 12866, 204, 1, 1143, 9, 2, 512, 1, 0.248, 34, 0, 37, 0, 11, 2, 0, 7693, 122, 0, 0, 0, 0, 91, 1, 0, 0, 0, 0),
        (0, 7, 1, 16, 1, 194336, 15690, 249, 3544, 56, 19194, 304, 1, 12146, 54, 7, 256, 1, 0.878, 170, 2, 63, 1, 70, 10, 0, 7920, 125, 0, 0, 0, 0, 156, 2, 0, 0, 0, 0),
        # ... add more sample rows as needed ...
    ]

    cur.executemany("""
        INSERT INTO 'stats-merged_arena_stats__bins_v1' VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, sample_data)
    conn.commit()
    conn.close()
    return test_db_path