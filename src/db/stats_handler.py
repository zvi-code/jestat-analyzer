# src/db/stats_handler.py
from typing import List, Dict, Any, Optional
from .base_handler import BaseDBHandler
import pandas as pd
import numpy as np
from constants import *

class StatsHandler(BaseDBHandler):
    def generate_comprehensive_report(self, window_size: int = 5, 
                                leak_threshold: float = 10.0) -> Dict:
        """Generate a comprehensive analysis report"""
        trends = self.analyze_memory_trends(window_size=window_size)
        fragmentation = self.analyze_fragmentation()
        efficiency = self.analyze_arena_efficiency()
        leaks = self.detect_potential_leaks(threshold_percent=leak_threshold)
        try:
            report = {
                'memory_trends': trends,
                'fragmentation_analysis': fragmentation,
                'arena_efficiency': efficiency,
                'potential_leaks': leaks,
                'summary': {
                    'avg_fragmentation': sum(f['fragmentation_ratio'] for f in fragmentation) / len(fragmentation) if fragmentation else 0,
                    'peak_memory': max(t['total_allocated'] for t in trends) if trends else 0,
                    'leak_incidents': sum(1 for l in leaks if l['status'] == 'Potential Leak'),
                    'efficiency_score': sum(e['dealloc_ratio'] for e in efficiency) / len(efficiency) if efficiency else 0
                }
            }
        except Exception as e:
            print(f"Error generating comprehensive report: {e}")
            return None
        return report

    def analyze_memory_trends(self, table_names: List[str] = None, window_size: int = 5) -> Dict:
        """Analyze memory allocation trends over time"""
        with self._get_cursor() as cur:
            query = f"""
            WITH arena_data AS (
                SELECT 
                    timestamp,
                    SUM(CAST(allocated AS FLOAT)) as total_allocated,
                    SUM(CAST(nmalloc AS FLOAT)) as total_allocs,
                    SUM(CAST(ndalloc AS FLOAT)) as total_deallocs
                FROM merged_arena_stats{SECTION_TABLE_CON}overall
                GROUP BY timestamp
            ),
            trend_data AS (
                SELECT 
                    timestamp,
                    total_allocated,
                    total_allocs,
                    total_deallocs,
                    AVG(total_allocated) OVER (
                        ORDER BY timestamp 
                        ROWS BETWEEN ? PRECEDING AND CURRENT ROW
                    ) as moving_avg_memory,
                    (total_allocated - LAG(total_allocated) OVER (ORDER BY timestamp)) 
                        / NULLIF(LAG(total_allocated) OVER (ORDER BY timestamp), 0) * 100 as memory_growth_rate
                FROM arena_data
            )
            SELECT * FROM trend_data
            ORDER BY timestamp
            """
            cur.execute(query, (window_size,))
            return [dict(zip([col[0] for col in cur.description], row)) 
                    for row in cur.fetchall()]
        
    def analyze_fragmentation(self) -> Dict:
        """Analyze memory fragmentation patterns"""
        with self._get_cursor() as cur:
            query = """
            WITH bin_stats AS (
                SELECT 
                    metadata_id,
                    timestamp,
                    SUM(CAST(curregs AS FLOAT)) as total_allocated_regions,
                    SUM(CAST(curslabs AS FLOAT)) as total_slabs,
                    SUM(CAST(nonfull_slabs AS FLOAT)) as total_nonfull_slabs,
                    AVG(CAST(util AS FLOAT)) as average_utilization
                FROM bins
                GROUP BY metadata_id, timestamp
            )
            SELECT 
                timestamp,
                average_utilization,
                (total_nonfull_slabs * 100.0 / NULLIF(total_slabs, 0)) as fragmentation_ratio,
                total_allocated_regions,
                total_slabs,
                total_nonfull_slabs
            FROM bin_stats
            ORDER BY timestamp
            """
            cur.execute(query)
            return [dict(zip([col[0] for col in cur.description], row)) 
                    for row in cur.fetchall()]
        
    def analyze_arena_efficiency(self) -> Dict:
        """Analyze efficiency metrics for each arena"""
        with self._get_cursor() as cur:
            query = f"""
            WITH arena_metrics AS (
                SELECT 
                    metadata_id,
                    timestamp,
                    {COL_HEADER_FILLER} as arena_id,  -- Use primary_0 as arena_id
                    SUM(CAST(allocated AS FLOAT)) as allocated,
                    SUM(CAST(nmalloc AS FLOAT)) as allocations,
                    SUM(CAST(ndalloc AS FLOAT)) as deallocations,
                    SUM(CAST(rps_nmalloc as FLOAT)) as alloc_rate,
                    SUM(CAST(rps_ndalloc as FLOAT)) as dealloc_rate
                FROM merged_arena_stats{SECTION_TABLE_CON}overall
                GROUP BY metadata_id, timestamp, {COL_HEADER_FILLER}
            )
            SELECT 
                timestamp,
                arena_id,
                allocated,
                allocations,
                deallocations,
                alloc_rate,
                dealloc_rate,
                ROUND(deallocations * 100.0 / NULLIF(allocations, 0), 2) as dealloc_ratio,
                ROUND(allocated / NULLIF(allocations, 0), 2) as avg_allocation_size
            FROM arena_metrics
            ORDER BY timestamp, arena_id
            """
            cur.execute(query)
            return [dict(zip([col[0] for col in cur.description], row)) 
                    for row in cur.fetchall()]
        
    def detect_potential_leaks(self, threshold_percent: float = 10.0) -> Dict:
        """Detect potential memory leaks based on allocation patterns"""
        with self._get_cursor() as cur:
            query = f"""
            WITH allocation_patterns AS (
                SELECT 
                    timestamp,
                    metadata_id,
                    SUM(CAST(allocated AS FLOAT)) as total_allocated,
                    SUM(CAST(nmalloc AS FLOAT)) - SUM(CAST(ndalloc AS FLOAT)) as net_allocations,
                    LAG(SUM(CAST(allocated AS FLOAT))) OVER (ORDER BY timestamp) as prev_allocated
                FROM merged_arena_stats{SECTION_TABLE_CON}overall
                GROUP BY timestamp, metadata_id
            )
            SELECT 
                timestamp,
                total_allocated,
                net_allocations,
                ROUND((total_allocated - prev_allocated) * 100.0 / 
                    NULLIF(prev_allocated, 0), 2) as growth_rate,
                CASE 
                    WHEN (total_allocated - prev_allocated) * 100.0 / 
                        NULLIF(prev_allocated, 0) > ? THEN 'Potential Leak'
                    ELSE 'Normal'
                END as status
            FROM allocation_patterns
            WHERE prev_allocated IS NOT NULL
            ORDER BY timestamp
            """
            cur.execute(query, (threshold_percent,))
            return [dict(zip([col[0] for col in cur.description], row)) 
                    for row in cur.fetchall()]
    def calculate_table_stats(self, table_name: str) -> dict:
        """Calculate comprehensive statistics for a table"""
        with self._get_cursor() as cur:
            cur.execute(f"SELECT * FROM '{table_name}' LIMIT 1")
            columns = [desc[0] for desc in cur.description]
            if not columns:
                return None

            columns2ignore = {'id', 'timestamp', 'section', 'table_name', 'metadata_id'}
            results = {}

            for col in columns:
                if col in columns2ignore:
                    continue

                # Calculate basic statistics
                cur.execute(f"""
                    SELECT 
                        MIN(CAST(TRIM({col}) AS FLOAT)),
                        MAX(CAST(TRIM({col}) AS FLOAT)),
                        AVG(CAST(TRIM({col}) AS FLOAT)),
                        SUM(CAST(TRIM({col}) AS FLOAT)),
                        COUNT(*)
                    FROM '{table_name}'
                    WHERE TRIM({col}) != ''
                """)
                min_val, max_val, avg_val, sum_val, count = cur.fetchone()

                # Print intermediate results for debugging
                print(f"Column: {col}")
                print(f"Min: {min_val}, Max: {max_val}, Avg: {avg_val}, Sum: {sum_val}, Count: {count}")

                # Calculate percentiles
                cur.execute(f"""
                    WITH sorted AS (
                        SELECT CAST(TRIM({col}) AS FLOAT) as val,
                            ROW_NUMBER() OVER (ORDER BY CAST(TRIM({col}) AS FLOAT)) as row_num,
                            COUNT(*) OVER () as cnt
                        FROM '{table_name}'
                        WHERE TRIM({col}) != ''
                    )
                    SELECT 
                        val,
                        row_num,
                        cnt
                    FROM sorted
                    ORDER BY row_num
                """)
                sorted_values = cur.fetchall()
                
                # Print sorted values for debugging
                print("Sorted values:")
                for val, row_num, cnt in sorted_values:
                    print(f"Value: {val}, Row: {row_num}, Count: {cnt}")

                # # Calculate percentiles
                # count = len(sorted_values)
                # p50 = sorted_values[count // 2 - 1][0] if count % 2 == 0 else sorted_values[count // 2][0]
                # p90 = sorted_values[min(int(count * 0.9), count - 1)][0]
                # p99 = sorted_values[min(int(count * 0.99), count - 1)][0]
                # Calculate percentiles
                count = len(sorted_values)
                if count % 2 == 0:
                    p50 = (sorted_values[count // 2 - 1][0] + sorted_values[count // 2][0]) / 2
                else:
                    p50 = sorted_values[count // 2][0]
                p90 = sorted_values[min(int(count * 0.9), count - 1)][0]
                p99 = sorted_values[min(int(count * 0.99), count - 1)][0]
                # Print calculated percentiles
                print(f"Calculated p50: {p50}, p90: {p90}, p99: {p99}")

                results[col] = {
                    'min': min_val,
                    'max': max_val,
                    'avg': avg_val,
                    'sum': sum_val,
                    'count': count,
                    'p50': p50,
                    'p90': p90,
                    'p99': p99
                }

            return results
    def print_table_stats(self, table_name: str, limit=(20, 15)) -> None:
        with self._get_cursor() as cur:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            # for (table_name,) in cur.fetchall():
            print(f"\n=== {table_name} ===")
            cur.execute(f"SELECT * FROM '{table_name}' LIMIT 1")
            columns = [desc[0] for desc in cur.description]
            
            # Pre-check numeric columns
            # numeric_cols = {col for col in columns if self.is_numeric(table_name, col)}
            numeric_cols = []
            # Create results dict for each metric
            results = {
                'SUM': {},
                'AVG': {},
                'STD': {},
                'P50': {},
                'P90': {},
                'P99': {}
            }
            columns2ignore = {'id', 'timestamp', 'section', 'table_name', 'metadata_id', 'metric', 'bins', 'size', 'large', 'extents', 'decaying', 'ind', 'Key','Value', 'id', 'name', 'regs'}
            # Calculate stats for each column
            for col in columns:
                if col in columns2ignore:
                    # if self.is_numeric(table_name, col):
                    # cur.execute(f"""
                    #     SELECT *
                    #     FROM '{table_name}'
                    # """)
                    # val = cur.fetchone()
                    cur.execute(f"""
                        SELECT CAST({col} AS TEXT) as val
                        FROM '{table_name}'
                    """)
                    val = cur.fetchone()
                    # print(f"Column: {col} - {val}")
                    for metric in ['SUM', 'AVG', 'STD', 'P50', 'P90', 'P99']:
                        results[metric][col] = f"{val[0]}" if val else "N/A"
                    continue
                try:
                    # if self.is_numeric(table_name, col):
                    cur.execute(f"""
                        SELECT 
                            SUM(CAST({col} as FLOAT)) as sum,
                            AVG(CAST({col} as FLOAT)) as avg,
                            SQRT(AVG(CAST({col} as FLOAT) * CAST({col} as FLOAT)) - 
                                    AVG(CAST({col} as FLOAT)) * AVG(CAST({col} as FLOAT))) as std
                        FROM '{table_name}'
                        WHERE CAST({col} AS TEXT) GLOB '*[0-9.]*' 
                        AND CAST({col} AS TEXT) NOT GLOB '*[A-Za-z]*'
                        GROUP BY timestamp
                    """)
                    sum_avg_std = cur.fetchone()
                    
                    if sum_avg_std[0] is not None:
                        results['SUM'][col] = f"{sum_avg_std[0]:.2f}"
                        results['AVG'][col] = f"{sum_avg_std[1]:.2f}"
                        results['STD'][col] = f"{sum_avg_std[2]:.2f}"
                        
                        # Calculate percentiles
                        for p, metric in [(50, 'P50'), (90, 'P90'), (99, 'P99')]:
                            cur.execute(f"""
                                WITH sorted AS (
                                    SELECT CAST({col} as FLOAT) as val
                                    FROM '{table_name}'
                                    WHERE CAST({col} AS TEXT) GLOB '*[0-9.]*' 
                                    AND CAST({col} AS TEXT) NOT GLOB '*[A-Za-z]*'
                                    ORDER BY CAST({col} as FLOAT)
                                )
                                SELECT val
                                FROM sorted
                                LIMIT 1
                                OFFSET (SELECT COUNT(*) * {p} / 100 - 1 FROM sorted)
                            """)
                            pvalue = cur.fetchone()
                            results[metric][col] = f"{pvalue[0]:.2f}" if pvalue else "N/A"
                        numeric_cols.append(col)
                        continue
                    for metric in results:
                        results[metric][col] = "N/A"
                            
                except:
                    for metric in results:
                        results[metric][col] = "N/A"
                    continue
            if not numeric_cols:
                print("No numeric columns found")
                return
            columns = columns[:limit[1]]  # Limit columns
            # Print table
            col_width = max(15, max(len(col) for col in columns))
            metric_width = 8
            
            # Header
            print(" " * metric_width + " | " + " | ".join(f"{col:<{col_width}}" for col in columns))
            print("-" * metric_width + "-+-" + "-+-".join("-" * col_width for _ in columns))
            
            # Data rows
            for metric in ['SUM', 'AVG', 'STD', 'P50', 'P90', 'P99']:
                row = f"{metric:<{metric_width}} | " + " | ".join(f"{results[metric].get(col, 'N/A'):<{col_width}}" for col in columns)
                print(row)     
    def analyze_arenas_activity(self, table_names: List[str] = None, timestamp: str = None):
        with self._get_cursor() as cur:
            required_columns = {
                'metadata_id': True,
                'allocated': True,
                'nmalloc': True,
                'ndalloc': True,
                'rps_nmalloc': True,
                'rps_ndalloc': True
            }
            primary_columns = ['bins', f'{COL_HEADER_FILLER}', 'large', 'extents','decaying']
            def validate_table(table):
                cur.execute(f'PRAGMA table_info("{table}")')
                columns = {row[1] for row in cur.fetchall()}
                if all(col in columns for col in required_columns):
                    for col in primary_columns:
                        if col in columns:
                            return (table, col)
                return None

            # Get and validate tables
            if table_names:
                all_tables = table_names
            else:
                cur.execute(f"SELECT name FROM sqlite_master WHERE type='table'")
                all_tables = cur.fetchall()
            arena_tables = [validate_table(t) for t in all_tables]
            arena_tables = [t for t in arena_tables if t]
            if not arena_tables:
                for table in all_tables:
                    cur.execute(f'PRAGMA table_info("{table}")')
                    columns = {row[1] for row in cur.fetchall()}
                    print(f"\n\nTable {table} does not have required \ncolumns={columns}\nrequired_columns={required_columns}\n\n")
                print(f"No valid arena tables found all_tables = {all_tables} table_names={table_names}")
                return        
            # Construct UNION query for arena data
            union_queries = []
            for (table,prim_col) in arena_tables:
                union_queries.append(f"""
                SELECT 
                    m.timestamp,
                    t.metadata_id,
                    '{table}' as table_name,
                    t.{prim_col} as row_name, 
                    t.allocated as allocated,
                    t.nmalloc as nmalloc,
                    t.ndalloc as ndalloc,
                    t.rps_nmalloc as alloc_rps,
                    t.rps_ndalloc dealloc_rps
                FROM '{table}' t
                JOIN je_metadata m ON t.metadata_id = m.id
                {f"WHERE m.timestamp = '{timestamp}'" if timestamp else ""}
                """)

            # Main analysis query
            query = f"""
            WITH arena_data AS ({' UNION ALL '.join(union_queries)}),
            arena_stats AS (
                SELECT 
                    timestamp,
                    metadata_id,
                    CAST(SUBSTR(table_name, 8, 
                        INSTR(SUBSTR(table_name, 8), '{SECTION_TABLE_CON}') - 1) AS INTEGER) as table_type,
                    CAST(SUBSTR(table_name, 
                        8 + INSTR(SUBSTR(table_name, 8), '{SECTION_TABLE_CON}'), 
                        INSTR(table_name, '{SECTION_TABLE_CON}') - (8 + INSTR(SUBSTR(table_name, 8), '{SECTION_TABLE_CON}'))) 
                        AS INTEGER) as arena_id,
                    SUM(allocated) as total_allocated,
                    SUM(CASE WHEN row_name = 0 THEN allocated ELSE 0 END) as small_allocated,
                    SUM(CASE WHEN row_name = 1 THEN allocated ELSE 0 END) as large_allocated,
                    SUM(nmalloc) as total_allocs,
                    SUM(ndalloc) as total_deallocs,
                    SUM(alloc_rps) as alloc_rps,
                    SUM(dealloc_rps) as dealloc_rps
                FROM arena_data
                GROUP BY timestamp, metadata_id, arena_id
            )
            SELECT 
                timestamp,
                metadata_id,
                arena_id,
                total_allocated,
                ROUND(total_allocated * 100.0 / NULLIF(SUM(total_allocated) 
                    OVER (PARTITION BY timestamp, metadata_id), 0), 2) as memory_percent,
                ROUND(small_allocated * 100.0 / NULLIF(total_allocated, 0), 2) as small_percent,
                ROUND(large_allocated * 100.0 / NULLIF(total_allocated, 0), 2) as large_percent,
                total_allocs,
                total_deallocs,
                alloc_rps,
                dealloc_rps
            FROM arena_stats 
            ORDER BY timestamp, metadata_id, total_allocated DESC
            """
            try:
                cur.execute(query)
            except Exception as e:
                print(f"Error executing query: {e} - {query}")
                return
            headers = ["Timestamp", "MetaID", "Arena", "Total Mem", "Mem%", "Small%", "Large%", 
                    "Allocs", "Deallocs", "Alloc RPS", "Dealloc RPS"]
            rows = cur.fetchall()
            
            # Group by timestamp and metadata_id
            current_ts = None
            current_meta = None
            grouped_rows = []
            
            for row in rows:
                if current_ts != row[0] or current_meta != row[1]:
                    if grouped_rows:
                        print(f"\n=== Timestamp: {current_ts}, MetaID: {current_meta} ===")
                        self.formatter.print_table(headers, grouped_rows)
                    current_ts = row[0]
                    current_meta = row[1]
                    grouped_rows = []
                grouped_rows.append(row)
                
            if grouped_rows:
                print(f"\n=== Timestamp: {current_ts}, MetaID: {current_meta} ===")
                self.formatter.print_table(headers, grouped_rows)
    def combine_stats(self):
        query = """
        SELECT d.timestamp, d.metadata_id, d.decaying, d.time, d.npages, d.sweeps, d.madvises, d.purged,
            b.total_pages,
            k.active, k.mapped, k.retained
        FROM MERGED_ARENA_STATS__DECAYING d
        LEFT JOIN (
            SELECT timestamp, SUM(curslabs * pgs) as total_pages
            FROM MERGED_ARENA_STATS__BINS_V1
            GROUP BY timestamp
        ) b ON d.timestamp = b.timestamp
        LEFT JOIN (
            SELECT timestamp, 
                MAX(CASE WHEN Key = 'active' THEN Value END) as active,
                MAX(CASE WHEN Key = 'mapped' THEN Value END) as mapped,
                MAX(CASE WHEN Key = 'retained' THEN Value END) as retained
            FROM MERGED_ARENA_STATS__KEY-VALUE
            WHERE Key IN ('active', 'mapped', 'retained')
            GROUP BY timestamp
        ) k ON d.timestamp = k.timestamp
        """
        
        result = self.execute_query(query)
        return result

    def execute_query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        return {'columns': columns, 'data': data}
    # def analyze_arenas_activity(self, table_names: List[str] = None, timestamp: str = None) -> List[dict]:
    #     with self._get_cursor() as cur:
    #         required_columns = {
    #             'metadata_id': True,
    #             f'{COL_HEADER_FILLER}': True, 
    #             'allocated': True,
    #             'nmalloc': True,
    #             'ndalloc': True,
    #             'rps_nmalloc': True,
    #             'rps_ndalloc': True
    #         }

    #         def validate_table(table):
    #             cur.execute(f'PRAGMA table_info("{table}")')
    #             columns = {row[1] for row in cur.fetchall()}
    #             # Print for debugging
    #             print(f"Table {table} columns: {columns}")
    #             print(f"Required columns: {required_columns.keys()}")
    #             return all(col in columns for col in required_columns)

    #         # Get and validate tables
    #         if table_names:
    #             arena_tables = [(t,) for t in table_names if validate_table(t)]
    #         else:
    #             cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'arenas{SECTION_NAME_CON}%{SECTION_TABLE_CON}overall'")
    #             all_tables = cur.fetchall()
    #             arena_tables = [(t[0],) for t in all_tables if validate_table(t[0])]

    #         if not arena_tables:
    #             print("No valid arena tables found")
    #             raise ValueError("No valid arena tables found")  # Add this line

    #         # Construct UNION query for arena data
    #         union_queries = []
    #         for (table,) in arena_tables:
    #             union_queries.append(f"""
    #             SELECT 
    #                 m.timestamp,
    #                 t.metadata_id,
    #                 '{table}' as table_name,
    #                 t.{COL_HEADER_FILLER} as row_name, 
    #                 t.allocated as allocated,
    #                 t.nmalloc as nmalloc,
    #                 t.ndalloc as ndalloc,
    #                 t.rps_nmalloc alloc_rps,
    #                 t.rps_ndalloc as dealloc_rps
    #             FROM '{table}' t
    #             JOIN je_metadata m ON t.metadata_id = m.id
    #             {f"WHERE m.timestamp = '{timestamp}'" if timestamp else ""}
    #             """)

    #         # Main analysis query
    #         query = f"""
    #             WITH arena_data AS ({' UNION ALL '.join(union_queries)})
    #             SELECT 
    #                 timestamp,
    #                 metadata_id,
    #                 0 as arena_id,  -- We're aggregating all into arena 0
    #                 SUM(allocated) as total_allocated,
    #                 100.0 as memory_percent,  -- Since we're aggregating all
    #                 ROUND(SUM(CASE WHEN row_name = '0' THEN allocated ELSE 0 END) * 100.0 / 
    #                     NULLIF(SUM(allocated), 0), 2) as small_percent,
    #                 ROUND(SUM(CASE WHEN row_name = '1' THEN allocated ELSE 0 END) * 100.0 / 
    #                     NULLIF(SUM(allocated), 0), 2) as large_percent,
    #                 SUM(nmalloc) as total_allocs,
    #                 SUM(ndalloc) as total_deallocs,
    #                 SUM(alloc_rps) as alloc_rps,
    #                 SUM(dealloc_rps) as dealloc_rps
    #             FROM arena_data 
    #             GROUP BY timestamp, metadata_id
    #             ORDER BY timestamp, metadata_id, total_allocated DESC
    #             """
    #         # Execute the query and fetch results
    #         cur.execute(query)
    #         rows = cur.fetchall()
    #         headers = ["timestamp", "metadata_id", "arena_id", "total_allocated", 
    #                 "memory_percent", "small_percent", "large_percent",
    #                 "total_allocs", "total_deallocs", "alloc_rps", "dealloc_rps"]
            
    #         # Convert rows to list of dictionaries
    #         results = [dict(zip(headers, row)) for row in rows]
        
    #         # Print formatted output
    #         current_ts = None
    #         current_meta = None
    #         grouped_rows = []
            
    #         for row in rows:
    #             if current_ts != row[0] or current_meta != row[1]:
    #                 if grouped_rows:
    #                     print(f"\n=== Timestamp: {current_ts}, MetaID: {current_meta} ===")
    #                     self.formatter.print_table(headers, grouped_rows)
    #                 current_ts = row[0]
    #                 current_meta = row[1]
    #                 grouped_rows = []
    #             grouped_rows.append(row)
                
    #         if grouped_rows:
    #             print(f"\n=== Timestamp: {current_ts}, MetaID: {current_meta} ===")
    #             self.formatter.print_table(headers, grouped_rows)

    #         return results

    def _analyze_bins_by_size(self, df: pd.DataFrame) -> Dict[str, Any]:
        size_groups = df.groupby("size")
        return {
            "count": size_groups.size().to_dict(),
            "total_allocated": size_groups["allocated"].sum().to_dict(),
            "avg_utilization": size_groups["util"].mean().to_dict(),
        }

    def _identify_allocation_hotspots(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        hotspots = df.nlargest(5, "nmalloc")
        return hotspots[["bins", "size", "nmalloc", "ndalloc", "util"]].to_dict("records")

    def _analyze_fragmentation(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            "avg_utilization": df["util"].mean(),
            "low_util_bins": df[df["util"] < 0.5]["bins"].tolist(),
            "nonfull_slabs_ratio": (df["nonfull_slabs"].sum() / df["curslabs"].sum()),
        }

    def _analyze_lock_contention(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            "total_lock_ops": df["n_lock_ops"].sum(),
            "total_wait_time": df["total_wait_ns"].sum(),
            "max_wait_time": df["max_wait_ns"].max(),
            "max_threads_contention": df["max_n_thds"].max(),
        }

    def _analyze_size_efficiency(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        df["wasted_space"] = df["size"] - df["allocated"] / df["curregs"]
        inefficient_sizes = df.nlargest(5, "wasted_space")
        return inefficient_sizes[["bins", "size", "wasted_space"]].to_dict("records")