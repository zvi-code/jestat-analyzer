# src/db/stats_handler.py
from typing import List, Dict, Any, Optional
from .base_handler import BaseDBHandler

class StatsHandler(BaseDBHandler):
    def generate_comprehensive_report(self, window_size: int = 5, 
                                leak_threshold: float = 10.0) -> Dict:
        """Generate a comprehensive analysis report"""
        trends = self.analyze_memory_trends(window_size=window_size)
        fragmentation = self.analyze_fragmentation()
        efficiency = self.analyze_arena_efficiency()
        leaks = self.detect_potential_leaks(threshold_percent=leak_threshold)
        
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
        
        return report

    def analyze_memory_trends(self, table_names: List[str] = None, window_size: int = 5) -> Dict:
        """Analyze memory allocation trends over time"""
        with self._get_cursor() as cur:
            query = """
            WITH arena_data AS (
                SELECT 
                    timestamp,
                    SUM(CAST(allocated_0 AS FLOAT)) as total_allocated,
                    SUM(CAST(nmalloc_1 AS FLOAT)) as total_allocs,
                    SUM(CAST(ndalloc_3 AS FLOAT)) as total_deallocs
                FROM arenas_0_overall
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
            query = """
            WITH arena_metrics AS (
                SELECT 
                    metadata_id,
                    timestamp,
                    arena_id,
                    SUM(CAST(allocated_0 AS FLOAT)) as allocated,
                    SUM(CAST(nmalloc_1 AS FLOAT)) as allocations,
                    SUM(CAST(ndalloc_3 AS FLOAT)) as deallocations,
                    SUM(CAST(rps_2 AS FLOAT)) as alloc_rate,
                    SUM(CAST(rps_4 AS FLOAT)) as dealloc_rate
                FROM arenas_0_overall
                GROUP BY metadata_id, timestamp, arena_id
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
            query = """
            WITH allocation_patterns AS (
                SELECT 
                    timestamp,
                    metadata_id,
                    SUM(CAST(allocated_0 AS FLOAT)) as total_allocated,
                    SUM(CAST(nmalloc_1 AS FLOAT)) - SUM(CAST(ndalloc_3 AS FLOAT)) as net_allocations,
                    LAG(SUM(CAST(allocated_0 AS FLOAT))) OVER (ORDER BY timestamp) as prev_allocated
                FROM arenas_0_overall
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

                # Calculate percentiles
                cur.execute(f"""
                    WITH sorted AS (
                        SELECT CAST(TRIM({col}) AS FLOAT) as val
                        FROM '{table_name}'
                        WHERE TRIM({col}) != ''
                        ORDER BY val
                    )
                    SELECT 
                        (SELECT val FROM sorted LIMIT 1 OFFSET (SELECT COUNT(*) * 50 / 100 - 1 FROM sorted)) as p50,
                        (SELECT val FROM sorted LIMIT 1 OFFSET (SELECT COUNT(*) * 90 / 100 - 1 FROM sorted)) as p90,
                        (SELECT val FROM sorted LIMIT 1 OFFSET (SELECT COUNT(*) * 99 / 100 - 1 FROM sorted)) as p99
                """)
                p50, p90, p99 = cur.fetchone()

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
        
    def calculate_table_stats2(self, table_name: str) -> None:
        """Calculate comprehensive statistics for a table"""
        with self._get_cursor() as cur:
            cur.execute(f"SELECT * FROM '{table_name}' LIMIT 1")
            columns = [desc[0] for desc in cur.description]
            if not columns:
                return

            # Columns to ignore in statistical calculations
            columns2ignore = {'id', 'timestamp', 'section', 'table_name', 'metadata_id'}
            
            # Create stats table if it doesn't exist
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS 'stats_{table_name}' (
                    metric TEXT,
                    {', '.join(f'"{col}" FLOAT' for col in columns if col not in columns2ignore)}
                )
            """)

            # Calculate statistics for each metric
            metrics = ['sum', 'avg', 'std', 'p50', 'p90', 'p99']
            for metric in metrics:
                cur.execute(f"""
                    INSERT INTO 'stats_{table_name}' (metric, {', '.join(col for col in columns if col not in columns2ignore)})
                    SELECT '{metric}',
                        {', '.join(f'''
                        ROUND(CASE '{metric}'
                            WHEN 'sum' THEN SUM(CAST(TRIM({col}) AS FLOAT))
                            WHEN 'avg' THEN AVG(CAST(TRIM({col}) AS FLOAT))
                            WHEN 'std' THEN SQRT(AVG(POWER(CAST(TRIM({col}) AS FLOAT), 2)) - POWER(AVG(CAST(TRIM({col}) AS FLOAT)), 2))
                            WHEN 'p50' THEN (SELECT val FROM (SELECT CAST(TRIM({col}) AS FLOAT) as val FROM '{table_name}' ORDER BY val LIMIT 1 OFFSET (SELECT COUNT(*) * 50 / 100 - 1 FROM '{table_name}')))
                            WHEN 'p90' THEN (SELECT val FROM (SELECT CAST(TRIM({col}) AS FLOAT) as val FROM '{table_name}' ORDER BY val LIMIT 1 OFFSET (SELECT COUNT(*) * 90 / 100 - 1 FROM '{table_name}')))
                            WHEN 'p99' THEN (SELECT val FROM (SELECT CAST(TRIM({col}) AS FLOAT) as val FROM '{table_name}' ORDER BY val LIMIT 1 OFFSET (SELECT COUNT(*) * 99 / 100 - 1 FROM '{table_name}')))
                        END, 2)''' for col in columns if col not in columns2ignore)}
                    FROM '{table_name}'
                """)

    def analyze_arenas_activity(self, table_names: List[str] = None, timestamp: str = None) -> List[dict]:
        """Analyze arena activity statistics"""
        with self._get_cursor() as cur:
            required_columns = {
                'metadata_id': True,
                'primary_0': True, 
                'allocated_0': True,
                'nmalloc_1': True,
                'ndalloc_3': True,
                'rps_2': True,
                'rps_4': True
            }

            def validate_table(table):
                cur.execute(f'PRAGMA table_info("{table}")')
                columns = {row[1] for row in cur.fetchall()}
                return all(col in columns for col in required_columns)

            # Get and validate tables
            if table_names:
                arena_tables = [(t,) for t in table_names if validate_table(t)]
            else:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'arenas_%.overall'")
                all_tables = cur.fetchall()
                arena_tables = [(t[0],) for t in all_tables if validate_table(t[0])]

            if not arena_tables:
                print("No valid arena tables found")
                raise ValueError("No valid arena tables found")  # Add this line

            # Construct UNION query for arena data
            union_queries = []
            for (table,) in arena_tables:
                union_queries.append(f"""
                SELECT 
                    m.timestamp,
                    t.metadata_id,
                    '{table}' as table_name,
                    t.primary_0 as row_name, 
                    t.allocated_0 as allocated,
                    t.nmalloc_1 as nmalloc,
                    t.ndalloc_3 as ndalloc,
                    t.rps_2 as alloc_rps,
                    t.rps_4 as dealloc_rps
                FROM '{table}' t
                JOIN je_metadata m ON t.metadata_id = m.id
                {f"WHERE m.timestamp = '{timestamp}'" if timestamp else ""}
                """)

            # Main analysis query
            query = f"""
                WITH arena_data AS ({' UNION ALL '.join(union_queries)})
                SELECT 
                    timestamp,
                    metadata_id,
                    0 as arena_id,  -- We're aggregating all into arena 0
                    SUM(allocated) as total_allocated,
                    100.0 as memory_percent,  -- Since we're aggregating all
                    ROUND(SUM(CASE WHEN row_name = '0' THEN allocated ELSE 0 END) * 100.0 / 
                        NULLIF(SUM(allocated), 0), 2) as small_percent,
                    ROUND(SUM(CASE WHEN row_name = '1' THEN allocated ELSE 0 END) * 100.0 / 
                        NULLIF(SUM(allocated), 0), 2) as large_percent,
                    SUM(nmalloc) as total_allocs,
                    SUM(ndalloc) as total_deallocs,
                    SUM(alloc_rps) as alloc_rps,
                    SUM(dealloc_rps) as dealloc_rps
                FROM arena_data 
                GROUP BY timestamp, metadata_id
                ORDER BY timestamp, metadata_id, total_allocated DESC
                """
            # Execute the query and fetch results
            cur.execute(query)
            rows = cur.fetchall()
            headers = ["timestamp", "metadata_id", "arena_id", "total_allocated", 
                    "memory_percent", "small_percent", "large_percent",
                    "total_allocs", "total_deallocs", "alloc_rps", "dealloc_rps"]
            
            # Convert rows to list of dictionaries
            results = [dict(zip(headers, row)) for row in rows]
        
            # Print formatted output
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

            return results
        
    def analyze_arenas_activity2(self, table_names: List[str] = None, timestamp: str = None) -> None:
        """Analyze arena activity statistics"""
        with self._get_cursor() as cur:
            required_columns = {
                'metadata_id': True,
                'primary_0': True, 
                'allocated_0': True,
                'nmalloc_1': True,
                'ndalloc_3': True,
                'rps_2': True,
                'rps_4': True
            }

            def validate_table(table):
                cur.execute(f'PRAGMA table_info("{table}")')
                columns = {row[1] for row in cur.fetchall()}
                return all(col in columns for col in required_columns)

            # Get and validate tables
            if table_names:
                arena_tables = [(t,) for t in table_names if validate_table(t)]
            else:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'arenas_%.overall'")
                all_tables = cur.fetchall()
                arena_tables = [(t[0],) for t in all_tables if validate_table(t[0])]

            if not arena_tables:
                print("No valid arena tables found")
                return

            # Construct UNION query for arena data
            union_queries = []
            for (table,) in arena_tables:
                union_queries.append(f"""
                SELECT 
                    m.timestamp,
                    t.metadata_id,
                    '{table}' as table_name,
                    t.primary_0 as row_name, 
                    t.allocated_0 as allocated,
                    t.nmalloc_1 as nmalloc,
                    t.ndalloc_3 as ndalloc,
                    t.rps_2 as alloc_rps,
                    t.rps_4 as dealloc_rps
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
                        INSTR(SUBSTR(table_name, 8), '_') - 1) AS INTEGER) as table_type,
                    CAST(SUBSTR(table_name, 
                        8 + INSTR(SUBSTR(table_name, 8), '_'), 
                        INSTR(table_name, '.') - (8 + INSTR(SUBSTR(table_name, 8), '_'))) 
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
            
            cur.execute(query)
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