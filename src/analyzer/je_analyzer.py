# src/analyzer/je_analyzer.py
import json
from src.db.stats_handler import StatsHandler
from src.db.display_handler import DisplayHandler
from src.analyzer.generic_analyzer import GenericAnalyzer
from src.utils.table_formatter import TableFormatter

class JeAnalyzer:
    def __init__(self, db_path: str, config: dict):
        self.db_path = db_path
        self.config = config
        self.stats_handler = StatsHandler(db_path)
        self.display_handler = DisplayHandler(db_path)
        self.generic_analyzer = GenericAnalyzer(db_path, config['schema_path'], config)
        self.table_formatter = TableFormatter()

    def analyze(self, mode: str, table_pattern: str = None, timestamp: str = None, limit: int = 10):
        try:
            if mode == 'raw':
                self.display_handler.display_raw_data(table_pattern, limit)
            elif mode == 'stats':
                self.analyze_table_stats(table_pattern)
            elif mode == 'arena':
                self.analyze_arenas_activity(table_pattern, timestamp)
            elif mode == 'meta':
                self.display_metadata()
            elif mode == 'table':
                self.print_table(table_pattern, limit)
            elif mode in self.config['analyses']:
                result = self.generic_analyzer.analyze(mode)
                self._print_formatted_result(result)
            else:
                print(f"Unknown mode: {mode} {self.config['analyses']}")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)} {self.config['analyses']}")

    def print_table(self, table_name: str, limit: int = 10):
        if not table_name:
            raise ValueError("Table name must be provided for 'table' mode")
        
        with self.generic_analyzer._get_cursor() as cursor:
            try:
                cursor.execute(f"SELECT * FROM '{table_name}' LIMIT {limit}")
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                
                if not rows:
                    print(f"No data found in table '{table_name}'")
                    return
                
                print(f"Data from table '{table_name}' (first {limit} rows):")
                self.table_formatter.print_table(columns, rows)
            except Exception as e:
                print(f"Error accessing table '{table_name}': {str(e)}")

    def analyze_bins(self):
        result = self.generic_analyzer.analyze('bins_analysis')
        self._print_activity_analysis(result)
    def list_tables(self, prefix=None):
        """
        List all tables in the database, optionally filtered by prefix
        """
        tables = self.generic_analyzer.list_available_tables(prefix)
        return tables
    def _print_activity_analysis(self, result):
        columns = result['columns']
        data = result['data']
        
        # Print the basic table
        self.table_formatter.print_table(columns, data)
        
        # Sort data by different metrics for analysis
        by_requests = sorted(data, key=lambda x: x[columns.index('total_requests')], reverse=True)
        by_rates = sorted(data, key=lambda x: x[columns.index('alloc_rate')] + x[columns.index('dealloc_rate')], reverse=True)
        by_churn = sorted(data, key=lambda x: x[columns.index('fills')] + x[columns.index('flushes')], reverse=True)
        
        print("\nActivity Analysis:")
        
        # Most active bins by requests
        print("\nTop 5 Most Active Bins (by total requests):")
        for row in by_requests[:5]:
            bin_id = row[columns.index('bins_0')]
            bin_size = row[columns.index('bin_size')]
            requests = row[columns.index('total_requests')]
            allocs = row[columns.index('alloc_ops')]
            deallocs = row[columns.index('dealloc_ops')]
            
            print(f"\nBin {bin_id} (size {bin_size} bytes):")
            print(f"  Total Requests: {requests:,}")
            print(f"  Allocations: {allocs:,}")
            print(f"  Deallocations: {deallocs:,}")
            print(f"  Cache hit ratio: {((requests - allocs) / requests * 100):.2f}%")
            print(f"  Current allocation rate: {row[columns.index('alloc_rate')]:,}/s")
            print(f"  Current deallocation rate: {row[columns.index('dealloc_rate')]:,}/s")
        
        # Bins with highest operation rates
        print("\nTop 5 Bins with Highest Operation Rates:")
        for row in by_rates[:5]:
            bin_id = row[columns.index('bins_0')]
            bin_size = row[columns.index('bin_size')]
            alloc_rate = row[columns.index('alloc_rate')]
            dealloc_rate = row[columns.index('dealloc_rate')]
            fill_rate = row[columns.index('fill_rate')]
            flush_rate = row[columns.index('flush_rate')]
            
            print(f"\nBin {bin_id} (size {bin_size} bytes):")
            print(f"  Allocation Rate: {alloc_rate:,}/s")
            print(f"  Deallocation Rate: {dealloc_rate:,}/s")
            print(f"  Fill Rate: {fill_rate:,}/s")
            print(f"  Flush Rate: {flush_rate:,}/s")
        
        # Bins with high slab churn
        print("\nTop 5 Bins with Highest Slab Activity:")
        for row in by_churn[:5]:
            bin_id = row[columns.index('bins_0')]
            bin_size = row[columns.index('bin_size')]
            fills = row[columns.index('fills')]
            flushes = row[columns.index('flushes')]
            lock_ops = row[columns.index('lock_ops')]
            owner_switches = row[columns.index('owner_switches')]
            
            print(f"\nBin {bin_id} (size {bin_size} bytes):")
            print(f"  Total Fills: {fills:,}")
            print(f"  Total Flushes: {flushes:,}")
            print(f"  Lock Operations: {lock_ops:,}")
            print(f"  Owner Switches: {owner_switches:,}")
            print(f"  Ops per owner switch: {(fills + flushes) / owner_switches:.2f}" if owner_switches > 0 else "  No owner switches")
        
        # Overall statistics
        total_requests = sum(row[columns.index('total_requests')] for row in data)
        total_allocs = sum(row[columns.index('alloc_ops')] for row in data)
        total_deallocs = sum(row[columns.index('dealloc_ops')] for row in data)
        total_fills = sum(row[columns.index('fills')] for row in data)
        total_flushes = sum(row[columns.index('flushes')] for row in data)
        
        print("\nOverall Activity Statistics:")
        print(f"Total Requests: {total_requests:,}")
        print(f"Total Allocations: {total_allocs:,}")
        print(f"Total Deallocations: {total_deallocs:,}")
        print(f"Overall Cache Hit Ratio: {((total_requests - total_allocs) / total_requests * 100):.2f}%")
        print(f"Total Fills: {total_fills:,}")
        print(f"Total Flushes: {total_flushes:,}")
        print(f"Fill/Flush Ratio: {total_fills / total_flushes:.2f}" if total_flushes > 0 else "No flushes")
    def _print_activity_analysis3(self, result):
        columns = result['columns']
        data = result['data']
        
        # Print the basic table
        self.table_formatter.print_table(columns, data)
        
        # Calculate activity statistics using only columns we know exist in this analysis
        total_requests = sum(row[columns.index('total_requests')] for row in data)
        total_allocs = sum(row[columns.index('alloc_ops')] for row in data)
        total_deallocs = sum(row[columns.index('dealloc_ops')] for row in data)
        total_fills = sum(row[columns.index('fills')] for row in data)
        total_flushes = sum(row[columns.index('flushes')] for row in data)
        
        print("\nActivity Analysis Summary:")
        print(f"Total Requests: {total_requests:,}")
        print(f"Total Allocations: {total_allocs:,}")
        print(f"Total Deallocations: {total_deallocs:,}")
        if total_requests > 0:
            print(f"Overall Cache Hit Ratio: {((total_requests - total_allocs) / total_requests * 100):.2f}%")
        print(f"Total Fills: {total_fills:,}")
        print(f"Total Flushes: {total_flushes:,}")

    def _print_pages_analysis(self, result):
        columns = result['columns']
        data = result['data']
        
        # Print the basic table
        self.table_formatter.print_table(columns, data)
        
        # Pages analysis specific calculations
        total_pages = sum(row[columns.index('total_pages')] for row in data)
        total_memory = sum(row[columns.index('total_allocated')] for row in data)
        
        print("\nPages Analysis Summary:")
        print(f"Total Pages Used: {total_pages:,} ({total_pages * 4:,} KB)")
        print(f"Total Memory Allocated: {total_memory:,} bytes ({total_memory/1024/1024:.2f} MB)")
    def _print_activity_analysis333w(self, result):
        columns = result['columns']
        data = result['data']
        
        # Print the basic table
        self.table_formatter.print_table(columns, data)
        
        # Sort data by different metrics for analysis
        by_requests = sorted(data, key=lambda x: x[columns.index('total_requests')], reverse=True)
        by_contention = sorted(data, key=lambda x: x[columns.index('waiting_count')] / x[columns.index('lock_ops')] if x[columns.index('lock_ops')] > 0 else 0, reverse=True)
        by_lock_churn = sorted(data, key=lambda x: x[columns.index('owner_switches')], reverse=True)
        
        print("\nActivity Analysis:")
        
        # Most active bins by requests
        print("\nTop 5 Most Active Bins (by requests):")
        for row in by_requests[:5]:
            bin_id = row[columns.index('bins_0')]
            bin_size = row[columns.index('bin_size')]
            requests = row[columns.index('total_requests')]
            allocs = row[columns.index('alloc_ops')]
            deallocs = row[columns.index('dealloc_ops')]
            
            print(f"\nBin {bin_id} (size {bin_size} bytes):")
            print(f"  Requests: {requests:,}")
            print(f"  Allocations: {allocs:,}")
            print(f"  Deallocations: {deallocs:,}")
            print(f"  Cache hit ratio: {((requests - allocs) / requests * 100):.2f}%")
        
        # Bins with high contention
        print("\nTop 5 Bins with Highest Lock Contention:")
        for row in by_contention[:5]:
            bin_id = row[columns.index('bins_0')]
            bin_size = row[columns.index('bin_size')]
            lock_ops = row[columns.index('lock_ops')]
            waiting = row[columns.index('waiting_count')]
            wait_time = row[columns.index('total_wait_ns')]
            
            if lock_ops > 0:
                contention_rate = (waiting / lock_ops) * 100
                avg_wait = wait_time / waiting if waiting > 0 else 0
                
                print(f"\nBin {bin_id} (size {bin_size} bytes):")
                print(f"  Lock operations: {lock_ops:,}")
                print(f"  Times waited: {waiting:,}")
                print(f"  Contention rate: {contention_rate:.2f}%")
                print(f"  Average wait time: {avg_wait:.2f} ns")
        
        # Bins with high lock churn
        print("\nTop 5 Bins with Highest Lock Churn:")
        for row in by_lock_churn[:5]:
            bin_id = row[columns.index('bins_0')]
            bin_size = row[columns.index('bin_size')]
            switches = row[columns.index('owner_switches')]
            fills = row[columns.index('fills')]
            flushes = row[columns.index('flushes')]
            
            if switches > 0:
                print(f"\nBin {bin_id} (size {bin_size} bytes):")
                print(f"  Owner switches: {switches:,}")
                print(f"  Fills: {fills:,}")
                print(f"  Flushes: {flushes:,}")
                print(f"  Ops per switch: {(fills + flushes) / switches:.2f}")
        
        # Overall statistics
        total_requests = sum(row[columns.index('total_requests')] for row in data)
        total_allocs = sum(row[columns.index('alloc_ops')] for row in data)
        total_deallocs = sum(row[columns.index('dealloc_ops')] for row in data)
        total_lock_ops = sum(row[columns.index('lock_ops')] for row in data)
        total_waiting = sum(row[columns.index('waiting_count')] for row in data)
        
        print("\nOverall Activity Statistics:")
        print(f"Total Requests: {total_requests:,}")
        print(f"Total Allocations: {total_allocs:,}")
        print(f"Total Deallocations: {total_deallocs:,}")
        print(f"Overall Cache Hit Ratio: {((total_requests - total_allocs) / total_requests * 100):.2f}%")
        print(f"Overall Lock Contention Rate: {(total_waiting / total_lock_ops * 100):.2f}%")

    def _print_formatted_result(self, result):
        columns = result['columns']
        data = result['data']
        
        # Print the table
        self.table_formatter.print_table(columns, data)
        
        # Calculate total pages and memory
        total_pages = sum(row[columns.index('total_pages')] for row in data)
        total_memory = sum(row[columns.index('total_allocated')] for row in data)
        
        # Sort bins by page usage
        sorted_by_pages = sorted(data, key=lambda x: x[columns.index('total_pages')], reverse=True)
        
        print("\nPage Usage Analysis:")
        print(f"Total Pages Used: {total_pages:,} ({total_pages * 4:,} KB)")
        print(f"Total Memory Allocated: {total_memory:,} bytes ({total_memory/1024/1024:.2f} MB)")
        print(f"Overall Memory Efficiency: {(total_memory / (total_pages * 4096)) * 100:.2f}%")
        
        print("\nTop 10 Bins by Page Usage:")
        for row in sorted_by_pages[:10]:
            bin_id = row[columns.index('bins_0')]
            bin_size = row[columns.index('bin_size')]
            pages = row[columns.index('total_pages')]
            util = row[columns.index('utilization')]
            allocated = row[columns.index('total_allocated')]
            
            print(f"\nBin {bin_id} (size {bin_size} bytes):")
            print(f"  Pages: {pages:,} ({pages * 4:,} KB)")
            print(f"  Utilization: {util:.2f}%")
            print(f"  Memory Allocated: {allocated:,} bytes")
            print(f"  Memory Efficiency: {(allocated / (pages * 4096)) * 100:.2f}%")
        
        print("\nUtilization Summary:")
        poor_util_bins = [row for row in data if row[columns.index('utilization')] < 0.5]
        print(f"Bins with <50% utilization: {len(poor_util_bins)}")
        wasted_pages = sum(row[columns.index('total_pages')] for row in poor_util_bins)
        print(f"Pages in poorly utilized bins: {wasted_pages:,} ({wasted_pages * 4:,} KB)")

    def r_print_formatted_result(self, result):
        columns = result['columns']
        data = result['data']
        
        # Print the table
        self.table_formatter.print_table(columns, data)
        
        # Print summary statistics
        print("\nSummary Statistics:")
        for i, column in enumerate(columns):
            if column == 'arena_id':
                # Skip arena_id for statistics or handle it separately
                continue
                
            values = [row[i] for row in data if row[i] is not None]
            if values:
                print(f"{column}:")
                print(f"  Min: {min(values):,}")
                print(f"  Max: {max(values):,}")
                print(f"  Avg: {sum(values) / len(values):,.2f}")
                print(f"  Sum: {sum(values):,}")
                print()

        # Add arena-specific statistics
        total_memory = sum(row[columns.index('total_allocated')] for row in data)
        total_allocs = sum(row[columns.index('allocation_rate')] for row in data)
        total_deallocs = sum(row[columns.index('deallocation_rate')] for row in data)
        
        print("\nOverall Statistics:")
        print(f"Total Memory Allocated: {total_memory:,} bytes ({total_memory/1024/1024:.2f} MB)")
        print(f"Total Allocations: {total_allocs:,}")
        print(f"Total Deallocations: {total_deallocs:,}")
        print(f"Net Allocations: {total_allocs - total_deallocs:,}")
        print(f"Number of Active Arenas: {len(data)}")
        
        # Add memory distribution analysis
        print("\nMemory Distribution:")
        memory_threshold = total_memory * 0.01  # 1% threshold
        significant_arenas = [row for row in data if row[columns.index('total_allocated')] > memory_threshold]
        print(f"Arenas using >1% of total memory: {len(significant_arenas)}")
        for row in significant_arenas:
            arena_id = row[columns.index('arena_id')]
            memory = row[columns.index('total_allocated')]
            percent = row[columns.index('memory_percent')]
            print(f"  Arena {arena_id}: {memory:,} bytes ({percent:.2f}%)")    
    def _print_formatted_result22(self, result):
        columns = result['columns']
        data = result['data']
        
        # Sort the data by the first column (bins_0)
        sorted_data = sorted(data, key=lambda x: int(x[0]))
        
        # Print the table
        self.table_formatter.print_table(columns, sorted_data)
        
        # Print summary statistics
        print("\nSummary Statistics:")
        for i, column in enumerate(columns):
            if column not in ['bins_0', 'size_1']:  # Exclude non-numeric columns
                values = [row[i] for row in sorted_data if row[i] is not None]
                if values:
                    print(f"{column}:")
                    print(f"  Min: {min(values)}")
                    print(f"  Max: {max(values)}")
                    print(f"  Avg: {sum(values) / len(values):.2f}")
                    print(f"  Sum: {sum(values)}")
                    print()                    
    # def _print_formatted_result(self, result):
    #     columns = result['columns']
    #     data = result['data']
        
    #     # Print the table
    #     self.table_formatter.print_table(columns, data)
        
    #     # Print summary statistics
    #     print("\nSummary Statistics:")
    #     for i, column in enumerate(columns):
    #         if column not in ['bins_0', 'size_1']:  # Exclude non-numeric columns
    #             values = [row[i] for row in data if row[i] is not None]
    #             if values:
    #                 print(f"{column}:")
    #                 print(f"  Min: {min(values)}")
    #                 print(f"  Max: {max(values)}")
    #                 print(f"  Avg: {sum(values) / len(values):.2f}")
    #                 print(f"  Sum: {sum(values)}")
    #                 print()
    # def analyze(self, mode: str, table_pattern: str = None, timestamp: str = None, limit: int = 10):
    #     if mode == 'raw':
    #         self.display_handler.display_raw_data(table_pattern, limit)
    #     elif mode == 'stats':
    #         self.analyze_table_stats(table_pattern)
    #     elif mode == 'arena':
    #         self.analyze_arenas_activity(table_pattern, timestamp)
    #     elif mode == 'meta':
    #         self.display_metadata()
    #     elif mode in self.config['analyses']:
    #         result = self.generic_analyzer.analyze(mode)
    #         print(json.dumps(result, indent=2))
    #     else:
    #         print(f"Unknown mode: {mode}")

    def analyze_bins(self, table_name: str):
        analysis = self.stats_handler.analyze_bins(table_name)
        print(json.dumps(analysis, indent=2))

    def analyze_table_stats(self, table_name: str) -> None:
        """Calculate and display statistics for a table"""
        print(f"\nAnalyzing statistics for {table_name}...")
        self.stats_handler.calculate_table_stats(table_name)
        self.display_handler.print_table_stats(table_name)

    def display_metadata(self) -> None:
        """Display metadata information"""
        self.display_handler.print_metadata_summary()
        self.display_handler.print_available_timestamps()

    def close(self) -> None:
        """Clean up resources"""
        self.stats_handler.close()
        self.display_handler.close()