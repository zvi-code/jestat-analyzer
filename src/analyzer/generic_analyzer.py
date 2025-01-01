# src/analyzer/generic_analyzer.py
from typing import Dict, Any, List
from src.db.base_table_handler import BaseTableHandler
import json
import re

class GenericAnalyzer(BaseTableHandler):
    def __init__(self, db_path: str, schema_path: str, config: dict):
        super().__init__(db_path, schema_path)
        self.analyzer_config = config['analyses']
    def list_available_tables(self, prefix=None):
        """
        List all tables in the database, optionally filtered by prefix
        """
        with self._get_cursor() as cursor:
            if prefix:
                # Use SQL LIKE for prefix matching
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ? ORDER BY name", (f'{prefix}%',))
            else:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            return [row[0] for row in cursor.fetchall()]
    # def list_available_tables(self):
    #     with self._get_cursor() as cursor:
    #         cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    #         return [row[0] for row in cursor.fetchall()]

    def _get_matching_tables(self, table_pattern: str) -> List[str]:
        available_tables = self.list_available_tables()
        
        # Convert the pattern to a regex
        regex_pattern = '^' + table_pattern.replace('*', '.*') + '$'
        pattern = re.compile(regex_pattern)
        
        matching_tables = [t for t in available_tables if pattern.match(t)]
        return matching_tables

    def _get_schema_for_table(self, table_name: str) -> Dict[str, Any]:
        for schema_pattern, schema in self.schemas.items():
            if re.match('^' + schema_pattern.replace('*', '.*') + '$', table_name):
                return schema
        raise ValueError(f"No schema found for table: {table_name}")
    
    def analyze(self, analysis_name: str) -> Dict[str, Any]:
        config = self.analyzer_config.get(analysis_name)
        if not config:
            raise ValueError(f"No configuration found for analysis: {analysis_name}")

        # Check if this is a special arena comparison analysis
        if config.get('special') == 'arena_pattern':
            return self._analyze_arena_comparison(config)

        # Regular analysis logic continues here...
        table_pattern = config['table']
        matching_tables = self._get_matching_tables(table_pattern)
        
        if not matching_tables:
            available_tables = self.list_available_tables()
            error_msg = f"No tables found matching the pattern '{table_pattern}'\n"
            error_msg += f"Available tables are: {', '.join(available_tables)}"
            raise ValueError(error_msg)
        elif len(matching_tables) > 1:
            error_msg = f"Multiple tables match the pattern '{table_pattern}': {', '.join(matching_tables)}\n"
            error_msg += "Please specify a more precise pattern."
            raise ValueError(error_msg)

        table = matching_tables[0]
        schema = self._get_schema_for_table(table)
        metrics = config['metrics']
        groupby = config.get('groupby', [])

        query = self._build_query(table, metrics, groupby, schema)
        
        with self._get_cursor() as cursor:
            cursor.execute(query)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
        sorted_rows = sorted(rows, key=lambda x: int(x[0]))

        return {
            'columns': columns,
            'data': sorted_rows
        }
    def _build_query_new(self, table: str, metrics: List[Dict[str, str]], groupby: List[str], schema: Dict[str, Any]) -> str:
        select_clauses = []
        where_clauses = []
        having_clauses = []
        
        for metric in metrics:
            if metric['operation'] == 'expression':
                formula = metric['formula']
                row_op = formula['row_operation']
                agg = formula['aggregation']
                
                if 'filter' in formula:
                    where_clauses.append(formula['filter'])
                    
                if 'having' in formula:
                    having_clauses.append(f"{metric['name']} {formula['having']}")
                    
                select_clauses.append(f"{agg}({row_op}) as {metric['name']}")
            else:
                column = metric['column']
                if column not in [col['name'] for col in schema['columns']]:
                    raise ValueError(f"Column '{column}' not found in schema for table '{table}'")
                select_clauses.append(f"{metric['operation']}({column}) as {metric['name']}")

        query = f'SELECT {", ".join(groupby)}, {", ".join(select_clauses)} FROM "{table}"'
        
        if where_clauses:
            query += f" WHERE {' AND '.join(where_clauses)}"
            
        if groupby:
            query += f" GROUP BY {', '.join(groupby)}"
            
        if having_clauses:
            query += f" HAVING {' AND '.join(having_clauses)}"

        # Add ORDER BY clause if sort is specified
        if 'sort' in self.analyzer_config.get(table, {}):
            sort_config = self.analyzer_config[table]['sort']
            if isinstance(sort_config, list):
                sort_clauses = [f"{sort['by']} {sort['order'].upper()}" for sort in sort_config]
                query += f" ORDER BY {', '.join(sort_clauses)}"
            else:
                query += f" ORDER BY {sort_config['by']} {sort_config['order'].upper()}"

        return query
    def _build_query(self, table: str, metrics: List[Dict[str, str]], groupby: List[str], schema: Dict[str, Any]) -> str:
        if 'special' in self.analyzer_config.get(table, {}) and self.analyzer_config[table]['special'] == 'arena_pattern':
            return self._build_arena_comparison_query(metrics, groupby)        
        select_clauses = []
        where_clauses = []
        having_clauses = []
        
        for metric in metrics:
            if metric['operation'] == 'expression':
                formula = metric['formula']
                row_op = formula['row_operation']
                agg = formula['aggregation']
                
                # Add any filtering conditions
                if 'filter' in formula:
                    where_clauses.append(formula['filter'])
                    
                # Add any having conditions
                if 'having' in formula:
                    having_clauses.append(f"{metric['name']} {formula['having']}")
                    
                select_clauses.append(f"{agg}({row_op}) as {metric['name']}")
                
            elif metric['operation'] == 'custom':
                # Handle custom SQL expressions
                select_clauses.append(f"{metric['formula']} as {metric['name']}")
                
            else:
                # Handle simple aggregation operations
                column = metric['column']
                if column not in [col['name'] for col in schema['columns']]:
                    raise ValueError(f"Column '{column}' not found in schema for table '{table}'")
                select_clauses.append(f"{metric['operation']}({column}) as {metric['name']}")

        # Build the complete query
        query = f'SELECT {", ".join(groupby)}, {", ".join(select_clauses)} FROM "{table}"'
    
        if where_clauses:
            query += f" WHERE {' AND '.join(where_clauses)}"
            
        if groupby:
            query += f" GROUP BY {', '.join(groupby)}"
            
        if having_clauses:
            query += f" HAVING {' AND '.join(having_clauses)}"

        # Add ORDER BY clause if sort is specified
        if 'sort' in self.analyzer_config.get(table, {}):
            sort_config = self.analyzer_config[table]['sort']
            if isinstance(sort_config, list):
                # Multiple sort criteria
                sort_clauses = []
                for sort_item in sort_config:
                    sort_clauses.append(f"{sort_item['by']} {sort_item['order'].upper()}")
                query += f" ORDER BY {', '.join(sort_clauses)}"
            else:
                # Single sort criterion
                query += f" ORDER BY {sort_config['by']} {sort_config['order'].upper()}"

        return query
    def _analyze_arena_comparison(self, config: Dict) -> Dict[str, Any]:
        """Special handler for arena comparison analysis"""
        # Get all arena overall tables
        arena_tables = self._get_matching_tables("arenas_*.overall")
        
        if not arena_tables:
            raise ValueError("No arena tables found")

        # Build and execute the combined query
        query = self._build_arena_comparison_query(arena_tables, config['metrics'])
        
        with self._get_cursor() as cursor:
            cursor.execute(query)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()

        return {
            'columns': columns,
            'data': rows
        }

    def _build_arena_comparison_query(self, arena_tables: List[str], metrics: List[Dict]) -> str:
        """Builds a UNION ALL query for arena comparison"""
        import re
        arena_pattern = re.compile(r'arenas_(\d+)\.overall')
        
        # First, create a CTE (Common Table Expression) to calculate total allocation
        cte_queries = []
        main_queries = []
        
        for table in arena_tables:
            arena_id = arena_pattern.match(table).group(1)
            select_clauses = []
            
            # Add arena_id as a constant
            select_clauses.append(f"'{arena_id}' as arena_id")
            
            # Add basic metrics
            for metric in metrics:
                if metric['operation'] == 'expression':
                    continue  # Handle expressions separately
                else:
                    column = metric['column']
                    select_clauses.append(f"{metric['operation']}({column}) as {metric['name']}")
            
            query = f"""
                SELECT {', '.join(select_clauses)}
                FROM "{table}"
            """
            main_queries.append(query)

        # Combine the main queries
        combined_query = f"""
        WITH arena_stats AS (
            {" UNION ALL ".join(main_queries)}
        ),
        total_stats AS (
            SELECT SUM(total_allocated) as total_memory
            FROM arena_stats
        )
        SELECT 
            arena_stats.*,
            ROUND(CAST(total_allocated AS FLOAT) * 100 / total_stats.total_memory, 2) as memory_percent
        FROM arena_stats, total_stats
        ORDER BY total_allocated DESC
        """
        
        return combined_query
    def _build_arena_comparison_que3ry(self, metrics: List[Dict[str, str]], groupby: List[str]) -> str:
        # Get all arena overall tables
        arena_tables = self._get_matching_tables("arenas_*.overall")
        
        # Extract arena IDs using regex
        import re
        arena_pattern = re.compile(r'arenas_(\d+)\.overall')
        
        # Build individual queries for each arena
        queries = []
        for table in arena_tables:
            arena_id = arena_pattern.match(table).group(1)
            select_clauses = []
            
            # Add arena_id as a constant
            select_clauses.append(f"'{arena_id}' as arena_id")
            
            for metric in metrics:
                if metric['operation'] == 'expression':
                    formula = metric['formula']
                    row_op = formula['row_operation']
                    agg = formula['aggregation']
                    select_clauses.append(f"{agg}({row_op}) as {metric['name']}")
                else:
                    column = metric['column']
                    select_clauses.append(f"{metric['operation']}({column}) as {metric['name']}")
            
            query = f"""
                SELECT {', '.join(select_clauses)}
                FROM "{table}"
            """
            queries.append(query)
        
        # Combine all queries with UNION ALL
        final_query = " UNION ALL ".join(queries)
        
        # Add ordering by allocated memory
        final_query += " ORDER BY total_allocated DESC"
        
        return final_query