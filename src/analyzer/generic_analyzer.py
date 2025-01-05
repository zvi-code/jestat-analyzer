# src/analyzer/generic_analyzer.py
from typing import Dict, Any, List
from src.db.base_table_handler import BaseTableHandler
import json
import re
from constants import *
import pandas as pd

SECTION_NAME_CON = '-'
class GenericAnalyzer(BaseTableHandler):
    def __init__(self, db_path: str, schema_path: str, config: dict):
        super().__init__(db_path, schema_path)
        self.analyzer_config = config['analyses']
        self.current_analysis = None  # Add this line

    def analyze(self, analysis_name: str, timestamp=None) -> Dict[str, Any]:
        self.current_analysis = analysis_name  # Set current analysis name
        config = self.analyzer_config.get(analysis_name)
        if not config:
            raise ValueError(f"No configuration found for analysis: {analysis_name}")

        # Check if this is a special arena comparison analysis
        if config.get('special') == 'arena_pattern':
            return self._analyze_arena_comparison(config)

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
        query = self._build_query(table, config['metrics'], config.get('groupby', []), schema, timestamp)
        with self._get_cursor() as cursor:
            cursor.execute(query)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()

        return {
            'columns': columns,
            'data': rows
        }

    def _build_query(self, table: str, metrics: List[Dict[str, str]], groupby: List[str], schema: Dict[str, Any], timestamp = None) -> str:
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
        if timestamp:
            print(f"Timestamp: {timestamp}")
            where_clauses.append(f"m.timestamp = '{timestamp}'")
        query = f'SELECT {", ".join(groupby)}, {", ".join(select_clauses)} FROM "{table}"'
        
        if where_clauses:
            query += f" WHERE {' AND '.join(where_clauses)}"
            
        if groupby:
            query += f" GROUP BY {', '.join(groupby)}"
            
        if having_clauses:
            query += f" HAVING {' AND '.join(having_clauses)}"

        # Add ORDER BY clause if sort is specified
        if self.current_analysis and 'sort' in self.analyzer_config[self.current_analysis]:
            sort_config = self.analyzer_config[self.current_analysis]['sort']
            if isinstance(sort_config, list):
                sort_clauses = [f"{sort['by']} {sort['order'].upper()}" for sort in sort_config]
                query += f" ORDER BY {', '.join(sort_clauses)}"
            else:
                query += f" ORDER BY {sort_config['by']} {sort_config['order'].upper()}"

        return query

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

    def _get_matching_tables(self, table_pattern: str) -> List[str]:
        available_tables = self.list_available_tables()
        # print(f"Available tables: {table_pattern}{available_tables}")
        # Convert the pattern to a regex
        # regex_pattern = '^' + table_pattern.replace('*', '.*') + '$'
        pattern = re.compile(table_pattern)
        
        matching_tables = [t for t in available_tables if pattern.match(t)]
        return matching_tables

    def _get_schema_for_table(self, table_name: str) -> Dict[str, Any]:
        for schema_pattern, schema in self.schemas.items():
            if re.match('^' + schema_pattern + '$', table_name):
                return schema
        raise ValueError(f"No schema found for table: {table_name}")
        
    def _analyze_arena_comparison(self, config: Dict) -> Dict[str, Any]:
        """Special handler for arena comparison analysis"""
        # Get all arena overall tables
        arena_tables = self._get_matching_tables(f"arenas{SECTION_NAME_CON}*{SECTION_TABLE_CON}overall")
        
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
        arena_pattern = re.compile(r'arenas_(\d+)%soverall' % SECTION_TABLE_CON)
        
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
    