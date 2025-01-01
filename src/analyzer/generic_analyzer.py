# src/analyzer/generic_analyzer.py
from typing import Dict, Any, List
from src.db.base_table_handler import BaseTableHandler
import json
import re

class GenericAnalyzer(BaseTableHandler):
    def __init__(self, db_path: str, schema_path: str, config: dict):
        super().__init__(db_path, schema_path)
        self.analyzer_config = config['analyses']

    def list_available_tables(self):
        with self._get_cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            return [row[0] for row in cursor.fetchall()]

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
    def analyze3(self, analysis_name: str) -> Dict[str, Any]:
        config = self.analyzer_config.get(analysis_name)
        if not config:
            raise ValueError(f"No configuration found for analysis: {analysis_name}")

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

    def _build_query(self, table: str, metrics: List[Dict[str, str]], groupby: List[str], schema: Dict[str, Any]) -> str:
        select_clauses = []
        for metric in metrics:
            if metric['operation'] == 'custom':
                select_clauses.append(f"{metric['formula']} as {metric['name']}")
            else:
                column = metric['column']
                if column not in [col['name'] for col in schema['columns']]:
                    raise ValueError(f"Column '{column}' not found in schema for table '{table}'")
                select_clauses.append(f"{metric['operation']}({column}) as {metric['name']}")
        
        select_clause = ', '.join(select_clauses)
        groupby_clause = ', '.join(groupby) if groupby else ''

        query = f'SELECT {", ".join(groupby)}, {select_clause} FROM "{table}"'
        if groupby_clause:
            query += f" GROUP BY {groupby_clause}"

        return query