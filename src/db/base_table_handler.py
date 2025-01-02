# src/db/base_table_handler.py
import json
from typing import Dict, Any, List
from src.db.base_handler import BaseDBHandler
from constants import *

class BaseTableHandler(BaseDBHandler):
    def __init__(self, db_path: str, schema_path: str):
        super().__init__(db_path)
        with open(schema_path, 'r') as f:
            self.schemas = json.load(f)

    def get_schema(self, table_name: str) -> Dict[str, Any]:
        return self.schemas.get(table_name, {})

    def get_columns(self, table_name: str) -> List[str]:
        schema = self.get_schema(table_name)
        return [col['name'] for col in schema.get('columns', [])]

    def get_primary_key(self, table_name: str) -> List[str]:
        schema = self.get_schema(table_name)
        return schema.get('primary_key', [])