import sqlite3
import json
from typing import Dict, Any
from constants import *
def custom_json_formatter(obj: Any) -> str:
    """
    Custom JSON formatter to control the layout of the JSON output.
    """
    if isinstance(obj, dict):
        if len(obj) == 2 and "name" in obj and "type" in obj:
            # Format column entries in a single line
            return f'{{"name": "{obj["name"]}", "type": "{obj["type"]}"}}' 
        else:
            # Format other dictionary entries with normal indentation
            items = []
            for key, value in obj.items():
                if isinstance(value, list):
                    formatted_value = json.dumps(value, default=custom_json_formatter)
                else:
                    formatted_value = json.dumps(value)
                items.append(f'"{key}": {formatted_value}')
            return "{\n  " + ",\n  ".join(items) + "\n}"
    return str(obj)

def can_be_integer(cursor, table_name: str, column_name: str) -> bool:
    """
    Check if all non-NULL values in a column can be converted to integers.
    """
    try:
        cursor.execute(f"""
            SELECT DISTINCT "{column_name}" 
            FROM "{table_name}" 
            WHERE "{column_name}" IS NOT NULL 
            LIMIT 100
        """)
        values = cursor.fetchall()
        
        for (value,) in values:
            if value is not None:
                try:
                    float(value)
                except (ValueError, TypeError):
                    return False
        return True
    except sqlite3.Error:
        return False
def get_primary_key_columns(columns: list) -> list:
    """
    Determines primary key columns based on predefined rules.
    """
    always_primary = ["metric", "timestamp", "metadata_id", f"{COL_HEADER_FILLER}", f"bins", "Key", f"large", f"extents"]
    return [col["name"] for col in columns if col["name"] in always_primary]
def generate_db_schema(db_path: str, output_file: str) -> None:
    """
    Generates a JSON schema for all tables in the database.
    Sets column type to INTEGER if all values can be converted to numbers.
    
    Args:
        db_path (str): Path to the SQLite database
        output_file (str): Path where the JSON file should be saved
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        schema_dict = {}
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns_info = cursor.fetchall()
            
            columns = []
            for col in columns_info:
                col_name = col[1]
                col_type = col[2].upper()
                
                if can_be_integer(cursor, table_name, col_name):
                    col_type = "INTEGER"
                else:
                    col_type = "TEXT"
                
                columns.append({
                    "name": col_name,
                    "type": col_type
                })
            
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            primary_key_columns = get_primary_key_columns(columns)
            
            table_schema = {
                "columns": columns,
                "primary_key": primary_key_columns
            }
            
            schema_dict[table_name] = table_schema
        
                # Write to JSON file with custom formatting
            with open(output_file, 'w') as f:
                f.write('{\n')
                
                # Write each table
                for i, (table_name, table_data) in enumerate(schema_dict.items()):
                    f.write(f'  "{table_name}": {{\n')
                    f.write('    "columns": [\n')
                    
                    # Write each column on a single line
                    for j, column in enumerate(table_data['columns']):
                        comma = ',' if j < len(table_data['columns']) - 1 else ''
                        f.write(f'        {{"name": "{column["name"]}", "type": "{column["type"]}"}}{comma}\n')
                    
                    f.write('      ],\n')
                    
                    # Write primary key
                    f.write('      "primary_key": ')
                    f.write(json.dumps(table_data['primary_key']))
                    
                    # Close table
                    comma = ',' if i < len(schema_dict) - 1 else ''
                    f.write(f'\n    }}{comma}\n')
                
                f.write('}\n')
            
        print(f"Schema successfully written to {output_file}")
            
        print(f"Schema successfully written to {output_file}")
        
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

# Example usage:
# generate_db_schema("path/to/your/database.db", "output_schema.json")

# Example usage
# generate_db_schema("path/to/database.db", "output/schema.json")
# This will generate a JSON file with the schema of all tables in the database
# and save it to the specified output file.

#main
import argparse
import sys
import os
from src.db.display_handler import DisplayHandler
from src.db.base_table_handler import BaseTableHandler
from src.analyzer.je_analyzer import JeAnalyzer

def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)
    
def main():
    parser = argparse.ArgumentParser(description='Analyze jemalloc statistics')
    parser.add_argument('db_path', help='Path to SQLite database')
    parser.add_argument('schema_path', default='./analyzer_config_gen.json', help='Path to analyzer configuration file')
    
    args = parser.parse_args()

    if not os.path.exists(args.db_path):
        print(f"Error: Database file not found: {args.db_path}")
        sys.exit(1)

    try:
        generate_db_schema(args.db_path, args.schema_path)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

    
