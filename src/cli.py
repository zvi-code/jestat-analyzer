#!/usr/bin/env python3
import argparse
import sys
import os
import json

# Add the parent directory of 'src' to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.analyzer.je_analyzer import JeAnalyzer

def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser(description='Analyze jemalloc statistics')
    parser.add_argument('db_path', help='Path to SQLite database')
    parser.add_argument('--config', default='config/analyzer_config.json', help='Path to analyzer configuration file')
    parser.add_argument('--mode',  
                        default='table', help='Analysis mode')
    parser.add_argument('--table', help='Table name or pattern to analyze')
    parser.add_argument('--timestamp', help='Filter by timestamp')
    parser.add_argument('--limit', type=int, default=10, help='Limit number of rows in display (default: 10)')
    parser.add_argument('--list-tables', action='store_true', help='List all tables in the database')
    # Add this new argument
    parser.add_argument('--prefix', help='Filter tables by prefix (e.g., "merged" or "arenas")')
    args = parser.parse_args()

    if not os.path.exists(args.db_path):
        print(f"Error: Database file not found: {args.db_path}")
        sys.exit(1)

    if not os.path.exists(args.config):
        print(f"Error: Configuration file not found: {args.config}")
        sys.exit(1)

    config = load_config(args.config)
    
    try:
        analyzer = JeAnalyzer(args.db_path, config)
        # Add this block to handle the --list-tables argument
        if args.list_tables:
            tables = analyzer.list_tables(prefix=args.prefix)
            print("\nAvailable tables in database:")
            if args.prefix:
                print(f"(filtered by prefix: '{args.prefix}')")
            for table in sorted(tables):  # Sort tables for better readability
                print(f"- {table}")
            return
        analyzer.analyze(args.mode, args.table, args.timestamp, args.limit)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()