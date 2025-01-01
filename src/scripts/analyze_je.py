#!/usr/bin/env python3
import argparse
import sys
import os
import json
# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ..analyzer.je_analyzer import JeAnalyzer

def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)
    
def main():
    parser = argparse.ArgumentParser(description='Analyze jemalloc statistics')
    parser.add_argument('db_path', help='Path to SQLite database')
    parser.add_argument('--mode', choices=['raw', 'stats', 'arena', 'meta', 'bins'], 
                        default='raw', help='Analysis mode')
    parser.add_argument('--table', help='Table pattern to analyze (e.g., "bins_v1")')
    parser.add_argument('--timestamp', help='Filter by timestamp')
    parser.add_argument('--limit', type=int, default=10, 
                        help='Limit number of rows in display (default: 10)')
    parser.add_argument('--config', default='config/analyzer_config.json', help='Path to json configuration files')

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
        analyzer.analyze(args.mode, args.table, args.timestamp, args.limit)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()