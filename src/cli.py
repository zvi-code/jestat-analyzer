# jeanalyzer/cli.py
#!/usr/bin/env python3
import argparse
import sys
import os
from .analyzer.je_analyzer import JeAnalyzer

def main():
    parser = argparse.ArgumentParser(description='Analyze jemalloc statistics')
    parser.add_argument('db_path', help='Path to SQLite database')
    parser.add_argument('--mode', choices=['raw', 'stats', 'arena', 'meta'], 
                      default='raw', help='Analysis mode')
    parser.add_argument('--table', help='Table pattern to analyze (e.g., "arenas_0*")')
    parser.add_argument('--timestamp', help='Filter by timestamp')
    parser.add_argument('--limit', type=int, default=10, 
                      help='Limit number of rows in display (default: 10)')
    
    args = parser.parse_args()

    if not os.path.exists(args.db_path):
        print(f"Error: Database file not found: {args.db_path}")
        sys.exit(1)

    try:
        analyzer = JeAnalyzer(args.db_path)
        analyzer.analyze(args.mode, args.table, args.timestamp, args.limit)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()