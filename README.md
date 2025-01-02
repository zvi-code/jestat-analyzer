# JEmalloc Stats Analyzer Usage Guide

This tool helps analyze jemalloc statistics by providing various analysis modes and customizable queries.

## Basic Usage

```bash
$ je-analyze <database_path> [options]
```

### Common Options

- `--mode <analysis_mode>`: Specify the analysis type
- `--table <table_name>`: View raw table data
- `--limit <n>`: Limit the number of rows displayed
- `--config <path>`: Specify custom configuration file (default: `config/analyzer_config.json`)
- `--list-tables`: Lists all tables available in the database. Can be combined with `--prefix` to filter tables by a specific prefix.
- `--prefix <prefix>`: Filter tables by a specific prefix (e.g., "merged" or "arenas").

## Generating the scheme for a db
```bash
$ python ./config/schemas_generator.py ../stats3.db ./config/table_schemas_gen.json
```

### Dependency between the analyzewr_config.json and the scheme

```json
{
    "schema_path": "config/table_schemas_gen.json",
    "analyses": {
      "bins_analysis": {
        "table": "merged*stats__bins_v1",
        "metrics": [
          {"name": "total_allocated", "column": "allocated_3", "operation": "sum"},
          {"name": "avg_utilization", "column": "util_16", "operation": "avg"},
          {"name": "total_slabs", "column": "curslabs_12", "operation": "sum"}
        ],
        "groupby": ["bins_0", "size_1"]
      },
    // rest of the json file
}
```


## Analysis Modes

### 1. Raw Table View

View raw data from any table in the database.

```bash
# View first 10 rows of a specific table
$ je-analyze stats.db --table stats-merged_arena_stats__bins_v1 --limit 10

# View all tables in the database
$ je-analyze stats.db --list-tables

# View tables filtered by prefix
$ je-analyze stats.db --list-tables --prefix "merged"
```

### 2. Bin Activity Analysis

Analyze allocation patterns and activity across different bin sizes.

```bash
$ je-analyze stats.db --mode bin_activity_analysis
```

This analysis shows:

- Most active bins by request count
- Allocation/deallocation patterns
- Cache hit ratios
- Fill/flush operations
- Lock contention metrics

**Example output:**

```
Top 5 Most Active Bins (by total requests):
Bin 5 (size 48 bytes):
  Total Requests: 16,075,645
  Allocations: 6,739,560
  Cache hit ratio: 58.08%
  Current allocation rate: 106,977/s
...
```

### 3. Memory Pages Analysis

Analyze page usage and memory efficiency.

```bash
$ je-analyze stats.db --mode bin_pages_analysis
```

Shows:

- Pages used per bin
- Memory utilization
- Slab allocation patterns
- Overall memory efficiency

### 4. Arena Comparison

Compare activity and memory usage across different arenas.

```bash
$ je-analyze stats.db --mode arena_comparison
```

Displays:

- Memory distribution across arenas
- Allocation rates per arena
- Memory utilization per arena
- Lock contention patterns

### 5. High Contention Analysis

Identify bins with high lock contention.

```bash
$ je-analyze stats.db --mode high_contention_bins
```

Shows:

- Lock wait times
- Contention rates
- Owner switch frequencies
- Impact on allocation performance

## Advanced Usage

### Custom Analysis Configuration

You can define custom analyses in the configuration file. Example configuration:

```json
{
    "custom_analysis": {
        "table": "stats-merged_arena_stats__bins_v1",
        "metrics": [
            {
                "name": "total_allocated",
                "operation": "sum",
                "column": "allocated_3"
            },
            {
                "name": "efficiency",
                "operation": "expression",
                "formula": {
                    "row_operation": "util_16",
                    "aggregation": "avg",
                    "filter": "allocated_3 > 1000",
                    "having": "< 0.5"
                }
            }
        ],
        "groupby": ["bins_0"],
        "sort": {
            "by": "total_allocated",
            "order": "desc"
        }
    }
}
```

#### Metric Types

- **Simple Aggregations:**

```json
{
    "name": "total_allocated",
    "operation": "sum",
    "column": "allocated_3"
}
```

- **Expressions with Filtering:**

```json
{
    "name": "efficiency",
    "operation": "expression",
    "formula": {
        "row_operation": "CAST(nonfull_slabs_13 AS FLOAT) / NULLIF(curslabs_12, 0)",
        "aggregation": "avg",
        "filter": "curslabs_12 >= 5",
        "having": "> 0.3"
    }
}
```

#### Sorting Results

Add sorting to any analysis:

```json
{
    "sort": {
        "by": "total_requests",
        "order": "desc"
    }
}
```

Or multiple sort criteria:

```json
{
    "sort": [
        {
            "by": "total_requests",
            "order": "desc"
        },
        {
            "by": "utilization",
            "order": "asc"
        }
    ]
}
```

## Common Analysis Patterns

### 1. Finding Memory Inefficiencies

```bash
# Find bins with poor utilization
$ je-analyze stats.db --mode inefficient_bins

# Analyze page usage
$ je-analyze stats.db --mode bin_pages_analysis
```

### 2. Identifying Performance Bottlenecks

```bash
# Check lock contention
$ je-analyze stats.db --mode high_contention_bins

# Analyze activity patterns
$ je-analyze stats.db --mode bin_activity_analysis
```

### 3. Understanding Memory Distribution

```bash
# Compare arena usage
$ je-analyze stats.db --mode arena_comparison

# Analyze bin allocation patterns
$ je-analyze stats.db --mode bins_analysis
```

## Tips and Best Practices

### Start with High-Level Analyses

- Use `arena_comparison` to understand memory distribution
- Use `bin_activity_analysis` to identify hot spots

### Drill Down into Specific Issues

- Use `high_contention_bins` for lock contention
- Use `bin_pages_analysis` for memory efficiency

### When Analyzing Performance

- Look at cache hit ratios
- Monitor lock contention rates
- Check fill/flush patterns

### For Memory Optimization

- Focus on bins with low utilization
- Check page usage efficiency
- Monitor fragmentation patterns

## Troubleshooting

### Common Issues and Solutions

- **"No tables found matching pattern":**
  - Use `--list-tables` to list available tables
  - Check table name spelling

- **"Column not found in schema":**
  - Verify column names in your configuration
  - Check table schema with `--table <table_name>`

- **Performance issues with large datasets:**
  - Use `--limit` to restrict output
  - Add appropriate filters in your analysis configuration

