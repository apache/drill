# EXCLUDE Clause in Apache Drill Window Functions

## Overview

The EXCLUDE clause allows you to exclude specific rows from window frame calculations. This feature is part of the SQL standard and was added in Calcite 1.38.

## Syntax

```sql
SELECT column_name,
       aggregate_function(...) OVER (
           [PARTITION BY ...]
           [ORDER BY ...]
           [frame_clause]
           EXCLUDE exclusion_type
       )
FROM table_name;
```

## Exclusion Types

### EXCLUDE NO OTHERS (default)

No rows are excluded from the frame. This is the default behavior when no EXCLUDE clause is specified.

```sql
SELECT n_regionkey,
       COUNT(*) OVER (
           PARTITION BY n_regionkey
           ORDER BY n_regionkey
           RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           EXCLUDE NO OTHERS
       ) AS total_count
FROM cp.`tpch/nation.parquet`;
```

### EXCLUDE CURRENT ROW

Excludes only the current row from the frame calculation.

```sql
SELECT n_regionkey,
       COUNT(*) OVER (
           PARTITION BY n_regionkey
           ORDER BY n_regionkey
           RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           EXCLUDE CURRENT ROW
       ) AS other_count
FROM cp.`tpch/nation.parquet`;
```

### EXCLUDE TIES

Excludes peer rows (rows with the same ORDER BY values) but keeps the current row.

```sql
SELECT n_regionkey,
       COUNT(*) OVER (
           PARTITION BY n_regionkey
           ORDER BY n_regionkey
           RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           EXCLUDE TIES
       ) AS self_only_count
FROM cp.`tpch/nation.parquet`;
```

### EXCLUDE GROUP

Excludes the current row and all its peer rows from the frame calculation.

```sql
SELECT n_regionkey,
       COUNT(*) OVER (
           PARTITION BY n_regionkey
           ORDER BY n_regionkey
           RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           EXCLUDE GROUP
       ) AS exclude_peers_count
FROM cp.`tpch/nation.parquet`;
```

## Supported Frame Types

The EXCLUDE clause is currently supported with:

- **RANGE frames**: Full support for all exclusion types
- **ROWS frames**: Supported for EXCLUDE NO OTHERS; other modes have limitations

## Common Use Cases

### Calculate differences from group average

```sql
SELECT employee_id, salary,
       AVG(salary) OVER (
           PARTITION BY department_id
           EXCLUDE CURRENT ROW
       ) AS avg_other_salaries,
       salary - AVG(salary) OVER (
           PARTITION BY department_id
           EXCLUDE CURRENT ROW
       ) AS diff_from_others
FROM employees;
```

### Count peer rows

```sql
SELECT order_date, order_id,
       COUNT(*) OVER (
           ORDER BY order_date
           RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           EXCLUDE CURRENT ROW
       ) AS other_orders_same_date
FROM orders;
```

### Running totals excluding peers

```sql
SELECT transaction_date, amount,
       SUM(amount) OVER (
           ORDER BY transaction_date
           RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           EXCLUDE TIES
       ) AS running_total_unique
FROM transactions;
```

## Notes

- When using EXCLUDE with RANGE frames, peer rows are determined by the ORDER BY clause. Rows with identical ORDER BY values are considered peers.
- For ROWS frames, all rows in the same partition are potential peers if they share ORDER BY values.
- EXCLUDE NO OTHERS is the default and can be omitted.
- The EXCLUDE clause must appear after the frame specification (ROWS/RANGE clause).

## Limitations

- ROWS frames with EXCLUDE CURRENT ROW, EXCLUDE TIES, or EXCLUDE GROUP may have limitations when partitioning by one column and aggregating a d
- ifferent column.
- For best results with complex EXCLUDE operations, use RANGE frames.

## Related Documentation

- [Calcite Window Functions](https://calcite.apache.org/docs/reference.html#window-functions)
- [SQL Standard Window Functions](https://www.iso.org/standard/63556.html)
