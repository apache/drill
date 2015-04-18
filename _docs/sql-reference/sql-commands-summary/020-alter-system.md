---
title: "ALTER SYSTEM Command"
parent: "SQL Commands Summary"
---
The ALTER SYSTEM command permanently changes a system setting. The new setting
persists across all sessions. Session level settings override system level
settings.

## Syntax

The ALTER SYSTEM command supports the following syntax:

    ALTER SYSTEM SET `<option_name>`=<value>;

## Parameters

*option_name*

This is the option name as it appears in the systems table.

_value_

A value of the type listed in the sys.options table: number, string, boolean,
or float. Use the appropriate value type for each option that you set.

## Usage Notes

Use the ALTER SYSTEM command to permanently set Drill query planning and
execution options per cluster. Options set at the system level affect the
entire system and persist between restarts.

You can run the following query to see a complete list of planning and
execution options that you can set at the system level:

    0: jdbc:drill:zk=local> select name, type, num_val, string_val, bool_val, float_val from sys.options where type like 'SYSTEM' order by name;
    +------------+------------+------------+------------+------------+------------+
    |    name    |    type    |  num_val   | string_val |  bool_val  | float_val  |
    +------------+------------+------------+------------+------------+------------+
    | drill.exec.functions.cast_empty_string_to_null | SYSTEM     | null       | null       | false      | null       |
    | drill.exec.storage.file.partition.column.label | SYSTEM     | null       | dir        | null       | null       |
    | exec.errors.verbose | SYSTEM     | null       | null       | false      | null       |
    | exec.java_compiler | SYSTEM     | null       | DEFAULT    | null       | null       |
    | exec.java_compiler_debug | SYSTEM     | null       | null       | true       | null       |
    | exec.java_compiler_janino_maxsize | SYSTEM     | 262144     | null       | null       | null       |
    | exec.queue.timeout_millis | SYSTEM     | 400000     | null       | null       | null       |
    | planner.add_producer_consumer | SYSTEM     | null       | null       | true       | null       |
    | planner.affinity_factor | SYSTEM     | null       | null       | null       | 1.2        |
    | planner.broadcast_threshold | SYSTEM     | 1000000    | null       | null       | null       |
    | planner.disable_exchanges | SYSTEM     | null       | null       | false      | null       |
    | planner.enable_broadcast_join | SYSTEM     | null       | null       | true       | null       |
    | planner.enable_hash_single_key | SYSTEM     | null       | null       | true       | null       |
    | planner.enable_hashagg | SYSTEM     | null       | null       | true       | null       |
    | planner.enable_hashjoin | SYSTEM     | null       | null       | true       | null       |
    | planner.slice_target | SYSTEM     | 100000     | null       | null       | null       |
    | planner.width.max_per_node | SYSTEM     | 2          | null       | null       | null       |
    | planner.width.max_per_query | SYSTEM     | 1000       | null       | null       | null       |
    | store.format | SYSTEM     | null       | parquet    | null       | null       |
    | store.json.all_text_mode | SYSTEM     | null       | null       | false      | null       |
    | store.mongo.all_text_mode | SYSTEM     | null       | null       | false      | null       |
    | store.parquet.block-size | SYSTEM     | 536870912  | null       | null       | null       |
    | store.parquet.use_new_reader | SYSTEM     | null       | null       | false      | null       |
    | store.parquet.vector_fill_check_threshold | SYSTEM     | 10         | null       | null       | null       |
    | store.parquet.vector_fill_threshold | SYSTEM     | 85         | null       | null       | null       |
    +------------+------------+------------+------------+------------+------------+

{% include startnote.html %}This is a truncated version of the list.{% include endnote.html %}

## Example

This example demonstrates how to use the ALTER SYSTEM command to set the
`planner.add_producer_consumer` option to “true.” This option enables a
secondary reading thread to prefetch data from disk.

    0: jdbc:drill:zk=local> alter system set `planner.add_producer_consumer` = true;
    +------------+------------+
    |   ok  |  summary   |
    +------------+------------+
    | true      | planner.add_producer_consumer updated. |
    +------------+------------+
    1 row selected (0.046 seconds)

You can issue a query to see all of the system level settings set to “true.”
Note that the option type is case-sensitive.

    0: jdbc:drill:zk=local> SELECT name, type, bool_val FROM sys.options WHERE type = 'SYSTEM' and bool_val=true;
    +------------+------------+------------+
    |   name    |   type    |  bool_val  |
    +------------+------------+------------+
    | exec.java_compiler_debug | SYSTEM     | true      |
    | planner.enable_mergejoin | SYSTEM     | true      |
    | planner.enable_broadcast_join | SYSTEM    | true      |
    | planner.enable_hashagg | SYSTEM   | true      |
    | planner.add_producer_consumer | SYSTEM    | true      |
    | planner.enable_hash_single_key | SYSTEM   | true      |
    | planner.enable_multiphase_agg | SYSTEM    | true      |
    | planner.enable_streamagg | SYSTEM     | true      |
    | planner.enable_hashjoin | SYSTEM  | true      |
    +------------+------------+------------+
    9 rows selected (0.159 seconds)

  

