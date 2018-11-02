---
title: "ALTER SYSTEM"
date: 2018-11-02
parent: "SQL Commands"
---
The ALTER SYSTEM command permanently changes a system setting. The new setting
persists across all sessions. Session level settings override system level
settings.

## Syntax

The ALTER SYSTEM command supports the following syntax:

    ALTER SYSTEM SET `option_name` = value;  
    ALTER SYSTEM RESET `option_name`;
    ALTER SYSTEM RESET ALL;

## Parameters

*option_name*

This is the option name as it appears in the systems table.

_value_

A value of the type listed in the sys.options table: number, string, boolean,
or float. Use the appropriate value type for each option that you set.

## Usage Notes

Use the ALTER SYSTEM SET command to permanently set Drill query planning and
execution options per cluster. Options set at the system level affect the
entire system and persist between restarts.

Using ALTER SYSTEM RESET changes the value of an option back to the default system setting. 

Using ALTER SYSTEM RESET ALL changes the value of every option back to the default system setting.  

You can run the following query to see a complete list of planning and
execution options that are currently set at the system or session level:

    0: jdbc:drill:zk=local> SELECT name, type FROM sys.options WHERE type in ('SYSTEM','SESSION') order by name;
    +------------+------------------------------------------------+
    |   name                                         |    type    |
    +----------------------------------------------+--------------+
    | drill.exec.functions.cast_empty_string_to_null | SYSTEM     |
    | drill.exec.storage.file.partition.column.label | SYSTEM     |
    | exec.errors.verbose                            | SYSTEM     |
    | exec.java_compiler                             | SYSTEM     |
    | exec.java_compiler_debug                       | SYSTEM     |
    …
    +------------+------------------------------------------------+

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

Issuing the ALTER SYSTEM RESET command resets the option back to the default system value (false):  

    0: jdbc:drill:zk=local> ALTER SYSTEM RESET `planner.add_producer_consumer`;  

Issuing the ALTER SYSTEM RESET ALL command resets all options back to their default system values:  

    0: jdbc:drill:zk=local> ALTER SYSTEM RESET ALL;

  

