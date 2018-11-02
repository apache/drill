---
title: "RESET"
date: 2018-11-02
parent: "SQL Commands"
---
The RESET command is available in Drill version 1.3 and later. The RESET command resets a session level option back to its default system setting.

## Syntax

The RESET command supports the following syntax:  

    [ALTER SESSION] RESET `option_name`;  

## Parameters

*option_name*  
This is the option name as it appears in the systems table.

*value*  
A value of the type listed in the sys.options table: number, string, boolean,
or float. Use the appropriate value type for each option that you set.

## Usage Notes
You can use the ALTER SESSION RESET command, however ALTER SESSION is just an alias for the RESET command. Use the RESET command to change Drill query planning and execution
options set at the session level back to their default system settings. You can reset any options set at the session level. 

You can run the following query to see a complete list of planning and
execution options that are currently set at the system or session level:

    0: jdbc:drill:zk=local> SELECT name, type FROM sys.options WHERE type in ('SYSTEM','SESSION') order by name;
    +------------+----------------------------------------------+
    |   name                                       |    type    |
    +----------------------------------------------+------------+
    | drill.exec.functions.cast_empty_string_to_null | SYSTEM   |
    | drill.exec.storage.file.partition.column.label | SYSTEM   |
    | exec.errors.verbose                          | SYSTEM     |
    | exec.java_compiler                           | SYSTEM     |
    | exec.java_compiler_debug                     | SYSTEM     |
    …
    +------------+----------------------------------------------+

{% include startnote.html %}This is a truncated version of the list.{% include endnote.html %}

## Example

This example demonstrates how to use the RESET command to reset the
`store.json.all_text_mode` option back to the default system setting (false) which disables all text mode so that Drill interprets data types when querying data.  

       0: jdbc:drill:zk=local> RESET `store.json.all_text_mode`;
       +-------+------------------------------------+
       |  ok   |              summary               |
       +-------+------------------------------------+
       | true  | store.json.all_text_mode updated.  |
       +-------+------------------------------------+
       1 row selected (0.093 seconds)  

You can issue a query to see the default system setting for the option:  

       0: jdbc:drill:zk=local> SELECT name, type, bool_val FROM sys.options WHERE name='store.json.all_text_mode';
       +---------------------------+---------+-----------+
       |           name            |  type   | bool_val  |
       +---------------------------+---------+-----------+
       | store.json.all_text_mode  | SYSTEM  | false     |
       +---------------------------+---------+-----------+
