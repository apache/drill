---
title: "ALTER SESSION Command"
parent: "SQL Commands Summary"
---
The ALTER SESSION command changes a system setting for the duration of a
session. Session level settings override system level settings.

## Syntax

The ALTER SESSION command supports the following syntax:

    ALTER SESSION SET `<option_name>`=<value>;

## Parameters

*option_name*  
This is the option name as it appears in the systems table.

*value*  
A value of the type listed in the sys.options table: number, string, boolean,
or float. Use the appropriate value type for each option that you set.

## Usage Notes

Use the ALTER SESSION command to set Drill query planning and execution
options per session in a cluster. The options that you set using the ALTER
SESSION command only apply to queries that run during the current Drill
connection. A session ends when you quit the Drill shell. You can set any of
the system level options at the session level.

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

**Note:** This is a truncated version of the list.

## Example

This example demonstrates how to use the ALTER SESSION command to set the
`store.json.all_text_mode` option to “true” for the current Drill session.
Setting this option to “true” enables text mode so that Drill reads everything
in JSON as a text object instead of trying to interpret data types. This
allows complicated JSON to be read using CASE and CAST.

    0: jdbc:drill:zk=local> alter session set `store.json.all_text_mode`= true;
    +------------+------------+
    |   ok  |  summary   |
    +------------+------------+
    | true      | store.json.all_text_mode updated. |
    +------------+------------+
    1 row selected (0.046 seconds)

You can issue a query to see all of the session level settings. Note that the
option type is case-sensitive.

    0: jdbc:drill:zk=local> SELECT name, type, bool_val FROM sys.options WHERE type = 'SESSION' order by name;
    +------------+------------+------------+
    |   name    |   type    |  bool_val  |
    +------------+------------+------------+
    | store.json.all_text_mode | SESSION    | true      |
    +------------+------------+------------+
    1 row selected (0.176 seconds)

