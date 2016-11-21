---
title: "Configuring the Drill Shell"
date: 2016-11-21 22:14:41 UTC
parent: "Configure Drill"
---
After [starting the Drill shell]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/), you can type queries on the shell command line. At the Drill shell command prompt, typing "help" lists the configuration and other options you can set to manage shell functionality. Apache Drill 1.0 and later formats the resultset output tables for readability if possible. In this release, columns having 70 characters or more cannot be formatted. This document formats all output for readability and example purposes.

Formatting tables takes time, which you might notice if running a huge query using the default `outputFormat` setting, which is `table` of the Drill shell. You can set another, more performant table formatting such as `csv`, as shown in the [examples]({{site.baseurl}}/docs/configuring-the-drill-shell/#examples-of-configuring-the-drill-shell). 


## Drill Shell Commands

The following table lists the commands that you can run on the Drill command line.

| Command       | Description                                                                                                                           |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------|
| !brief        | Set verbose mode off.                                                                                                                 |
| !close        | Close the current connection to the database.                                                                                         |
| !closeall     | Close all current open connections.                                                                                                   |
| !connect      | Open a new connection to the database. Use this command to hide the password when starting Drill in authentication mode.              |
| !help         | Print a summary of command usage.                                                                                                     |
| !history      | Display the command history.                                                                                                          |
| !list         | List the current connections.                                                                                                         |
| !outputformat | Set the output format for displaying results.                                                                                         |
| !properties   | Connect to the database specified in the properties file(s).                                                                          |
| !quit         | Exits the Drill shell.                                                                                                                |
| !reconnect    | Reconnect to the database.                                                                                                            |
| !record       | Record all output to the specified file.                                                                                              |
| !run          | Run a script from the specified file.                                                                                                 |
| !save         | Save the current variables and aliases.                                                                                               |
| !script       | Start saving a script to a file.                                                                                                      |
| !set          | Set the given variable. See [Set Command Variables]({{site.baseurl}}/docs/configuring-the-drill-shell/#the-set-command-variables).    |
| !tables       | List all the tables in the database.                                                                                                  |
| !verbose      | Show unabbreviated error messages.                                                                                                    |

## Example of Hiding the Password When Starting Drill

When starting Drill in authentication mode, you can use the **!connect** command as shown in the section, ["User Authentication Process"]({{site.baseurl}}/docs/configuring-user-authentication/#user-authentication-process), instead of using a command such as **sqlline**, **drill-embedded**, or **drill-conf** commands. For example, after running the sqlline script, you enter this command to connect to Drill:

`sqlline> !connect jdbc:drill:zk=localhost:2181`  

When prompted you enter a user name and password, which is hidden as you type it.

## Examples of Configuring the Drill Shell

For example, quit the Drill shell:

    0: jdbc:drill:zk=local> !quit

List the current connections. 

    0: jdbc:drill:zk=local> !list
    1 active connection:
     #0  open     jdbc:drill:zk=local

Set the maximum width of the Drill shell to 10000.

     0: jdbc:drill:zk=local> !set maxwidth 10000

Set the output format to CSV to improve performance of a huge query.

     0: jdbc:drill:zk=local> !set outputFormat csv

## The Set Command Variables

| Variable Name   | Valid Variable Values  | Description                                                                                                                                                            |
|-----------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| autoCommit      | true/false             | Enable/disable automatic transaction commit. Should remain enabled (true).                                                                                             |
| autoSave        | true/false             | Automatically save preferences.                                                                                                                                        |
| color           | true/false             | Control whether color is used for display.                                                                                                                             |
| fastConnect     | true/false             | Skip building table/column list for tab-completion.                                                                                                                    |
| force           | true/false             | Continue running script even after errors.                                                                                                                             |
| headerInterval  | \<integer\>            | The interval between which headers are displayed.                                                                                                                      |
| historyFile     | \<path\>               | File in which to save command history. Default is $HOME/.sqlline/history (UNIX, Linux, Mac OS), $HOME/sqlline/history (Windows).                                       |
| incremental     | true/false             | Do not receive all rows from server before printing the first row. Uses fewer resources, especially for long-running queries, but column widths may be incorrect.      |
| isolation       | \<level\>              | Set transaction isolation level.                                                                                                                                       |
| maxColumnWidth  | \<integer\>            | Maximum width for displaying columns.                                                                                                                                  |
| maxHeight       | \<integer\>            | Maximum height of the terminal.                                                                                                                                        |
| maxWidth        | \<integer\>            | Maximum width of the terminal.                                                                                                                                         |
| numberFormat    | \<pattern\>            | Format numbers using DecimalFormat pattern.                                                                                                                            |
| outputFormat    | table/vertical/csv/tsv | Format mode for result display.                                                                                                                                        |
| properties      | \<path\>               | File from which the shell reads properties on startup. Default is $HOME/.sqlline/sqlline.properties (UNIX, Linux, Mac OS), $HOME/sqlline/sqlline.properties (Windows). |
| rowLimit        | \<integer\>            | Maximum number of rows returned from a query; zero means no limit.                                                                                                     |
| showElapsedTime | true/false             | Display execution time when verbose.                                                                                                                                   |
| showHeader      | true/false             | Show column names in query results.                                                                                                                                    |
| showNestedErrs  | true/false             | Display nested errors.                                                                                                                                                 |
| showWarnings    | true/false             | Display connection warnings.                                                                                                                                           |
| silent          | true/false             | Disable or enable showing information specified by show commands.                                                                                                      |
| timeout         | \<integer\>            | Query timeout in seconds; less than zero means no timeout.                                                                                                             |
| trimScripts     | true/false             | Remove trailing spaces from lines read from script files.                                                                                                              |
| verbose         | true/false             | Show unabbreviated error messages and debug info.                                                                                                                      |

### autoCommit

Drill performs read-only operations primarily, and autocommits writes. Drill JDBC throws an exception if autoCommit is disabled.
