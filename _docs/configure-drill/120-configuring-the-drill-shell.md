---
title: "Configuring the Drill Shell"
date: 2018-12-28
parent: "Configure Drill"
---  
Drill uses SQLLine as the Drill shell. SQLLine is a pure-Java console-based utility for connecting to relational databases and running SQL commands. 

Starting in Drill 1.15, Drill uses SQLLine 1.6, which you can customize through the Drill [configuration file, drill-sqlline-override.conf]({{site.baseurl}}/docs/configuring-the-drill-shell/#customizing-sqlline-in-the-drill-sqlline-override-conf-file). Before installing and running Drill with SQLLine 1.6, delete the old SQLLine history file located in:  


- $HOME/.sqlline/history (UNIX, Linux, Mac OS)
- $HOME/sqlline/history (Windows) 


After [starting the Drill shell]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/), you can run Drill shell commands and queries from the command line. Typing the shell command "!help" lists configuration and other options that you can set to manage shell functionality. 

Formatting tables takes time, which you may notice when running a huge query using the default outputFormat. The default outputFormat is “table.” You can set the outputFormat to a more performant table formatting, such as csv, as shown in the [examples]({{site.baseurl}}/docs/configuring-the-drill-shell/#examples-of-configuring-the-drill-shell).  

## SQLLine Connection Parameters  

You can use a jdbc connection string to connect to SQLLine when Drill is installed in embedded mode or distributed mode, as shown in the following examples:  

- Embedded mode:  
`./sqlline -u jdbc:drill:drillbit=local`  
- Distributed mode:  
`./sqlline –u jdbc:drill:zk=cento23,centos24,centos26:5181`   

When you use a jdbc connection string to connect to Drill via SQLLine, you can include SQLLine connection parameters in the connection string, as shown in the following example:  

`./sqlline -u 'jdbc:drill:drillbit=local' --help`  

**Note:** Use single quotes around the jdbc portion of the connection string when using SQLLine parameters.  
 
The --help command prints all the SQLLine connection parameters that you can include in the jdbc connection string, as shown in the following table:
  

| **Connection Parameters**                                      | **Description**                                                 |
|----------------------------------------------------------------------------|-------------------------------------------------------------|
|     -u <database url>                                                      |  the JDBC URL to connect to                                 |
|      -n <username>                                                         |  the username to connect as                                 |
|      -p <password>                                                         | the password to connect as                                  |
|      -d <driver class>                                                     | the driver class to use                                     |
|      -e <command>                                                          | the command to execute                                      |
|      -nn <nickname>                                                        | nickname for the connection                                 |
|      -ch <command handler>[,<command handler>]*                            | a custom command handler to use                             |
|      -f <file>                                                             | script file to execute (same as --run)                      |
|      -log <file>                                                           | file to write output                                        |
|      -ac <class name>                                                      | application configuration class name                        |
|      --color=[true/false]                                                  | control whether color is used for display                   |
|      --colorScheme=[chester/dark/dracula/light/obsidian/solarized/vs2010]  | Syntax highlight schema                                     |
|      --csvDelimiter=[delimiter]                                            | Delimiter in csv outputFormat                               |
|      --csvQuoteCharacter=[char]                                            | Quote character in csv outputFormat                         |
|      --escapeOutput=[true/false]                                           | escape control symbols in output                            |
|      --showHeader=[true/false]                                             | show column names in query results                          |
|      --headerInterval=ROWS                                                 | the interval between which headers are displayed            |
|      --fastConnect=[true/false]                                            | skip building table/column list for tab-completion          |
|      --autoCommit=[true/false]                                             | enable/disable automatic transaction commit                 |
|      --verbose=[true/false]                                                | show verbose error messages and debug info                  |
|      --showTime=[true/false]                                               | display execution time when verbose                         |
|      --showWarnings=[true/false]                                           | display connection warnings                                 |
|      --showNestedErrs=[true/false]                                         | display nested errors                                       |
|      --strictJdbc=[true/false]                                             | use strict JDBC                                             |
|      --nullValue=[string]                                                  | use string in place of NULL values                          |
|      --numberFormat=[pattern]                                              | format numbers using DecimalFormat pattern                  |
|      --dateFormat=[pattern]                                                | format dates using SimpleDateFormat pattern                 |
|      --timeFormat=[pattern]                                                | format times using SimpleDateFormat pattern                 |
|      --timestampFormat=[pattern]                                           | format timestamps using SimpleDateFormat pattern            |
|      --force=[true/false]                                                  | continue running script even after errors                   |
|      --maxWidth=MAXWIDTH                                                   | the maximum width of the terminal                           |
|      --maxColumnWidth=MAXCOLWIDTH                                          | the maximum width to use when displaying columns            |
|      --maxHistoryFileRows=ROWS                                             | the maximum number of history rows to store in history file |
|      --maxHistoryRows=ROWS                                                 | the maximum number of history rows to store in memory       |
|      --mode=[emacs/vi]                                                     | the editing mode                                            |
|      --silent=[true/false]                                                 | be more silent                                              |
|      --autosave=[true/false]                                               | automatically save preferences                              |
|      --outputformat=[table/vertical/csv/tsv/xmlattrs/xmlelements/json]     | format mode for result display                              |
|      --isolation=LEVEL                                                     | set the transaction isolation level                         |
|      --run=/path/to/file                                                   | run one script and then exit                                |
|      --historyfile=/path/to/file                                           | use or create history file in specified path                |
|      --useLineContinuation=[true/false]                                    | Use line continuation                                       |
|      --help                                                                | display parameters and commands                             |  

### Example Connection String with Parameter  

This example shows a jdbc connection string that includes the `-e` parameter to pass a query during the connection to Drill. The `-e` parameter is equivalent to the `-q` parameter. This connection string starts Drill, runs the query and then closes the connection. This type of connection string is useful in a script. The connection is run from DRILL_HOME/bin. 

	[root@doc23 bin]# ./sqlline -u 'jdbc:drill:zk=local' -e 'select * from cp.`employee.json` limit 2;'  
	Dec 28, 2018 2:13:30 PM org.jline.utils.Log logr
	WARNING: Unable to create a system terminal, creating a dumb terminal (enable debug logging for more information)
	Apache Drill 1.15.0-SNAPSHOT
	"Say hello to my little Drill."
	0: jdbc:drill:zk=local>  
	+--------------+------------------+-------------+------------+--------------+---------------------+-----------+----------------+-------------+------------------------+----------+----------------+------------------+-----------------+---------+--------------------+
	| employee_id  |    full_name     | first_name  | last_name  | position_id  |   position_title    | store_id  | department_id  | birth_date  |       hire_date        |  salary  | supervisor_id  | education_level  | marital_status  | gender  |  management_role   |
	+--------------+------------------+-------------+------------+--------------+---------------------+-----------+----------------+-------------+------------------------+----------+----------------+------------------+-----------------+---------+--------------------+
	| 1            | Sheri Nowmer     | Sheri       | Nowmer     | 1            | President           | 0         | 1              | 1961-08-26  | 1994-12-01 00:00:00.0  | 80000.0  | 0              | Graduate Degree  | S               | F       | Senior Management  |
	| 2            | Derrick Whelply  | Derrick     | Whelply    | 2            | VP Country Manager  | 0         | 1              | 1915-07-03  | 1994-12-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree  | M               | M       | Senior Management  |
	+--------------+------------------+-------------+------------+--------------+---------------------+-----------+----------------+-------------+------------------------+----------+----------------+------------------+-----------------+---------+--------------------+
	2 rows selected (2.907 seconds)
	0: jdbc:drill:zk=local> Closing: org.apache.drill.jdbc.impl.DrillConnectionImpl


## Drill Shell Commands

The following table lists some of the Drill shell commands that you can run from the command line. To see a complete list of shell commands issue the `!help` command.   

**Note:** Some new shell commands, such as `!reset`, and key-strokes were introduced when Drill upgraded to SQLLine 1.6 (Drill 1.15 and later); however, not all of the commands are supported.   


| **Command**       | **Description **                                                                                                                          |
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
| !record       | Record all output to the specified file.
|
| !reset        | Reset a sqlline variable
|                                                                                             |
| !run          | Run a script from the specified file.                                                                                                 |
| !save         | Save the current variables and aliases.                                                                                               |
| !script       | Start saving a script to a file.                                                                                                      |
| !set          | Set the given variable. See [Set Command Variables]({{site.baseurl}}/docs/configuring-the-drill-shell/#set-command-variables).    |
| !tables       | List all the tables in the database.                                                                                                  |
| !verbose      | Show unabbreviated error messages.                                                                                                    |



### Examples of Configuring the Drill Shell

For example, quit the Drill shell:

    0: jdbc:drill:zk=local> !quit

List the current connections. 

    0: jdbc:drill:zk=local> !list
    1 active connection:
     #0  open     jdbc:drill:zk=local  

### Example of Hiding the Password When Starting Drill

When starting Drill in authentication mode, you can use the **!connect** command as shown in the section, ["User Authentication Process"]({{site.baseurl}}/docs/configuring-user-authentication/#user-authentication-process), instead of using a command such as **sqlline**, **drill-embedded**, or **drill-conf** commands. For example, after running the sqlline script, you enter this command to connect to Drill:

`sqlline> !connect jdbc:drill:zk=localhost:2181`  

When prompted you enter a user name and password, which is hidden as you type it.


## Set Command Variables  
The following table lists some of the set command variables that you can use with the !set command: 

**Note:** Issue the !set command without a variable to see the full list of command variables.

| **Variable Name**   | **Valid Variable Values**  | **Description**                                                                                                                                                            |
|-----------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| autoCommit      | true/false             | Enable/disable automatic transaction commit. Should remain enabled (true). Drill performs read-only operations primarily, and autocommit writes. Drill JDBC throws an exception if autoCommit is disabled.                                                                                             |
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


###Examples of the set Command with Variables  
Set the maximum width of the Drill shell to 10000.

     0: jdbc:drill:zk=local> !set maxwidth 10000

Set the output format to CSV to improve performance of a huge query.

     0: jdbc:drill:zk=local> !set outputFormat csv  

## Customizing SQLLine in the drill-sqlline-override.conf File
Starting in Drill 1.15, SQLLine (the Drill shell) is upgraded to version 1.6. You can customize SQLLine through the Drill configuration file, `drill-sqlline-override.conf`, located in the `<drill-installation>/conf` directory. 

You can customize quotes of the day; the quotes you see at the command prompt when starting Drill, such as “Just Drill it,” and you can override the SQLLine default properties. The SQLLine default properties are those that print when you run `!set` from the Drill shell. 

Drill reads the `drill-sqlline-override.conf` file and applies the custom configuration during start-up. You must restart Drill for the changes to take effect. The file remains in the directory and Drill applies the custom configuration at each restart.  

**Note:** The SQLLine configuration file in the `<drill-installation>/conf` directory is named `drill-sqlline-override-example.conf`. Use this file and the information provided in the file as guidance for the `drill-sqlline-override.conf` file you create and store in the directory with your customizations.






