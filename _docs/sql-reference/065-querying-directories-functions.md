---
title: "Querying Directories Functions"
parent: "SQL Reference"
---
The following functions are SQL function extensions available in Drill 1.0 and later.

* MAXDIR
* MINDIR
* IMAXDIR
* IMINDIR

These functions in directory queries restrict the query to the first or last  directory in the sorted list of subdirectories. The MAXDIR and MINDIR functions are case-sensitive. The IMAXDIR and IMINDIR functions are case-insensitive.

## MAXDIR
Returns the name of the first directory in an ascending list of case-sensitive directory names

### MAXDIR Syntax

    MAXDIR('<plugin>.<workspace>', '<table name>') FROM <directory of subdirectories>

### MAXDIR Usage Notes 
To prevent Drill from scanning all data in directories, use MAXDIR instead of the MAX aggregate function to select the first directory in the list of directories.

    SELECT * FROM dfs.my_workspace.`my_table/*/` WHERE dir0 = MAXDIR('dfs.my_workspace', 'my_table');

Enclose arguments to MAXDIR in single-quotes, not backticks, the first parameter is the plugin and workspace and the second is the table name.