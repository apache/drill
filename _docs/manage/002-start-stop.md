---
title: "Starting/Stopping Drill"
parent: "Manage Drill"
---
How you start Drill depends on the installation method you followed. If you
installed Drill in embedded mode, invoking SQLLine automatically starts a
Drillbit locally. If you installed Drill in distributed mode on one or
multiple nodes in a cluster, you must start the Drillbit service and then
invoke SQLLine. Once SQLLine starts, you can issue queries to Drill.

## Starting a Drillbit

If you installed Drill in embedded mode, you do not need to start the
Drillbit.

To start a Drillbit, navigate to the Drill installation directory, and issue
the following command:

`bin/drillbit.sh restart`

## Invoking SQLLine/Connecting to a Schema

SQLLine is used as the Drill shell. SQLLine connects to relational databases
and executes SQL commands. You invoke SQLLine for Drill in embedded or
distributed mode. If you want to connect directly to a particular schema, you
can indicate the schema name when you invoke SQLLine.

To start SQLLine, issue the appropriate command for your Drill installation
type:

<table ><tbody><tr><td valign="top"><strong>Drill Install Type</strong></td><td valign="top"><strong>Example</strong></td><td valign="top"><strong>Command</strong></td></tr><tr><td valign="top">Embedded</td><td valign="top">Drill installed locally (embedded mode);Hive with embedded metastore</td><td valign="top">To connect without specifying a schema, navigate to the Drill installation directory and issue the following command:<code>$ bin/sqlline -u jdbc:drill:zk=local -n admin -p admin </code><span> </span>Once you are in the prompt, you can issue<code> USE &lt;schema&gt; </code>or you can use absolute notation: <code>schema.table.column.</code>To connect to a schema directly, issue the command with the schema name:<code>$ bin/sqlline -u jdbc:drill:schema=&lt;database&gt;;zk=local -n admin -p admin</code></td></tr><tr><td valign="top">Distributed</td><td valign="top">Drill installed in distributed mode;Hive with remote metastore;HBase</td><td valign="top">To connect without specifying a schema, navigate to the Drill installation directory and issue the following command:<code>$ bin/sqlline -u jdbc:drill:zk=&lt;zk1host&gt;:&lt;port&gt;,&lt;zk2host&gt;:&lt;port&gt;,&lt;zk3host&gt;:&lt;port&gt; -n admin -p admin</code>Once you are in the prompt, you can issue<code> USE &lt;schema&gt; </code>or you can use absolute notation: <code>schema.table.column.</code>To connect to a schema directly, issue the command with the schema name:<code>$ bin/sqlline -u jdbc:drill:schema=&lt;database&gt;;zk=&lt;zk1host&gt;:&lt;port&gt;,&lt;zk2host&gt;:&lt;port&gt;,&lt;zk3host&gt;:&lt;port&gt; -n admin -p admin</code></td></tr></tbody></table>
  
When SQLLine starts, the system displays the following prompt:

`0: jdbc:drill]:schema=<database>;zk=<zkhost>:<port>`

At this point, you can use Drill to query your data source or you can discover
metadata.

## Exiting SQLLine

To exit SQLLine, issue the following command:

`!quit`  

