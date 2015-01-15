---
title: "Starting/Stopping Drill"
parent: "Manage Drill"
---
How you start Drill depends on the installation method you followed. If you
installed Drill in embedded mode, invoking SQLLine automatically starts a
Drillbit locally. If you installed Drill in distributed mode on one or
multiple nodes in a cluster, you must start the Drillbit service and then
invoke SQLLine. Once SQLLine starts, you can issue queries to Drill.

### Starting a Drillbit

If you installed Drill in embedded mode, you do not need to start the
Drillbit.

To start a Drillbit, navigate to the Drill installation directory, and issue
the following command:

`bin/drillbit.sh restart`

### Invoking SQLLine/Connecting to a Schema

SQLLine is used as the Drill shell. SQLLine connects to relational databases
and executes SQL commands. You invoke SQLLine for Drill in embedded or
distributed mode. If you want to connect directly to a particular schema, you
can indicate the schema name when you invoke SQLLine.

To start SQLLine, issue the appropriate command for your Drill installation
type:

<div class="table-wrap"><table class="confluenceTable"><tbody><tr><td class="confluenceTd"><p><strong>Drill Install Type</strong></p></td><td class="confluenceTd"><p><strong>Example</strong></p></td><td class="confluenceTd"><p><strong>Command</strong></p></td></tr><tr><td class="confluenceTd"><p>Embedded</p></td><td class="confluenceTd"><p>Drill installed locally (embedded mode);</p><p>Hive with embedded metastore</p></td><td class="confluenceTd"><p>To connect without specifying a schema, navigate to the Drill installation directory and issue the following command:</p><p><code>$ bin/sqlline -u jdbc:drill:zk=local -n admin -p admin </code><span> </span></p><p>Once you are in the prompt, you can issue<code> USE &lt;schema&gt; </code>or you can use absolute notation: <code>schema.table.column.</code></p><p>To connect to a schema directly, issue the command with the schema name:</p><p><code>$ bin/sqlline -u jdbc:drill:schema=&lt;database&gt;;zk=local -n admin -p admin</code></p></td></tr><tr><td class="confluenceTd"><p>Distributed</p></td><td class="confluenceTd"><p>Drill installed in distributed mode;</p><p>Hive with remote metastore;</p><p>HBase</p></td><td class="confluenceTd"><p>To connect without specifying a schema, navigate to the Drill installation directory and issue the following command:</p><p><code>$ bin/sqlline -u jdbc:drill:zk=&lt;zk1host&gt;:&lt;port&gt;,&lt;zk2host&gt;:&lt;port&gt;,&lt;zk3host&gt;:&lt;port&gt; -n admin -p admin</code></p><p>Once you are in the prompt, you can issue<code> USE &lt;schema&gt; </code>or you can use absolute notation: <code>schema.table.column.</code></p><p>To connect to a schema directly, issue the command with the schema name:</p><p><code>$ bin/sqlline -u jdbc:drill:schema=&lt;database&gt;;zk=&lt;zk1host&gt;:&lt;port&gt;,&lt;zk2host&gt;:&lt;port&gt;,&lt;zk3host&gt;:&lt;port&gt; -n admin -p admin</code></p></td></tr></tbody></table></div>
  
When SQLLine starts, the system displays the following prompt:

`0: [jdbc:drill](http://jdbcdrill):schema=<database>;zk=<zkhost>:<port>>`

At this point, you can use Drill to query your data source or you can discover
metadata.

### Exiting SQLLine

To exit SQLLine, issue the following command:

`!quit`  

