---
title: "Plugin Configuration Introduction"
parent: "Storage Plugin Configuration"
---
When you add or update storage plugin instances on one Drill node in a Drill
cluster, Drill broadcasts the information to all of the other Drill nodes 
to have identical storage plugin configurations. You do not need to
restart any of the Drillbits when you add or update a storage plugin instance.

Use the Drill Web UI to update or add a new storage plugin. Launch a web browser, go to: `http://<IP address or host name>:8047`, and then go to the Storage tab. 

To create and configure a new storage plugin:

1. Enter a storage name in New Storage Plugin.
   Each storage plugin registered with Drill must have a distinct
name. Names are case-sensitive.
2. Click Create.  
3. In Configuration, configure attributes of the storage plugin, if applicable, using JSON formatting. The Storage Plugin Attributes table in the next section describes attributes typically reconfigured by users. 
4. Click Create.

Click Update to reconfigure an existing, enabled storage plugin.

## Storage Plugin Attributes
The following diagram of the dfs storage plugin briefly describes options you configure in a typical storage plugin configuration:

![dfs plugin]({{ site.baseurl }}/docs/img/connect-plugin.png)

The following table describes the attributes you configure for storage plugins in more detail than the diagram. 

<table>
  <tr>
    <th>Attribute</th>
    <th>Example Values</th>
    <th>Required</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>"type"</td>
    <td>"file"<br>"hbase"<br>"hive"<br>"mongo"</td>
    <td>yes</td>
    <td>The storage plugin type name supported by Drill.</td>
  </tr>
  <tr>
    <td>"enabled"</td>
    <td>true<br>false</td>
    <td>yes</td>
    <td>The state of the storage plugin.</td>
  </tr>
  <tr>
    <td>"connection"</td>
    <td>"classpath:///"<br>"file:///"<br>"mongodb://localhost:27017/"<br>"maprfs:///"</td>
    <td>implementation-dependent</td>
    <td>The type of distributed file system. Drill can work with any distributed system, such as HDFS and S3, or files in your file system.</td>
  </tr>
  <tr>
    <td>"workspaces"</td>
    <td>null<br>"logs"</td>
    <td>no</td>
    <td>One or more unique workspace names, enclosed in double quotation marks. If a workspace is defined more than once, the latest one overrides the previous ones. Not used with local or distributed file systems.</td>
  </tr>
  <tr>
    <td>"workspaces". . . "location"</td>
    <td>"location": "/"<br>"location": "/tmp"</td>
    <td>no</td>
    <td>The path to a directory on the file system.</td>
  </tr>
  <tr>
    <td>"workspaces". . . "writable"</td>
    <td>true<br>false</td>
    <td>no</td>
    <td>One or more unique workspace names, enclosed in double quotation marks. If a workspace is defined more than once, the latest one overrides the previous ones. Not used with local or distributed file systems.</td>
  </tr>
  <tr>
    <td>"workspaces". . . "defaultInputFormat"</td>
    <td>null<br>"parquet"<br>"csv"<br>"json"</td>
    <td>no</td>
    <td>The format of data Drill reads by default, regardless of extension. Parquet is the default.</td>
  </tr>
  <tr>
    <td>"formats"</td>
    <td>"psv"<br>"csv"<br>"tsv"<br>"parquet"<br>"json"<br>"avro"<br>"maprdb"*</td>
    <td>yes</td>
    <td>One or more file formats of data Drill can read. Drill can implicitly detect some file formats based on the file extension or the first few bits of data within the file, but you need to configure an option for others.</td>
  </tr>
  <tr>
    <td>"formats" . . . "type"</td>
    <td>"text"<br>"parquet"<br>"json"<br>"maprdb"</td>
    <td>yes</td>
    <td>The type of the format specified. For example, you can define two formats, csv and psv, as type "Text", but having different delimiters. Drill enables the maprdb plugin if you define the maprdb type.</td>
  </tr>
  <tr>
    <td>formats . . . "extensions"</td>
    <td>["csv"]</td>
    <td>format-dependent</td>
    <td>The extensions of the files that Drill can read.</td>
  </tr>
  <tr>
    <td>"formats" . . . "delimiter"</td>
    <td>"\t"<br>","</td>
    <td>format-dependent</td>
    <td>The delimiter used to separate columns in text files such as CSV. Specify a non-printable delimiter in the storage plugin config by using the form \uXXXX, where XXXX is the four numeral hex ascii code for the character.</td>
  </tr>
</table>

\* Only appears when you install Drill on a cluster using the mapr-drill package.

The configuration of other attributes, such as `size.calculator.enabled` in the hbase plugin and `configProps` in the hive plugin, are implementation-dependent and beyond the scope of this document.

Although Drill can work with different file types in the same directory, restricting a Drill workspace to one file type prevents confusion.

## Case-sensitive Names
As previously mentioned, workspace and storage plugin names are case-sensitive. For example, the following query uses a storage plugin name `dfs` and a workspace name `clicks`. When you refer to `dfs.clicks` in an SQL statement, use the defined case:

    0: jdbc:drill:> USE dfs.clicks;

For example, using uppercase letters in the query after defining the storage plugin and workspace names using lowercase letters does not work. 

## REST API

Drill provides a REST API that you can use to create a storage plugin. Use an HTTP POST and pass two properties:

* name
  The plugin name. 

* config
  The storage plugin definition as you would enter it in the Web UI.

For example, this command creates a plugin named myplugin for reading files of an unknown type located on the root of the file system:

    curl -X POST -/json" -d '{"name":"myplugin", "config": {"type": "file", "enabled": false, "connection": "file:///", "workspaces": { "root": { "location": "/", "writable": false, "defaultInputFormat": null}}, "formats": null}}' http://localhost:8047/storage/myplugin.json

## Bootstrapping a Storage Plugin
If you need to add a storage plugin to Drill and do not want to use a web browser, you can create a [bootstrap-storage-plugins.json](https://github.com/apache/drill/blob/master/contrib/storage-hbase/src/main/resources/bootstrap-storage-plugins.json) file and include it on the classpath when starting Drill. The storage plugin loads when Drill starts up.

If you configure an HBase storage plugin using bootstrap-storage-plugins.json file and HBase is not install, you might experience a delay when executing the queries. Configure the [HBase client timeout](http://hbase.apache.org/book.html#config.files) and retry settings in the config block of HBase plugin instance configuration.