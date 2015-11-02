---
title: "Plugin Configuration Basics"
parent: "Storage Plugin Configuration"
---
When you add or update storage plugin configurations on one Drill node in a 
cluster having multiple installations of Drill, Drill broadcasts the information to other Drill nodes 
to synchronize the storage plugin configurations. You do not need to
restart any of the Drillbits when you add or update a storage plugin configuration.

## Using the Drill Web Console

You can use the Drill Web Console to update or add a new storage plugin configuration. The Drill shell needs to be running to start the Web Console. 

To create a name and new configuration:

1. [Start the Drill shell]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/).  
2. [Start the Web Console]({{site.baseurl}}/docs/starting-the-web-console/).  
3. On the Storage tab, enter a name in **New Storage Plugin**.
   Each configuration registered with Drill must have a distinct
name. Names are case-sensitive.  
     ![sandbox plugin]({{ site.baseurl }}/docs/img/storage_plugin_config.png)

    {% include startnote.html %}The URL differs depending on your installation and configuration.{% include endnote.html %}  
4. Click **Create**.  
5. In Configuration, use JSON formatting to modify a copy of an existing configuration if possible.  
   Using a copy of an existing configuration reduces the risk of JSON coding errors. Use the Storage Plugin Attributes table in the next section as a guide for making typical modifications.  
6. Click **Create**.

<!-- Add to step 3 when the feature goes into 1.3: The Storage tab appears on the Web Console if you are [authorized]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/) to view, update, or add storage plugins.   -->

## Storage Plugin Attributes
The following graphic shows key attributes of a typical `dfs`-based storage plugin configuration:  
![dfs plugin]({{ site.baseurl }}/docs/img/connect-plugin.png)
## List of Attributes and Definitions
The following table describes the attributes you configure for storage plugins installed with Drill. 
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
    <td>A valid storage plugin type name.</td>
  </tr>
  <tr>
    <td>"enabled"</td>
    <td>true<br>false</td>
    <td>yes</td>
    <td>State of the storage plugin.</td>
  </tr>
  <tr>
    <td>"connection"</td>
    <td>"classpath:///"<br>"file:///"<br>"mongodb://localhost:27017/"<br>"hdfs://"</td>
    <td>implementation-dependent</td>
    <td>The type of distributed file system, such as HDFS, Amazon S3, or files in your file system, and an address/path name.</td>
  </tr>
  <tr>
    <td>"workspaces"</td>
    <td>null<br>"logs"</td>
    <td>no</td>
    <td>One or more unique workspace names. If a workspace name is used more than once, only the last definition is effective. </td>
  </tr>
  <tr>
    <td>"workspaces". . . "location"</td>
    <td>"location": "/Users/johndoe/mydata"<br>"location": "/tmp"</td>
    <td>no</td>
    <td>Full path to a directory on the file system.</td>
  </tr>
  <tr>
    <td>"workspaces". . . "writable"</td>
    <td>true<br>false</td>
    <td>no</td>
    <td>One or more unique workspace names. If defined more than once, the last workspace name overrides the others.</td>
  </tr>
  <tr>
    <td>"workspaces". . . "defaultInputFormat"</td>
    <td>null<br>"parquet"<br>"csv"<br>"json"</td>
    <td>no</td>
    <td>Format for reading data, regardless of extension. Default = "parquet"</td>
  </tr>
  <tr>
    <td>"formats"</td>
    <td>"psv"<br>"csv"<br>"tsv"<br>"parquet"<br>"json"<br>"avro"<br>"maprdb" *</td>
    <td>yes</td>
    <td>One or more valid file formats for reading. Drill implicitly detects formats of some files based on extension or bits of data in the file; others require configuration.</td>
  </tr>
  <tr>
    <td>"formats" . . . "type"</td>
    <td>"text"<br>"parquet"<br>"json"<br>"maprdb" *</td>
    <td>yes</td>
    <td>Format type. You can define two formats, csv and psv, as type "Text", but having different delimiters. </td>
  </tr>
  <tr>
    <td>formats . . . "extensions"</td>
    <td>["csv"]</td>
    <td>format-dependent</td>
    <td>File name extensions that Drill can read.</td>
  </tr>
  <tr>
    <td>"formats" . . . "delimiter"</td>
    <td>"\t"<br>","</td>
    <td>format-dependent</td>
    <td>Sequence of one or more characters that serve as a record separator in a delimited text file, such as CSV. Use a 4-digit hex code syntax \uXXXX for a non-printable delimiter. </td>
  </tr>
  <tr>
    <td>"formats" . . . "quote"</td>
    <td>"""</td>
    <td>no</td>
    <td>A single character that starts/ends a value in a delimited text file.</td>
  </tr>
  <tr>
    <td>"formats" . . . "escape"</td>
    <td>"`"</td>
    <td>no</td>
    <td>A single character that escapes a quotation mark inside a value.</td>
  </tr>
  <tr>
    <td>"formats" . . . "comment"</td>
    <td>"#"</td>
    <td>no</td>
    <td>The line decoration that starts a comment line in the delimited text file.</td>
  </tr>
  <tr>
    <td>"formats" . . . "skipFirstLine"</td>
    <td>true</td>
    <td>no</td>
    <td>To include or omit the header when reading a delimited text file. Set to true to avoid reading headers as data.
    </td>
  </tr>
</table>

\* Pertains only to distributed Drill installations using the mapr-drill package.  

## Using the Formats Attributes

You set the formats attributes, such as skipFirstLine, in the `formats` area of the storage plugin configuration. When setting attributes for text files, such as CSV, you also need to set the `sys.options` property `exec.storage.enable_new_text_reader` to true (the default). For more information and examples of using formats for text files, see ["Text Files: CSV, TSV, PSV"]({{site.baseurl}}{{site.baseurl}}/docs/text-files-csv-tsv-psv/).

## Using Other Attributes

The configuration of other attributes, such as `size.calculator.enabled` in the `hbase` plugin and `configProps` in the `hive` plugin, are implementation-dependent and beyond the scope of this document.

## Case-sensitive Names
As previously mentioned, workspace and storage plugin names are case-sensitive. For example, the following query uses a storage plugin name `dfs` and a workspace name `clicks`. When you refer to `dfs.clicks` in an SQL statement, use the defined case:

    0: jdbc:drill:> USE dfs.clicks;

For example, using uppercase letters in the query after defining the storage plugin and workspace names using lowercase letters does not work. 

## Storage Plugin REST API

If you need to add a storage plugin configuration to Drill and do not want to use a web browser, Drill provides a REST API that you can use to create a storage plugin configuration. Use a POST and pass two properties:

* name  
  The storage plugin configuration name. 

* config  
  The attribute settings as entered in the Web Console.

For example, this command creates a storage plugin named myplugin for reading files of an unknown type located on the root of the file system:

    curl -X POST -H "Content-Type: application/json" -d '{"name":"myplugin", "config": {"type": "file", "enabled": false, "connection": "file:///", "workspaces": { "root": { "location": "/", "writable": false, "defaultInputFormat": null}}, "formats": null}}' https://localhost:8047/storage/myplugin.json

For more information about the REST API, see this [Google doc](https://docs.google.com/document/d/1mRsuWk4Dpt6ts-jQ6ke3bB30PIwanRiCPfGxRwZEQME).

## Bootstrapping a Storage Plugin

The REST API is recommended for programmatically adding a storage plugin configuration to Drill. An alternative for use in a distributed environment only is bootstrapping. You can create a [bootstrap-storage-plugins.json](https://github.com/apache/drill/blob/master/contrib/storage-hbase/src/main/resources/bootstrap-storage-plugins.json) file and include it on the classpath when starting Drill. The storage plugin configuration loads when Drill starts up.

Currently, bootstrapping a storage plugin configuration works only when the first Drillbit in the cluster first starts up. The configuration is
stored in ZooKeeper, preventing Drill from picking up the bootstrap-storage-plugins.json again.

After cluster startup, you have to use the REST API or Drill Web Console to add a storage plugin configuration. Alternatively, you
can modify the entry in ZooKeeper by uploading the json file for
that plugin to the /drill directory of the zookeeper installation, or by just deleting the /drill directory if you do not have configuration properties to preserve.

If you load an HBase storage plugin configuration using bootstrap-storage-plugins.json file and HBase is not installed, you might experience a delay when executing the queries. Configure the [HBase client timeout](http://hbase.apache.org/book.html#config.files) and retry settings in the config block of the HBase plugin configuration.
