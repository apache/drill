---
title: "Plugin Configuration Basics"
date: 2019-04-09
parent: "Storage Plugin Configuration"
---
There are several ways you can configure storage plugins. For example, you can configure storage plugins in the Drill Web UI,  using REST API, or through configuration files. See [Configuring Storage Plugins]({{site.baseurl}}/docs/configuring-storage-plugins/) for more information.

When you configure storage plugins, you use a set of storage plugin attributes, such as the storage plugin type, formats that the plugin type supports, and connection parameters.   

The following sections describe the attributes that you can use in your storage plugin configurations and provide information related to the use of attributes. 
  

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
    <td>Allows or disallows writing in the workspaces</td>
  </tr>
  <tr>
    <td>"workspaces". . . "defaultInputFormat"</td>
    <td>null<br>"parquet"<br>"csv"<br>"json"</td>
    <td>no</td>
    <td>Format for reading data, regardless of extension. Default = "parquet"</td>
  </tr>
  <tr>
    <td>"workspaces". . . "allowAccessOutsideWorkspace"</td>
    <td>false<br>true<br></td>
    <td>yes</td>
    <td>Introduced in Drill 1.12. Prevents users from accessing paths outside the root of a workspace. Set to false by default to disallow access outside the root of a workspace. To allow access to paths outside the root of a workspace, change the value to true. Dfs storage plugins configured prior to Drill 1.12 (that do not have the parameter specified) cannot access paths outside of the workspace unless this parameter is included in the workspace configuration and set to true.</td>
  </tr>
  <tr>
    <td>"formats"</td>
    <td>"pcap"<br><a href="https://pcapng.github.io/pcapng/">"pcapng"</a><br>"psv"<br>"csv"<br>"tsv"<br>"parquet"<br>"json"<br>"avro"<br>"maprdb"<br>"image"<br>"sequencefile"<br>"httpd"</td>
    <td>yes</td>
    <td>One or more valid file formats for reading. Drill detects formats of some files; others require configuration. The maprdb format is in installations of the mapr-drill package.  </td>
  </tr>
  <tr>
    <td>"formats" . . . "type"</td>
    <td>"pcap"<br>"pcapng"<br>"text"<br>"parquet"<br>"json"<br>"maprdb"<br>"avro"<br>"image"<br>"sequencefile"<br>"httpd"<br>"[syslog]({{site.baseurl}}/docs/sys-log-format-plugin/)"</td>
    <td>yes</td>
    <td>Format type. You can define two formats, csv and psv, as type "Text", but having different delimiters. </td>
  </tr>
  <tr>
    <td>formats . . . "extensions"</td>
    <td>["csv"]<br><a href="https://drill.apache.org/docs/logfile-plugin/">["log"]</a><br></td>
    <td>format-dependent</td>
    <td>File name extensions that Drill can read.</td>
  </tr>
  <tr>
    <td>"formats" . . . "delimiter"</td>
    <td>"\n"<br>"\r"<br>"\t"<br>"\r\n"<br>","</td>
    <td>format-dependent</td>
    <td>Sequence of one or more characters that signifies the end of a line of text and the start of a new line in a delimited text file, such as CSV. Drill treats \n as the standard line delimiter. As of Drill 1.8, Drill supports multi-byte delimiters, such as \r\n. Use a 4-digit hex code syntax \uXXXX for a non-printable delimiter. </td>
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
    <tr>
    <td>"formats" . . . "extractHeader"</td>
    <td>true</td>
    <td>no</td>
    <td>Set to true to extract and use headers as column names when reading a delimited text file, false otherwise. Ensure skipFirstLine is not true when extractHeader=false.
    </td>
  </tr>
</table>

## Using the Formats Attributes

You set the formats attributes, such as skipFirstLine, in the `formats` area of the storage plugin configuration. When setting attributes for text files, such as CSV, you also need to set the `sys.options` property `exec.storage.enable_new_text_reader` to true (the default). For more information and examples of using formats for text files, see ["Text Files: CSV, TSV, PSV"]({{site.baseurl}}{{site.baseurl}}/docs/text-files-csv-tsv-psv/).  

## Using the Formats Attributes as Table Function Parameters

In Drill version 1.4 and later, you can also set the formats attributes defined above on a per query basis. To pass parameters to the format plugin, use the table function syntax:  

`select a, b from table({table function name}(parameters))`

The `table function name` is the table name, the type parameter is the format name, and the other parameters are the fields that the format plugin configuration accepts, as defined in the table above (except for `extensions` which do not apply in this context).

For example, to read a CSV file and parse the header:  
``select a, b from table(dfs.`path/to/data.csv`(type => 'text',
fieldDelimiter => ',', extractHeader => true))``

For more information about format plugin configuration see ["Text Files: CSV, TSV, PSV"]({{site.baseurl}}{{site.baseurl}}/docs/text-files-csv-tsv-psv/).  

## Using Other Attributes

The configuration of other attributes, such as `size.calculator.enabled` in the `hbase` plugin and `configProps` in the `hive` plugin, are implementation-dependent and beyond the scope of this document.

## Case-Sensitivity  

Starting in Drill 1.15, storage plugin names and workspaces (schemas) are case-insensitive. For example, the following query uses a storage plugin named `dfs` and a workspace named `clicks`. You can reference `dfs.clicks` in an SQL statement in uppercase or lowercase, as shown:

       USE dfs.clicks;  
       USE DFS.CLICKs;
       USE dfs.CLICKS; 

Refer to [Case-Sensitivity]({{site.baseurl}}/docs/lexical-structure/#case-sensitivity) for more information about case-sensitivity in Drill.
  






 
 