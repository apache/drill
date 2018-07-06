---
title: "Configuring Storage Plugins"
date: 2018-07-06 02:00:49 UTC
parent: "Storage Plugin Configuration"
---  

Storage plugins enable Drill to access data sources. Drill provides a default set of storage plugin configurations upon installation. However, you can modify the storage plugin configurations, as  described in following sections. When you modify storage plugin configurations on one Drill node in a Drill cluster, Drill broadcasts the information to the other Drill nodes to synchronize the storage plugin configurations across all of the Drill nodes.  

Starting in Drill 1.14, you can use the `storage-plugins-override.conf` file to store your custom storage plugin configurations, as described in the Storage Plugins Configurations File topic below. 

Once you decide how you want to configure storage plugins in Drill, you may also want to reference [Plugin Configuration Basics]({{site.baseurl}}/docs/plugin-configuration-basics/) for configuration details, such as supported data sources and file formats. 

The following sections describe each of these storage plugin configuration methods in more detail.  

## Configuring Storage Plugins in the Drill Web UI  

You can use the Drill Web UI to update or add a new storage plugin configuration. The Drill shell needs to be running to start the Web UI. 

To create a name and new configuration:

1. [Start the Drill shell]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/).
2. [Start the Web Console]({{site.baseurl}}/docs/starting-the-web-console/). The Storage tab appears in the Web Console if you are [authorized]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/) to view, update, or add storage plugins. 
3. On the Storage tab, enter a name in **New Storage Plugin**.
   Each configuration registered with Drill must have a distinct
name. Names are case-sensitive.  
     ![sandbox plugin]({{ site.baseurl }}/docs/img/storage_plugin_config.png)

    {% include startnote.html %}The URL differs depending on your installation and configuration.{% include endnote.html %}  
4. Click **Create**.  
5. In Configuration, use JSON formatting to modify a copy of an existing configuration if possible.  
   Using a copy of an existing configuration reduces the risk of JSON coding errors. Use the Storage Plugin Attributes table in the next section as a guide for making typical modifications.  
6. Click **Create**.  

**Note:** Drill 1.12 and later provides syntax highlighting and error checking for plugin configurations to prevent syntax errors, such as missing commas or curly brackets, when configuring storage plugins. 
 
You can see an example of syntax highlighting for a storage plugin configuration in the following image:  

![](https://i.imgur.com/LdiQC7E.png)  

A red box with an x indicates that the syntax contains an error, as shown in the following image:  

![](https://i.imgur.com/cFDCH0v.png) 
    


## Configuring Storage Plugins Using REST API  

If you need to add a storage plugin configuration to Drill and do not want to use a web browser, you can use the [Drill REST API]({{site.baseurl}}/docs/rest-api/#get-status-threads) to create a storage plugin configuration. Use a POST request and pass the following two properties:

* name  
  The storage plugin configuration name. 

* config  
  The attribute settings as entered in the Web Console.

For example, this command creates a storage plugin named myplugin for reading files of an unknown type located on the root of the file system:

    curl -X POST -H "Content-Type: application/json" -d '{"name":"myplugin", "config": {"type": "file", "enabled": false, "connection": "file:///", "workspaces": { "root": { "location": "/", "writable": false, "defaultInputFormat": null}}, "formats": null}}' http://localhost:8047/storage/myplugin.json

This example assumes HTTPS has not been enabled. 

## Configuring Storage Plugins with a Bootstrap File  

You can create a [``bootstrap-storage-plugins.json``](https://github.com/apache/drill/blob/master/contrib/storage-hbase/src/main/resources/bootstrap-storage-plugins.json) file and include the file in the classpath when you start Drill. Drill loads the storage plugin configuration from the bootstrap file into the [PStore]({{site.baseurl}}/docs/persistent-configuration-storage/) (persistent configuration storage) when Drill starts; ZooKeeper is the default PStore for Drill in distributed mode. You can then use the REST API, Drill Web UI, or `storage-plugins-override.conf` file to modify the storage plugins configurations.

Bootstrapping a storage plugin configuration only works for the first Drillbit in the cluster that starts. Drill cannot pick up the configuration from the `bootstrap-storage-plugins.json` file during a restart; however, this can be achieved with a `storage-plugins-override.conf` file. 

Alternatively, you can modify the entry in ZooKeeper directly, by uploading the configuration for a plugin into the `/drill` directory of the ZooKeeper installation, or by deleting the `/drill` directory (if you do not have configuration properties to preserve).

**Note:** If you load an HBase storage plugin configuration using the `bootstrap-storage-plugins.json` file and HBase is not installed, you might experience a delay when executing the queries. Configure the [HBase client timeout](http://hbase.apache.org/book.html/#config.files) and retry settings in the `config` block of the HBase plugin configuration.  

## Configuring Storage Plugins with the storage-plugins-override.conf File  

Starting in Drill 1.14, you can manage storage plugin configurations in the Drill configuration file, `storage-plugins-override.conf`, located in the `<drill-installation>/conf` directory. When you provide the storage plugin configurations in the `storage-plugins-override.conf` file, Drill reads the file and configures the plugins during start-up. 

For the configurations in the `storage-plugins-override.conf` file to take effect, you must restart Drill. After applying the new storage plugins configurations, the `storage-plugins-override.conf` file remains in the `/conf` directory. Drill will apply the configurations again, after the next Drill restart.  

The `drill.exec.storage.action_on_plugins_override_file` [start-up option]({{site.baseurl}}/docs/start-up-options/#configuring-start-up-options) in the `<drill_home>/conf/drill-override.conf` file determines what happens to the file after Drill successfully updates storage plugin configurations. If you do not want Drill to apply the configurations after restarting, set the option to `“rename”` or `“remove”`.  

You can set the `drill.exec.storage.action_on_plugins_override_file` option to one of the following values:  

- `"none"`  
(Default) The file remains in the directory after Drill uses the file.  
- `"rename"`  
The `storage-plugins-override.conf` file name is changed to `storage-plugins-override-[current_timestamp].conf` after Drill uses the file.  
- `"remove"`  
The file is removed after Drill uses the file for the first time.
  


