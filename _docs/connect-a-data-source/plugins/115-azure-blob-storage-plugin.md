---
title: "Azure Blob Storage Plugin"
date: 2018-11-02
parent: "Connect a Data Source"
---
Drill works well with Azure Blob Storage thanks to the Hadoop compatible layer that exists and make Azure Blob Storage usable by any tool that supports HDFS, just like Apache Drill.

## Install Azure Jars

The first step is to download the jars from Maven. The ones the works with the current version of Drill are the followin:

* [hadoop-azure-2.7.7.jar](http://central.maven.org/maven2/org/apache/hadoop/hadoop-azure/2.7.7/hadoop-azure-2.7.7.jar)
* [azure-storage-8.0.0.jar](http://central.maven.org/maven2/com/microsoft/azure/azure-storage/8.0.0/azure-storage-8.0.0.jar)

The first one is the HDFS wrapper around Azure Blob Storage and the second provides access Azure Blob Storage from Java. Download them jars and save them into `$DRILL_HOME/jars/3rdparty` folder.

## Providing Azure Blob Storage Credentials  

Your environment determines where you provide your Azure Blob Storage credentials. You can define your Azure Blob Storage credentials one of three ways:  

- Directly in the Azure Blob Storage storage plugin. Note that this method is the least secure, but sufficient for use on a single machine, such as a laptop.  
- In a non-Hadoop environment, you can use the Drill-specific `core-site.xml` file to provide the Azure Blob Storage credentials.  
- In a Hadoop environment, you can use the existing Azure Blob Storage configuration for Hadoop. The Azure Blob Storage credentials should already be defined. All you need to do is [configure the Azure Blob Storage storage plugin]({{site.baseurl}}/docs/azure-blob-storage-plugin/#configuring-the-azure-blob-storage-plugin).  

### Defining Access Keys in the Azure Blob Storage Plugin  

Refer to [Configuring the Azure Blob Storage Plugin]({{site.baseurl}}/docs/azure-blob-storage-plugin/#configuring-the-azure-blob-storage-plugin). 

### Defining Access Keys in the Drill core-site.xml File

In order to configure Drill to access the Azure Blob Storage that contains that data you want to query with Drill, the authentication key must be provided. To get the authentication key you can use AZ CLI:

	az storage account keys list -g <resource-group> -n <storage-account-name>

pick the primary or secondary key and put it in the `site-conf.xml` file that you can find in `$DRILL_HOME/conf` or `$DRILL_SITE` folder. If it doesn't exists already, go on and create it (you may also just copy `core-site-example.xml` file to `core-site.xml` and start from there):

	<?xml version="1.0" encoding="UTF-8" ?>
	<configuration>
		<property>
			<name>fs.azure.account.key.STORAGE_ACCOUNT_NAME.blob.core.windows.net</name>
			<value>AUTHENTICATION_KEY</value>
		</property>
	</configuration>

**Note:** When you rename the file, Hadoop support breaks if `$HADOOP_HOME` was in the path because Drill pulls in the Drill core-site.xml file instead of the Hadoop core-site.xml file. In this situation, make the changes in the Hadoop core-site.xml file. Do not create a core-site.xml file for Drill.  

## Configuring the Azure Blob Storage Plugin

The Storage page in the Drill Web UI provides an Azure Blob Storage plugin that you configure to connect Drill to the Azure Blob Storage file system registered in `core-site.xml`. If you did not define your Azure Blob Storage credentials in the `core-site.xml` file, you can define them in the storage plugin configuration.   

To configure the Azure Blob Storage plugin, log in to the Drill Web UI and then update the Azure Blob Storage configuration with the bucket name, as described in the following steps:   

1\. To access the Drill Web UI, enter the following URL in the address bar of your web browser:  

       http://<drill-hostname>:8047  
  
       //The drill-hostname is a node on which Drill is running.  

2\. To configure the Azure Blob Storage plugin in Drill, complete the following steps:  

   a\. Click on the **Storage** page.  
   b\. Find the CP option on the page and then click **Update** next to the option.  
   c\. Copy the entire content in the clipboard and the go **Back**.
   d\. At the bottom of the page, "New storage Plugin" section, type `AZ` in the textbox and click on **Create**.
   e\. Paste the text copied from the CP plugin.
   f\. Configure the Azure Blob Storage plugin, specifying the container you want to access to in the `"connection"` property, as shown in the following example:  

**Note:** The `"config"` block in the following Azure Blob Storage plugin configuration contains the access key and endpoint properties required if you want to define your Azure Blob Storage credentials here. *Do not include* the `"config"` block in your Azure Blob Storage plugin configuration if you defined your Azure Blob Storage credentials in the `core-site.xml` file.   
    
	"type": "file",
	"enabled": true,
	"connection": "wasbs://CONTAINER@STORAGE_ACCOUNT_NAME.blob.core.windows.net",
	"config": {
		"fs.azure.account.key.STORAGE_ACCOUNT_NAME.blob.core.windows.net": "AUTHENTICATION_KEY"
	},
	"workspaces": {
		"root": {
			"location": "/user/robot/drill",
			"writable": true,
			"defaultInputFormat": null
		},
		"tmp": {
			"location": "/tmp",
			"writable": true,
			"defaultInputFormat": null
		}
	}
                   
4-Click **Update** to save the configuration.  
5-Navigate back to the **Storage** page.  
6-On the **Storage** page, the newly create AZ option should be automatically enabled.  
	
Drill should now be able to use access data in your Azure Blob Storage container and query it.

https://vimeo.com/286972298 