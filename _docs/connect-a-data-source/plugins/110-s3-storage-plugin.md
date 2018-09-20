---
title: "S3 Storage Plugin"
date: 2018-06-21 23:39:47 UTC
parent: "Connect a Data Source"
---
Drill works with data stored in the cloud. With a few simple steps, you can configure the S3 storage plugin for Drill and be off to the races running queries. 

Drill has the ability to query files stored on Amazon's S3 cloud storage using the HDFS s3a library. The HDFS s3a library adds support for files larger than 5 gigabytes (these were unsupported using the older HDFS s3n library).

To connect Drill to S3:  


- Provide your AWS credentials.   
- Configure the S3 storage plugin with an S3 bucket name.  

For additional information, refer to the [HDFS S3 documentation](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html).   

**Note:** Drill does not use HDFS 3.x, therefore Drill does not support AWS temporary credentials, as described in the s3a documentation.


## Providing AWS Credentials  

Your environment determines where you provide your AWS credentials. You can define your AWS credentials one of three ways:  

- Directly in the S3 storage plugin. Note that this method is the least secure, but sufficient for use on a single machine, such as a laptop.  
- In a non-Hadoop environment, you can use the Drill-specific core-site.xml file to provide the AWS credentials.  
- In a Hadoop environment, you can use the existing S3 configuration for Hadoop. The AWS credentials should already be defined. All you need to do is [configure the S3 storage plugin]({{site.baseurl}}/docs/s3-storage-plugin/#configuring-the-s3-storage-plugin).  

### Defining Access Keys in the S3 Storage Plugin  

Refer to [Configuring the S3 Storage Plugin]({{site.baseurl}}/docs/s3-storage-plugin/#configuring-the-s3-storage-plugin). 

### Defining Access Keys in the Drill core-site.xml File

To configure the access keys in Drill's core-site.xml file, navigate to the `$DRILL_HOME/conf` or `$DRILL_SITE` directory, and rename the core-site-example.xml file to core-site.xml. Replace the text `ENTER_YOUR_ACESSKEY` and `ENTER_YOUR_SECRETKEY` with your AWS credentials and also include the endpoint, as shown in the following example:   

       <configuration>
           <property>
               <name>fs.s3a.access.key</name>
               <value>ACCESS-KEY</value>
           </property>
           <property>
               <name>fs.s3a.secret.key</name>
               <value>SECRET-KEY</value>
           </property>
           <property>
               <name>fs.s3a.endpoint</name>
               <value>s3.REGION.amazonaws.com</value>
           </property>
       </configuration>  

### Configuring Drill to use AWS IAM Roles for Accessing S3

If you use IAM roles/Instance profiles, to access data in s3, use the following settings in your core-site.xml. Do not specify the secret key or access key properties. As an example:

       <configuration>
		<property>
		    <name>fs.s3a.aws.credentials.provider</name>
		    <value>com.amazonaws.auth.InstanceProfileCredentialsProvider</value>
		</property>
       </configuration>            

**Note:** When you rename the file, Hadoop support breaks if `$HADOOP_HOME` was in the path because Drill pulls in the Drill core-site.xml file instead of the Hadoop core-site.xml file. In this situation, make the changes in the Hadoop core-site.xml file. Do not create a core-site.xml file for Drill.  

## Configuring the S3 Storage Plugin

The Storage page in the Drill Web UI provides an S3 storage plugin that you configure to connect Drill to the S3 distributed file system registered in core-site.xml. If you did not define your AWS credentials in the core-site.xml file, you can define them in the storage plugin configuration.   

To configure the S3 storage plugin, log in to the Drill Web UI and then update the S3 configuration with the bucket name, as described in the following steps:   

1\. To access the Drill Web UI, enter the following URL in the address bar of your web browser:  

       http://<drill-hostname>:8047  
  
       //The drill-hostname is a node on which Drill is running.  

2\. To configure the S3 storage plugin in Drill, complete the following steps:  

   a\. Click on the **Storage** page.  
   b\. Find the S3 option on the page and then click **Update** next to the option.  
   c\. Configure the S3 storage plugin, specifying the bucket in the `"connection"` property, as shown in the following example:  

**Note:** The `"config"` block in the following S3 storage plugin configuration contains the access key and endpoint properties required if you want to define your AWS credentials here. Do not include the `"config"` block in your S3 storage plugin configuration if you defined your AWS credentials in the core-site.xml file.   

       {
	"type": "file",
	"enabled": true,
	"connection": "s3a://bucket-name/",
	"config": {
		"fs.s3a.access.key": "<key>",
		"fs.s3a.secret.key": "<key>",
		"fs.s3a.endpoint": "s3.us-west-1.amazonaws.com"
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
	},
	"formats": {
		"psv": {
			"type": "text",
			"extensions": [
				"tbl"
			],
			"delimiter": "|"
		},
		"csv": {
			"type": "text",
			"extensions": [
				"csv"
			],
			"delimiter": ","
		    }
	    }
    }
          
         
4-Click **Update** to save the configuration.  
5-Navigate back to the **Storage** page.  
6-On the **Storage** page, click **Enable** next to the S3 option.  
	
Drill should now be able to use the HDFS s3a library to access data in S3.


## Quering Parquet Format Files On S3 

Drill uses the Hadoop distributed file system (HDFS) for reading S3 input files, which ultimately uses the Apache HttpClient. The HttpClient has a default limit of four simultaneous requests, and it puts the subsequent S3 requests in the queue. A Drill query with large number of columns or a Select * query, on Parquet formatted files ends up issuing many S3 requests and can fail with ConnectionPoolTimeoutException.   

Fortunately, as a part of S3a implementation in Hadoop 2.7.1, HttpClient's required limit parameter is extracted out in a config and can be raised to avoid ConnectionPoolTimeoutException. This is how you can set this parameter in core-site.xml:


       <configuration>
         ...
         
         <property>
           <name>fs.s3a.connection.maximum</name>
           <value>100</value>
         </property>
       
       </configuration>

