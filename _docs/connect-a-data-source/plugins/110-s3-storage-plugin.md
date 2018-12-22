---
title: "S3 Storage Plugin"
date: 2018-12-22
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

Your environment determines where you provide your AWS credentials. You can use the following methods to define your AWS credentials:  

- In the S3 storage plugin configuration:
	- [You can point to an encrypted file in an external provider.]({{site.baseurl}}/docs/s3-storage-plugin/#using-an-external-provider-for-credentials) (Drill 1.15 and later) 
	- [You can put your access and secret keys directly in the storage plugin configuration.]({{site.baseurl}}/docs/s3-storage-plugin/#adding-credentials-directly-to-the-s3-plugin) Note that this method is the least secure, but sufficient for use on a single machine, such as a laptop.
- In a non-Hadoop environment, you can use the Drill-specific core-site.xml file to provide the AWS credentials.    

### Defining Access Keys in the S3 Storage Plugin  

Refer to [Configuring the S3 Storage Plugin]({{site.baseurl}}/docs/s3-storage-plugin/#configuring-the-s3-storage-plugin). 

### Defining Access Keys in the Drill core-site.xml File

To configure the access keys in Drill's core-site.xml file, navigate to the `$DRILL_HOME/conf` or `$DRILL_SITE` directory, and rename the `core-site-example.xml` file to `core-site.xml`. Replace the text `ENTER_YOUR_ACESSKEY` and `ENTER_YOUR_SECRETKEY` with your AWS credentials and also include the endpoint, as shown in the following example:   

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

If you use IAM roles/Instance profiles, to access data in s3, use the following settings in your core-site.xml. Do not specify the secret key or access key properties. For example:

       <configuration>
		<property>
		    <name>fs.s3a.aws.credentials.provider</name>
		    <value>com.amazonaws.auth.InstanceProfileCredentialsProvider</value>
		</property>
       </configuration>            

**Note:** When you rename the file, Hadoop support breaks if `$HADOOP_HOME` was in the path because Drill pulls in the Drill core-site.xml file instead of the Hadoop core-site.xml file. In this situation, make the changes in the Hadoop core-site.xml file. Do not create a core-site.xml file for Drill.  

##Configuring the S3 Storage Plugin

The Storage page in the Drill Web UI provides an S3 storage plugin that you configure to connect Drill to the S3 distributed file system registered in core-site.xml. If you did not define your AWS credentials in the core-site.xml file, you can define them in the storage plugin configuration. You can define the credentials directly in the configuration, or you can use an external provider. 

To configure the S3 storage plugin, log in to the Drill Web UI at `http://<drill-hostname>:8047`. The drill-hostname is a node on which Drill is running. Go to the **Storage** page and click **Update** next to the S3 storage plugin option. Edit the configuration and then click **Update** to save the configuration.  

**Note:** The `"config"` block in the S3 storage plugin configuration contains contains properties to define your AWS credentials. Do not include the `"config"` block in your S3 storage plugin configuration if you defined your AWS credentials in the core-site.xml file. 

Use either of the following methods to provide your credentials:

### Using an External Provider for Credentials
Starting in Drill 1.15, the S3 storage plugin supports the [Hadoop Credential Provider API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html]), which allows you to store secret keys and other sensitive data in an encrypted file in an external provider versus storing them in plain text in a configuration file or storage plugin configuration.

When you configure the S3 storage plugin to use an external provider, Drill first checks the external provider for the keys. If the keys are not available via the provider, or the provider is not configured, Drill can fall back to using the plain text data in the `core-site.xml` file or S3 configuration, unless the `hadoop.security.credential.clear-text-fallback` property is set to `false`.  

**Configuring the S3 Plugin to use an External Provider**
Add the bucket name, `hadoop.security.credential.provider.path` and `fs.s3a.impl.disable.cache` properties to the S3 storage plugin configuration, as shown in the following example:
 
	{
	 "type":
	"file",
	  "connection": "s3a://bucket-name/",
	  "config": {
	  	"hadoop.security.credential.provider.path":"jceks://file/tmp/s3.jceks",
	  	"Fs.s3a.impl.disable.cache":"true",
	  	...
	  	},
	  "workspaces": {
	    ...
	  }

 
**Note:** The `hadoop.security.credential.provider.path` property should point to a file that contains your encrypted passwords. The `fs.s3a.impl.disable.cache` option must be set to true.

###Adding Credentials Directly to the S3 Plugin  
You can add your AWS credentials directly to the S3 configuration, though this method is the least secure, but sufficient for use on a single machine, such as a laptop. 

Add the S3 bucket name and the `"config"` block with the properties shown in the following example: 

    {
	"type": "file",
	"enabled": true,
	"connection": "s3a://bucket-name/",
	"config": {
		"fs.s3a.access.key": "<key>",
		"fs.s3a.secret.key": "<key>",
		"fs.s3a.endpoint": "s3.us-west-1.amazonaws.com"
	},
	"workspaces": {...
		},
	
         
Drill can now use the HDFS s3a library to access data in S3.


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

