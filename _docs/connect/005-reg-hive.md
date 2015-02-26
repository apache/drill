---
title: "Registering Hive"
parent: "Storage Plugin Registration"
---
You can register a storage plugin instance that connects Drill to a Hive data
source that has a remote or embedded metastore service. When you register a
storage plugin instance for a Hive data source, provide a unique name for the
instance, and identify the type as “`hive`”. You must also provide the
metastore connection information.

Currently, Drill supports Hive version 0.13. To access Hive tables
using custom SerDes or InputFormat/OutputFormat, all nodes running Drillbits
must have the SerDes or InputFormat/OutputFormat `JAR` files in the
`<drill_installation_directory>/jars/3rdparty` folder.

## Hive Remote Metastore

In this configuration, the Hive metastore runs as a separate service outside
of Hive. Drill communicates with the Hive metastore through Thrift. The
metastore service communicates with the Hive database over JDBC. Point Drill
to the Hive metastore service address, and provide the connection parameters
in the Drill Web UI to configure a connection to Drill.

**Note:** Verify that the Hive metastore service is running before you register the Hive metastore.

To register a remote Hive metastore with Drill, complete the following steps:

  1. Issue the following command to start the Hive metastore service on the system specified in the `hive.metastore.uris`:

        hive --service metastore
  2. Navigate to [http://localhost:8047](http://localhost:8047/), and select the **Storage** tab.
  3. In the disabled storage plugins section, click **Update** next to the `hive` instance.
  4. In the configuration window, add the `Thrift URI` and port to `hive.metastore.uris`.

     **Example**
     
        {
          "type": "hive",
          "enabled": true,
          "configProps": {
            "hive.metastore.uris": "thrift://<localhost>:<port>",  
            "hive.metastore.sasl.enabled": "false"
          }
        }       
  5. Click **Enable**.
  6. Verify that `HADOOP_CLASSPATH` is set in `drill-env.sh`. If you need to set the classpath, add the following line to `drill-env.sh`.
  
        export HADOOP_CLASSPATH=/<directory path>/hadoop/hadoop-0.20.2

Once you have configured a storage plugin instance for a Hive data source, you
can [query Hive tables](/drill/docs/querying-hive/).

## Hive Embedded Metastore

In this configuration, the Hive metastore is embedded within the Drill
process. Provide the metastore database configuration settings in the Drill
Web UI. Before you register Hive, verify that the driver you use to connect to
the Hive metastore is in the Drill classpath located in `/<drill installation
dirctory>/lib/.` If the driver is not there, copy the driver to `/<drill
installation directory>/lib` on the Drill node. For more information about
storage types and configurations, refer to [AdminManual
MetastoreAdmin](/confluence/display/Hive/AdminManual+MetastoreAdmin).

To register an embedded Hive metastore with Drill, complete the following
steps:

  1. Navigate to `[http://localhost:8047](http://localhost:8047/)`, and select the **Storage** tab
  2. In the disabled storage plugins section, click **Update** next to `hive` instance.
  3. In the configuration window, add the database configuration settings.

     **Example**
     
        {
          "type": "hive",
          "enabled": true,
          "configProps": {
            "javax.jdo.option.ConnectionURL": "jdbc:<database>://<host:port>/<metastore database>;create=true",
            "hive.metastore.warehouse.dir": "/tmp/drill_hive_wh",
            "fs.default.name": "file:///",   
          }
        }
  4. Click** Enable.**
  5. Verify that `HADOOP_CLASSPATH` is set in `drill-env.sh`. If you need to set the classpath, add the following line to `drill-env.sh`.
  
        export HADOOP_CLASSPATH=/<directory path>/hadoop/hadoop-0.20.2
 