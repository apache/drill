title: "Deploying and Using a Hive UDF"
parent: "Data Sources"
---
If the extensive Hive functions, such as the mathematical and date functions, which Drill supports do not meet your needs, you can use a Hive UDF in Drill queries. Drill supports your existing Hive scalar UDFs. You can do queries on Hive tables and access existing Hive input/output formats, including custom serdes. Drill serves as a complement to Hive deployments by offering low latency queries.

## Creating the UDF
You create the JAR for a UDF to use in Drill in a conventional manner with a few caveats, using a unique name and creating a Drill resource, covered in this section.

1. Use a unique name for the Hive UDF to avoid conflicts with Drill custom functions of the same name.
2. Create a custom Hive UDF using either of these APIs:

   * Simple API: org.apache.hadoop.hive.ql.exec.UDF
   * Complex API: org.apache.hadoop.hive.ql.udf.generic.GenericUDF
3. Create an empty `drill-module.conf` in the resources directory in the Java project. 
4. Export the logic to a JAR, including the `drill-module.conf` file in resources.

The `drill-module.conf` file defines [startup options](/drill/docs/start-up-options/) and makes the JAR functions available to use in queries throughout the Hadoop cluster. After exporting the UDF logic to a JAR file, set up the UDF in Drill. Drill users can access the custom UDF for use in Hive queries.

## Setting Up a UDF
After you export the custom UDF as a JAR, perform the UDF setup tasks so Drill can access the UDF. The JAR needs to be available at query execution time as a session resource, so Drill queries can refer to the UDF by its name.
 
To set up the UDF:

1. Register Hive. [Register a Hive storage plugin](/drill/docs/registering-hive/) that connects Drill to a Hive data source.
2. In Drill 0.7 and later, add the JAR for the UDF to the Drill CLASSPATH. In earlier versions of Drill, place the JAR file in the `/jars/3rdparty` directory of the Drill installation on all nodes running a Drillbit.
3. On each Drill node in the cluster, restart the Drillbit.
   `<drill installation directory>/bin/drillbit.sh restart`
 
## Using a UDF
Use a Hive UDF just as you would use a Drill custom function. For example, to query using a Hive UDF named upper-to-lower that takes a column.value argument, the SELECT statement looks something like this:  
     
     SELECT upper-to-lower(my_column.myvalue) FROM mytable;
     






