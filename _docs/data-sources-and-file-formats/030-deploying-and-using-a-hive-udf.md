---
title: "Deploying and Using a Hive UDF"
date: 2018-11-02
parent: "Data Sources and File Formats"
---
If the extensive Hive functions, such as the mathematical and date functions, which Drill supports do not meet your needs, you can use a Hive UDF in Drill queries. Drill supports your existing Hive scalar UDFs. You can do queries on Hive tables and access existing Hive input/output formats, including custom serdes. Drill serves as a complement to Hive deployments by offering low latency queries.

## Creating the UDF
You create the JAR for a UDF to use in Drill in a conventional manner with a few caveats, using a unique name and creating a Drill resource, covered in this section. Sample code for this function is in [Github](https://github.com/viadea/HiveUDF).

1. Use a unique name for the Hive UDF to avoid conflicts with Drill custom functions of the same name.

        @Description(
                name = "my_upper",
                value = "_FUNC_(str) - Converts a string to uppercase",
                extended = "Example:\n" +
                "  > SELECT my_upper(a) FROM test;\n" +
                "  ABC"
                )

2. Create a custom Hive UDF using either of these APIs:  
   * Simple API: org.apache.hadoop.hive.ql.exec.UDF
   * Complex API: org.apache.hadoop.hive.ql.udf.generic.GenericUDF
3. Create an empty `drill-module.conf` in the resources directory in the Java project.  

        # ls -altr src/main/resources/drill-module.conf
        -rw-r--r-- 1 root root 0 Aug 12 23:16 src/main/resources/drill-module.conf

4. Export the logic to a JAR, including the `drill-module.conf` file in resources.

5. Make sure the drill-module.conf is in the JAR.

        # jar tf target/MyUDF-1.0.0.jar  |grep -i drill
        drill-module.conf

6. Test the UDF in Hive as shown in the [Github readme](https://github.com/viadea/HiveUDF/#c-test-udf).

The `drill-module.conf` file defines [startup options]({{ site.baseurl }}/docs/start-up-options/) and makes the JAR functions available to use in queries throughout the Hadoop cluster. After exporting the UDF logic to a JAR file, set up the UDF in Drill. Drill users can access the custom UDF for use in Hive queries.

## Setting Up a UDF
After you export the custom UDF as a JAR, perform the UDF setup tasks so Drill can access the UDF. The JAR needs to be available at query execution time as a session resource, so Drill queries can refer to the UDF by its name.
 
To set up the UDF:

1. Enable the default [Hive storage plugin configuration]({{ site.baseurl }}/docs/hive-storage-plugin/) that connects Drill to a Hive data source.  
2. Add the JAR for the UDF in the `/jars/3rdparty` directory of the Drill installation on all nodes running a Drillbit.  
    `clush -a cp /xxx/target/MyUDF-1.0.0.jar /xxx/drill-1.1.0/jars/3rdparty/`  
3. On each Drill node in the cluster, restart the Drillbit.  
   `<drill installation directory>/bin/drillbit.sh restart`
 
## Using a UDF
Use a Hive UDF just as you would use a Drill custom function. For example, to query using a Hive UDF named MY_UPPER, the SELECT statement looks something like this:  
     
    SELECT MY_UPPER('abc') from (VALUES(1));
    +---------+
    | EXPR$0  |
    +---------+
    | ABC     |
    +---------+
    1 row selected (1.516 seconds)







