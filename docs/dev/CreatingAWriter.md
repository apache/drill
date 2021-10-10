# Creating a Writer for a Storage Plugin
This tutorial explains the mostly undocumented features of how to create a writer for a Drill storage plugin.  

Much has been written about creating a storage plugin in Drill [1], however all this focuses on the reader interface.  What if you want to write data back out to that storage 
plugin?  At the time of writing , Drill only implemented writing for filesystems, and for the Kudu storage plugin, 
however [DRILL-8005](https://github.com/apache/drill/pull/2327) adds this capability to the JDBC storage plugin as well. This will hopefully be merged in Drill 1.20.0. 

## Step One: Set the Flags to Enable Writing
The absolute first thing you will need to do is set a `boolean` flag in whichever class extends the `AbstractStoragePlugin`.  This is accomplished by overwriting the  
`supportsWrite()` and making sure that it returns `true`.  In the case of the JDBC plugin, it is configurable whether the individual connection is writable or not, so we are 
pulling the value from there, however this could simply be `return true`.

```java
@Override
  public boolean supportsWrite() {
    return config.isWritable();
  }
```

At this point, I would recommend creating a unit test something like the following:

```java
  @Test
  public void testBasicCTAS() throws Exception {
    String query = "CREATE TABLE mysql.`drill_mysql_test`.`test_table` (ID, NAME) AS SELECT * FROM (VALUES(1,2), (3,4))";
    // Create the table and insert the values
    QuerySummary insertResults = queryBuilder().sql(query).run();
    assertTrue(insertResults.succeeded());
  }
```

We will use this unit test to debug the various pieces of the process.

## Adding the Table
At the time of writing, Drill only supports the following DDL statements:

* `CREATE TABLE AS <sql query>`
* `CREATE TABLE IF NOT EXISTS AS <sql query>`
* `DROP TABLE`
* `DROP TABLE IF NOT EXISTS`

In order to implement the logic to create a table, you'll have to find the classes in your storage plugin that extend the `AbstractSchema` class.  The first step is to 
overwrite the `createTableEntry()` function as shown below.

```java
  @Override
  public CreateTableEntry createNewTable(String tableName, List<String> partitionColumns, StorageStrategy strategy) {
    if (! plugin.getConfig().isWritable()) {
      throw UserException
        .dataWriteError()
        .message(plugin.getName() + " is not writable.")
        .build(logger);
    }

    return new CreateTableEntry() {

      @Override
      public Writer getWriter(PhysicalOperator child) throws IOException {
        String tableWithSchema = JdbcQueryBuilder.buildCompleteTableName(tableName, catalog, schema);
        return new JdbcWriter(child, tableWithSchema, inner, plugin);
      }

      @Override
      public List<String> getPartitionColumns() {
        return Collections.emptyList();
      }
    };
  }
```
This function should be overwritten in the lowest level class that extends `AbstractSchema`.  In the example above, the function first checks to see if the storage plugin is 
writable or not, and if not throws an exception.  This was done so that the user received an understandable error message.

[1]: https://github.com/paul-rogers/drill/wiki/Create-a-Storage-Plugin

