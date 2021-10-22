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

You will also need to ovewrite the `isMutable()` function as well.  

```java
  @Override
  public boolean isMutable() {
    return plugin.getConfig().isWritable();
  }
```
At this point, I would recommend creating a unit test something like the following.  For the JDBC plugin, there are unit tests for writable and unwritable plugin instances.  
You could just set these functions to return `true`, however in the case of JDBC, we wanted this controllable at the config level.

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

## Step Two: Adding the Table
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

THe JDBC implementation does not allow for partitions in inserts, so this function simply returns an empty collection.

At this point, you should set a breakpoint in the `CreateTableEntry`, run the unit test and make sure that it is getting to the function. 

## Step Three: Batching and Writing
The next step you'll have to do is create a class which extends the `AbstractWriter` class. This is really just a serializable holder.  Take a look at the `JdbcWriter` class in 
the JDBC Storage Plugin for an example. 

You'll also need to create a batch creator object which creates the actual batch writers. 

```java
public class JdbcWriterBatchCreator implements BatchCreator<JdbcWriter> {

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, JdbcWriter config, List<RecordBatch> children)
    throws ExecutionSetupException {
    assert children != null && children.size() == 1;

    return new WriterRecordBatch(config, children.iterator().next(), context,
      new JdbcRecordWriter (config.getPlugin().getDataSource(), null, config.getTableName(), config));
  }
}
```
The example above from the JDBC plugin is a single threaded example.  However, you could implement this such that multiple batch readers would be created.

The final step is to implement a class which extends the `AbstractRecordWriter` interface.  This interface was originally meant for writing files, so not all of these methods 
will line up well with other storage plugins.  In any event, you will have to implement the following methods:

* `init(Map<String, String> writerOptions)`:  This function is called once when the first RecordBatch is created.  This function could be used to establish connections or do 
  other preparatory work. 
* `updateSchema(VectorAccessible batch)`:  The `updateSchema()` function is also called once when the schema is actually created or updated.  It is unclear whether this 
  function is called during each batch.  
* `startRecord()`:  Called at the beginning of each record.  This corresponds to the beginning of a record row. 
* `endRecord()`: Called at the end of each row record. 
* `abort()`:  Called in the event the writing did not succeed.
* `cleanup()`:  This is called at the end of a successful batch.  For the JDBC plugin, this is where the actual INSERT query is executed. 

Once you've implemented these methods, you should put a breakpoint in each one, run your unit test and see that all the functions are being hit. 

Now the bad news, you'll need to create functions which overwrite the converters for all data types.  You can do this with a FreeMarker template, but you'll have to have one 
function for each data type, and for each mode, `NULLABLE`, `REQUIRED`, and `REPEATED`.  Which data types you'll need to implement will depend on what data types are supported 
in your source system.  If you don't implement a particular converter function, the user will receive an error stating that the particular data type is not supported.

```java
  @Override
  public FieldConverter getNewNullableIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableIntJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableIntJDBCConverter extends FieldConverter {
    private final NullableIntHolder holder = new NullableIntHolder();

    public NullableIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.INT, DataMode.OPTIONAL));
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add("null");
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new IntJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class IntJDBCConverter extends FieldConverter {
    private final IntHolder holder = new IntHolder();

    public IntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.INT, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

```
That's it!  You're done!  Now write a bunch of unit tests and make sure it works!



[1]: https://github.com/paul-rogers/drill/wiki/Create-a-Storage-Plugin

