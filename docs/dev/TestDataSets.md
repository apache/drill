# Data Sets

Drill includes several data sets for testing, and also provides some tools for generating test data sets.

## Bundled Data Sets

There are three primary data sets bundled with drill for testing:

  - **Sample Data:** These are parquet files in the [sample-data](../../sample-data) folder.
  - **Resource Data:** These are data files in the [exec/java-exec/src/test/resources](../../exec/java-exec/src/test/resources) folder.
  - **TPCH Data:** These are trimmed down versions of the tpch data sets. They are retrieved and bundled
  in the [contrib/data](../../contrib/data) maven submodule. They are also accessible on [Apache Drill's S3 bucket](http://apache-drill.s3.amazonaws.com/files/sf-0
  .01_tpc-h_parquet.tgz).
  When unit tests are running all of the files in these data set are available from the classpath storage plugin. The tpch
  files include:
    - **customer.parquet**
    - **lineitem.parquet**
    - **nation.parquet**
    - **orders.parquet**
    - **part.parquet**
    - **partsup.parquet**
    - **region.parquet**
    - **supplier.parquet**
  
### Using Sample Data in Unit Tests

#### [ClusterFixture](ClusterFixture.md)

See **seventhTest()** in [ExampleTest](../../exec/java-exec/src/test/java/org/apache/drill/test/ExampleTest.java) for an example of how to do this.

#### BaseTestQuery (Deprecated Use [ClusterFixture](ClusterFixture.md) or [ClusterTest](ClusterTest.md) Instead)

When using the [BaseDirTestWatcher](../../exec/java-exec/src/test/java/org/apache/drill/test/BaseDirTestWatcher.java) you
can make [sample-data](../../sample-data) accessible from the ```dfs``` storage plugin by doing the following:

```
public class TestMyClass {
  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();
  
  @BeforeClass
  public static void setupFiles() {
    dirTestWatcher.copyFileToRoot(Paths.get("sample-data", "region.parquet"));
  }
  
  @Test
  public void simpleTest() {
     // dfs.root.`sample-data/region.parquet` will be accessible from my test
  }
}
```

Or if you are extending [BaseTestQuery](../../exec/java-exec/src/test/java/org/apache/drill/test/BaseDirTestWatcher.java)

```
public class TestMyClass extends BaseTestQuery {
  @BeforeClass
  public static void setupFiles() {
    dirTestWatcher.copyFileToRoot(Paths.get("sample-data", "region.parquet"));
  }
  
  @Test
  public void simpleTest() {
     // dfs.root.`sample-data/region.parquet` will be accessible from my test
  }
}
```

### Using Resource Data in Unit Tests

#### [ClusterFixture](ClusterFixture.md)

See **sixthTest()** in [ExampleTest](../../exec/java-exec/src/test/java/org/apache/drill/test/ExampleTest.java) for an example of how to do this.

#### BaseTestQuery (Deprecated Use [ClusterFixture](ClusterFixture.md) or [ClusterTest](ClusterTest.md) Instead)

When using the [BaseDirTestWatcher](../../exec/java-exec/src/test/java/org/apache/drill/test/BaseDirTestWatcher.java) you
can make data from [exec/java-exec/src/test/resources](../../exec/java-exec/src/test/resources) accessible from the ```dfs``` storage plugin by doing the following:

```
public class TestMyClass {
  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();
  
  @BeforeClass
  public static void setupFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("join", "empty_part"));
  }
  
  @Test
  public void simpleTest() {
     // src/test/resources/join/empty_part is acessible at dfs.root.`join/empty_part` from my test
  }
}
```

Or if you are extending [BaseTestQuery](../../exec/java-exec/src/test/java/org/apache/drill/test/BaseDirTestWatcher.java)

```
public class TestMyClass extends BaseTestQuery {
  @BeforeClass
  public static void setupFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("join", "empty_part"));
  }
  
  @Test
  public void simpleTest() {
     // src/test/resources/join/empty_part is acessible at dfs.root.`join/empty_part` from my test
  }
}
```

### Using TPCH Data in Unit Tests

TPCH data is accessible via the classpath storage plugin

```
cp.`tpch/customer.parquet`
```

## Generating Data Sets

There are a few ways to generate data for testing:

 * [JsonFileBuilder](../../exec/java-exec/src/test/java/org/apache/drill/test/rowSet/file/JsonFileBuilder.java)
 * Inline Mock Scanner
 * [MockRecordReader](./MockRecordReader.md)

### Json

The [JsonFileBuilder](../../exec/java-exec/src/test/java/org/apache/drill/test/rowSet/file/JsonFileBuilder.java)
can be used to create json data files. It's useful for creating data files for both integration and unit tests.
An example of using the JsonFileBuilder can be found in secondTest() in 
[ExampleTest](../../exec/java-exec/src/test/java/org/apache/drill/test/ExampleTest.java).

### Inline Mock Scanner

The MockScanner is a special scanner that generates data for a query. It can only be used for integration testing.

#### Using In A Query

The MockScanner can be used directly from sql queries. An example can be found in **thirdTest()** in 
[ExampleTest](../../exec/java-exec/src/test/java/org/apache/drill/test/ExampleTest.java).

The example uses the following query

```
SELECT id_i, name_s10 FROM `mock`.`employees_5`
```

The select columns encode the name and type of the columns to generate. The table name also encodes
the number of records to generate.

 - **Columns:** Columns in the sql query have the form **(name)_(type)**. **(name)** is the name that is assigned to a column. **(type)** is the data type
  of the column. Valid types are:
   - Use **i** for specifying an integer column.
   - Use **s(n)** for specifying a string column. Replace **(n)** with an integer which specifies the number of character in each varchar record.
 - **Table:** Tables in the sql query have the form **(name)_(n)**. Where **(name)** is the name of the source table. **(n)** specifies the number of records to
 generate for the source table.
