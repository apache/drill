# Testing with Physical Plans and Mock Data

Drill processes SQL in a series of steps that result in a physical plan sent to each Drillbit for execution. Early in Drill's development the system consisted of operators, but did not yet provide the SQL parser we have today. To test operators, developers created physical plans as input to test cases. Though later tests often focused on SQL, the physical plan approach is still very valuable when the goal is to exercise specific features of operators or the fragment hierarchy as it gives us fine control over the precise test setup.

In addition, it turns out that Drill provides a very basic test data generator in the form of a mock scanner that allows a test to generate a large amount of test data without actually having a large file.

Both topics are explained here.

## Using Physical Plans

Physical plans are a JSON representation of a series of operators to be run within one or more fragments. For testing we most often consider a single fragment (or maybe two for testing the network communication operators. Here is an [example plan](https://github.com/apache/drill/blob/master/exec/java-exec/src/test/resources/xsort/oom_sort_test.json) for testing the external sort. A single operator (external sort in this case) looks like this:

```
        {
            @id:4,
            child: 3,
            pop:"external-sort",
            orderings: [
              {expr: "blue", order : "DESC"}
            ],
            initialAllocation: 1000000,
            maxAllocation: 30000000
        },
```

There are two ways to get a physical plan (short of writing it from scratch):

* Modify an existing test plan to suit your specific needs.
* Capture the physical plan created for an SQL query and modify it as needed.

## Using the Mock Scanner

Take a look at [`TestSimpleExternalSort`](https://github.com/apache/drill/blob/master/exec/java-exec/src/test/java/org/apache/drill/exec/physical/impl/xsort/TestSimpleExternalSort.java). This uses a physical plan: [`xsort/one_key_sort_descending.json`](https://github.com/apache/drill/blob/master/exec/java-exec/src/test/resources/xsort/one_key_sort_descending.json). This plan contains the following:

```
      {
           @id:1,
           pop:"mock-scan",
           url: "http://apache.org",
           entries:[
               {records: 1000000, types: [
                 {name: "blue", type: "INT", mode: "REQUIRED"},
                 {name: "green", type: "INT", mode: "REQUIRED"}
               ]}
           ]
       },
```

Which, when invoked, uses [`MockScanBatchCreator`](https://github.com/apache/drill/blob/master/exec/java-exec/src/main/java/org/apache/drill/exec/store/mock/MockScanBatchCreator.java) which creates [`MockRecordReader`](https://github.com/apache/drill/blob/master/exec/java-exec/src/main/java/org/apache/drill/exec/store/mock/MockRecordReader.java).

The mock reader invokes the `generateTestData()` method on value vectors to generate data. The generated data is not very random, however. Also, the `generateTestData()` method is marked as deprecated, meaning someone intended to remove it.

It seems this concept could be extended to do a better job of random data generation. Or, generate the data using a class provided in the physical spec.

I've not found a way to invoke the mock reader from SQL. That would be another handy addition.

## Test Framework for Physical Plans

Once you have a physical plan, you need a way to execute it within a test. Again, consider [`TestSimpleExternalSort`](https://github.com/apache/drill/blob/master/exec/java-exec/src/test/java/org/apache/drill/exec/physical/impl/xsort/TestSimpleExternalSort.java) which runs physical plans as follows:

```
public class TestSimpleExternalSort extends BaseTestQuery {
  ...
  private void sortOneKeyDescendingMergeSort() throws Throwable {
    List<QueryDataBatch> results = testPhysicalFromFileWithResults("xsort/one_key_sort_descending.json");
    // Process the results
```

The `BaseTestQuery` class sets up an embedded Drillbit and client. This pattern is different from other tests which do the setup work as part of the test. Because the client is already running, you can set session options in SQL before you run the physical plan:

```
    QueryTestUtil.test(client, "ALTER SESSION SET `a.b.c` = "def");
```

Another good enhancement would be to rationalize the many ways that tests set up the test Drillbit and client.