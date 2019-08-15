# RowSet Framework

The RowSet Framework allows you to create custom instances of:

 * [BatchSchema](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/BatchSchema.java)
 * [VectorContainer](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/VectorContainer.java)
 * [TupleMetadata](../../exec/vector/src/main/java/org/apache/drill/exec/record/metadata/TupleMetadata.java)
 * [RecordBatch](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/RecordBatch.java): This is effectively a fake instance of an upstream operator.
 
It also allows the comparison of data container in [VectorContainer](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/VectorContainer.java)s through the use
of the [RowSetComparison](../../exec/java-exec/src/test/java/org/apache/drill/test/rowSet/RowSetComparison.java) and 
[RowSetUtilities](../../exec/java-exec/src/test/java/org/apache/drill/test/rowSet/RowSetUtilities.java).

## Creating A [BatchSchema](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/BatchSchema.java)

The [SchemaBuilder](../../exec/vector/src/main/java/org/apache/drill/exec/record/metadata/SchemaBuilder.java) class can be used
to create an instance [BatchSchema](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/BatchSchema.java). An example 
of how to to this can be found the **secondTest()** method of [ExampleTest](../../exec/java-exec/src/test/java/org/apache/drill/test/ExampleTest.java).

**Note:** The [BatchSchema](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/BatchSchema.java) class has limited complex type support. When
possible use [TupleMetadata](../../exec/vector/src/main/java/org/apache/drill/exec/record/metadata/TupleMetadata.java) and
[TupleSchema](../../exec/vector/src/main/java/org/apache/drill/exec/record/metadata/TupleSchema.java) instead.

## Creating [TupleMetadata](../../exec/vector/src/main/java/org/apache/drill/exec/record/metadata/TupleMetadata.java)

```
TupleMetadata schema = new SchemaBuilder()
    .add(...)
    .add(...)
    .buildSchema();
```

## Creating Test [VectorContainer](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/VectorContainer.java)

[VectorContainer](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/VectorContainer.java)s populated with data can be created with the 
[RowSetBuilder](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/rowSet/RowSetBuilder.java). In order to use it do the following:

 1. Create an allocator
    ```
    BufferAllocator allocator = operatorFixture.allocator();
    ```
 1. Create the desired BatchSchema using the [SchemaBuilder](../../exec/vector/src/main/java/org/apache/drill/exec/record/metadata/SchemaBuilder.java).
    ```
    TupleMetadata schema = new SchemaBuilder()
            .add(...)
            .add(...)
            .buildSchema();
    ```
 1. Create a [RowSetBuilder](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/rowSet/RowSetBuilder.java) and add
    records to it. Then build a [RowSet](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/rowSet/RowSet.java).
    ```
    RowSet rowSet = new RowSetBuilder(allocator, schema)
      .addRow(110, "green", new floatArray(5.5f, 2.3f), strArray("1a", "1b"))
      .addRow(109, "blue", new floatArray(1.5f), strArray("2a"))
      .addRow(108, "red", new floatArray(-11.1f, 0.0f, .5f), strArray("3a", "3b", "3c"))
      .build();
    ```
 1. Retrieve the [VectorContainer](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/VectorContainer.java) wrapped by the
    [RowSet](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/rowSet/RowSet.java).
    ```
    VectorContainer container = rowSet.container();
    ```

## Creating A Mock Record Batch (Upstream Operator) With Data

Create a [RowSet](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/rowSet/RowSet.java) and then create a
[RowSetBatch](../../exec/java-exec/src/test/java/org/apache/drill/exec/physical/impl/MockRecordBatch.java)

```
MockRecordBatch.Builder rowSetBatchBuilder = new MockRecordBatch.Builder()
    .sendData(rowSet)
```

## Comparison Of Results

### Compare Two [RowSet](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/rowSet/RowSet.java)s

Use [RowSetUtilities](../../exec/java-exec/src/test/java/org/apache/drill/test/rowSet/RowSetUtilities.java).

```
RowSetUtilities.verify(expectedRowSet, actualRowSet)
```

### Compare A [VectorContainer](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/VectorContainer.java) To A [RowSet](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/rowSet/RowSet.java)

You can convert a [VectorContainer](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/VectorContainer.java) into a [RowSet](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/rowSet/RowSet.java)
a few ways:

 * If you are using an [OperatorFixture](OperatorFixture.md) the best way to do this is with:
   ```
   operatorFixture.wrap(container);
   ```
 * When there is no selection vector you can do the following:
   ```
   RowSet rowSet = DirectRowSet.fromContainer(container);
   ```
 * When there is a [SelectionVector2](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/selection/SelectionVector2.java).
   ```
   RowSet rowSet = IndirectRowSet.fromSv2(container, container.getSelectionVector2());
   ```
 * When there is a [SelectionVector4](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/selection/SelectionVector4.java).
   ```
   RowSet rowSet = HyperRowSetImpl.fromContainer(container, container.getSelectionVector4());
   ```

After the [VectorContainer](../../exec/java-exec/src/main/java/org/apache/drill/exec/record/VectorContainer.java) is wrapped in a [RowSet](../.
./exec/java-exec/src/test/java/org/apache/drill/test/rowSet/RowSet.java) you can compare the two
[RowSet](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/rowSet/RowSet.java)s as usual.

## End To End Example

A good example of building a [RowSet](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/rowSet/RowSet.java) and comparing results can be found in the 
**testInitialSchema()** test in [TestResultSetLoaderProtocol](../../exec/java-exec/src/test/java/org/apache/drill/exec/physical/resultSet/impl/TestResultSetLoaderProtocol.java).
