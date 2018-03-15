# Instantiating Components

There are a few techniques for creating instances classes in unit tests:

* Use a mocking library. **(Depracated)**
* Provide a simple impementation of an interface
* Build a real instance of class using the class's builders / constructors
* Use the [ClusterFixture](ClusterFixture.md) or [OperatorFixture](OperatorFixture.md) classes to create instances of objects.

## Mocking Libraries (Deprecated)

Drill uses two mocking libraries in order to mock classes.

* [Mockito](http://site.mockito.org)
* [JMockit](http://jmockit.github.io/tutorial.html)

These libraries were originally used to work around the lack of well defined interfaces and adequate testing tools. Drill has made significant improvements in these areas
so using mocking libraries are no longer required. Existing tests that use these libraries will be refactored to remove them, and new tests should NOT use these libraries.

## Instantiating Contexts

There are several contexts used throughout Drill, for a complete description of each and how
they are used please see [FragmentContextImpl](../../exec/java-exec/src/main/java/org/apache/drill/exec/ops/FragmentContextImpl.java).

When doing tests you can use the following mock contexts:

  * [MockFragmentContext](../../exec/java-exec/src/test/java/org/apache/drill/test/OperatorFixture.java) is a simple mock implementation of
    the [FragmentContext](../../exec/java-exec/src/main/java/org/apache/drill/exec/ops/FragmentContext.java). A mock instance of the class can also be retrieved from an
    [OperatorFixture](OperatorFixture.md).
    ```
    FragmentContext context = operatorFixture.getFragmentContext();
    ```

## Creating An Instance of [QueryId](../../protocol/src/main/java/org/apache/drill/exec/proto/beans/QueryId.java)

```
UserBitShared.QueryId queryId = UserBitShared.QueryId.newBuilder()
  .setPart1(1L)
  .setPart2(2L)
  .build();
```

## Creating [FragmentHandle](../../protocol/src/main/java/org/apache/drill/exec/proto/beans/FragmentHandle.java)

```
ExecProtos.FragmentHandle fragmentHandle = ExecProtos.FragmentHandle.newBuilder()
  .setQueryId(queryId)
  .setMinorFragmentId(1)
  .setMajorFragmentId(2)
  .build();
```

## Creating A [DrillConfig](../../common/src/main/java/org/apache/drill/common/config/DrillConfig.java)

There are a few ways to create a [DrillConfig](../../common/src/main/java/org/apache/drill/common/config/DrillConfig.java). The simplest way is to
 create a [ClusterFixture](ClusterFixture.md) or [OperatorFixture](OperatorFixture.md) and then do the following:

```
DrillConfig drillConfig = clusterFixture.config();
```

or

```
DrillConfig drillConfig = operatorFixture.config();
```

If you need a [DrillConfig](../../common/src/main/java/org/apache/drill/common/config/DrillConfig.java) and don't want all the extra things provided
by [ClusterFixture](ClusterFixture.md) and [OperatorFixture](OperatorFixture.md), you can use
[ConfigBuilder](../../exec/java-exec/src/test/java/org/apache/drill/test/ConfigBuilder.java).

## Creating A [SpillSet](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/impl/spill/SpillSet.java)

 1. Create a [PhysicalOperator](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/base/PhysicalOperator.java).
    ```
    HashJoinPOP pop = new HashJoinPOP(null, null, null, JoinRelType.FULL);
    ```
 1. Create a [DrillConfig](../../common/src/main/java/org/apache/drill/common/config/DrillConfig.java).
 1. Create a [FragmentHandle](../../protocol/src/main/java/org/apache/drill/exec/proto/beans/FragmentHandle.java) as described above.
 1. Create a [SpillSet](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/impl/spill/SpillSet.java).
    ```
    SpillSet spillSet = new SpillSet(config, fragmentHandle, pop);
    ```
 
## Creating A [PersistentStoreProvider](../../exec/java-exec/src/main/java/org/apache/drill/exec/store/sys/PersistentStoreProvider.java)

```
LocalPersistentStoreProvider provider = new LocalPersistentStoreProvider(drillConfig);
provider.start();
```
 
## Creating A [LogicalPlanPersistence](../../logical/src/main/java/org/apache/drill/common/config/LogicalPlanPersistence.java)

```
LogicalPlanPersistence logicalPlanPersistence = PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(drillConfig);
```

## Creating An Instance Of An Option Manager

You can create an instance of the [SystemOptionManager](../../exec/java-exec/src/main/java/org/apache/drill/exec/server/options/SystemOptionManager.java) by leveraging the
[OperatorFixture](OperatorFixture.md).

 1. Create an [OperatorFixture](OperatorFixture.md).
 1. Retrieve the [SystemOptionManager](../../exec/java-exec/src/main/java/org/apache/drill/exec/server/options/SystemOptionManager.java).
    ```
    operatorFixture.getOptionManager();
    ```
