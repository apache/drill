# OperatorFixture

Drill provides a simple way to test internal components without a full Drill server (assuming, of course, that the component is structured in a way that allows such testing; something that only a few parts of the code allow at the moment.)

Testing is based on the `OperatorFixture` which sets up the basic internal mechanisms:

* Memory allocator
* Operator context
* Fragment context (but without the server-related `DrillbitContext`)

Here is a very simple example to allow testing a UDF that uses VarChar holders, which in turn use DrillBuf, which requires access to a memory allocator:

```
  @Test
  public void testDupItFn() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      OperatorContext context = fixture.operatorContext(null);
      try {
        // Test code goes here
      } finally {
        context.close();
      }
    }
  }
```

In the above, we simply create an instance of the `OperatorFixture` in a try-with-resources block so we can be sure that the memory allocator shuts down, even if the test fails.

Then, since this test needs an `OperatorContext`, we go ahead and create one of those. Since it isn't auto-closeable, we run tests in a try/finally block so we can be sure it is closed.

Then, our code that run tests (in this case, that allocates a buffer using the managed allocator) goes in the test block.

Tests that don't need an operator context are even simpler. See Drill code for examples.
