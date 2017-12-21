/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.BaseFragmentContext;
import org.apache.drill.exec.ops.BaseOperatorContext;
import org.apache.drill.exec.ops.BufferManager;
import org.apache.drill.exec.ops.BufferManagerImpl;
import org.apache.drill.exec.ops.FragmentContextInterface;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.test.ClusterFixtureBuilder.RuntimeOption;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.HyperRowSetImpl;
import org.apache.drill.test.rowSet.IndirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Test fixture for operator and (especially) "sub-operator" tests.
 * These are tests that are done without the full Drillbit server.
 * Instead, this fixture creates a test fixture runtime environment
 * that provides "real" implementations of the classes required by
 * operator internals, but with implementations tuned to the test
 * environment. The services available from this fixture are:
 * <ul>
 * <li>Configuration (DrillConfig)</li>
 * <li>Memory allocator</li>
 * <li>Code generation (compilers, code cache, etc.)</li>
 * <li>Read-only version of system and session options (which
 * are set when creating the fixture.</li>
 * <li>Write-only version of operator stats (which are easy to
 * read to verify in tests.</li>
 * </ul>
 * What is <b>not</b> provided is anything that depends on a live server:
 * <ul>
 * <li>Network endpoints.</li>
 * <li>Persistent storage.</li>
 * <li>ZK access.</li>
 * <li>Multiple threads of execution.</li>
 * </ul>
 */

public class OperatorFixture extends BaseFixture implements AutoCloseable {

  /**
   * Builds an operator fixture based on a set of config options and system/session
   * options.
   */

  public static class OperatorFixtureBuilder
  {
    protected ConfigBuilder configBuilder = new ConfigBuilder();
    protected List<RuntimeOption> systemOptions;
    protected ExecutionControls controls;

    public ConfigBuilder configBuilder() {
      return configBuilder;
    }

    public OperatorFixtureBuilder systemOption(String key, Object value) {
      if (systemOptions == null) {
        systemOptions = new ArrayList<>();
      }
      systemOptions.add(new RuntimeOption(key, value));
      return this;
    }

    public OperatorFixtureBuilder setControls(ExecutionControls controls) {
      this.controls = controls;
      return this;
    }

    public OperatorFixture build() {
      return new OperatorFixture(this);
    }
  }

  /**
   * Provide a simplified test-time code generation context that
   * uses the same code generation mechanism as the full Drill, but
   * provide test-specific versions of various other services.
   */

  public static class TestFragmentContext extends BaseFragmentContext {

    private final DrillConfig config;
    private final OptionManager options;
    private final CodeCompiler compiler;
    private ExecutionControls controls;
    private final BufferManagerImpl bufferManager;
    private final BufferAllocator allocator;

    public TestFragmentContext(DrillConfig config, OptionManager options, BufferAllocator allocator) {
      super(newFunctionRegistry(config, options));
      this.config = config;
      this.options = options;
      this.allocator = allocator;
      compiler = new CodeCompiler(config, options);
      bufferManager = new BufferManagerImpl(allocator);
    }

    private static FunctionImplementationRegistry newFunctionRegistry(
        DrillConfig config, OptionManager options) {
      ScanResult classpathScan = ClassPathScanner.fromPrescan(config);
      return new FunctionImplementationRegistry(config, classpathScan, options);
    }

    public void setExecutionControls(ExecutionControls controls) {
      this.controls = controls;
    }

    @Override
    public OptionManager getOptions() {
      return options;
    }

    @Override
    public boolean shouldContinue() {
      return true;
    }

    @Override
    public ExecutionControls getExecutionControls() {
      return controls;
    }

    @Override
    public DrillConfig getConfig() {
      return config;
    }

    @Override
    public DrillbitContext getDrillbitContext() {
      throw new UnsupportedOperationException("Drillbit context not available for operator unit tests");
    }

    @Override
    protected CodeCompiler getCompiler() {
       return compiler;
    }

    @Override
    protected BufferManager getBufferManager() {
      return bufferManager;
    }

    @SuppressWarnings("resource")
    @Override
    public OperatorContext newOperatorContext(PhysicalOperator popConfig,
        OperatorStats stats) throws OutOfMemoryException {
      BufferAllocator childAllocator = allocator.newChildAllocator(
          "test:" + popConfig.getClass().getSimpleName(),
          popConfig.getInitialAllocation(),
          popConfig.getMaxAllocation()
          );
      return new TestOperatorContext(this, childAllocator, popConfig);
    }

    @Override
    public OperatorContext newOperatorContext(PhysicalOperator popConfig)
        throws OutOfMemoryException {
      return newOperatorContext(popConfig, null);
    }

    @Override
    public String getQueryUserName() {
      return "fred";
    }
  }

  private final SystemOptionManager options;
  private final TestFragmentContext context;

  protected OperatorFixture(OperatorFixtureBuilder builder) {
    config = builder.configBuilder().build();
    allocator = RootAllocatorFactory.newRoot(config);
    options = new SystemOptionManager(config);
    try {
      options.init();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to initialize the system option manager", e);
    }
    if (builder.systemOptions != null) {
      applySystemOptions(builder.systemOptions);
    }
    context = new TestFragmentContext(config, options, allocator);
   }

  private void applySystemOptions(List<RuntimeOption> systemOptions) {
    for (RuntimeOption option : systemOptions) {
      options.setLocalOption(option.key, option.value);
    }
  }

  public SystemOptionManager options() { return options; }
  public FragmentContextInterface fragmentContext() { return context; }

  @Override
  public void close() throws Exception {
    allocator.close();
    options.close();
  }

  public static OperatorFixtureBuilder builder() {
    OperatorFixtureBuilder builder = new OperatorFixtureBuilder();
    builder.configBuilder()
      // Required to avoid Dynamic UDF calls for missing or
      // ambiguous functions.
      .put(ExecConstants.UDF_DISABLE_DYNAMIC, true);
    return builder;
  }

  public static OperatorFixture standardFixture() {
    return builder().build();
  }

  public RowSetBuilder rowSetBuilder(BatchSchema schema) {
    return rowSetBuilder(TupleSchema.fromFields(schema));
  }

  public RowSetBuilder rowSetBuilder(TupleMetadata schema) {
    return new RowSetBuilder(allocator, schema);
  }

  public ExtendableRowSet rowSet(BatchSchema schema) {
    return DirectRowSet.fromSchema(allocator, schema);
  }

  public ExtendableRowSet rowSet(TupleMetadata schema) {
    return DirectRowSet.fromSchema(allocator, schema);
  }

  public RowSet wrap(VectorContainer container) {
    switch (container.getSchema().getSelectionVectorMode()) {
    case FOUR_BYTE:
      return HyperRowSetImpl.fromContainer(container, container.getSelectionVector4());
    case NONE:
      return DirectRowSet.fromContainer(container);
    case TWO_BYTE:
      return IndirectRowSet.fromSv2(container, container.getSelectionVector2());
    default:
      throw new IllegalStateException( "Unexpected selection mode" );
    }
  }

  public static class TestOperatorContext extends BaseOperatorContext {

    private final OperatorStats stats;

    public TestOperatorContext(FragmentContextInterface fragContext,
        BufferAllocator allocator,
        PhysicalOperator config) {
      super(fragContext, allocator, config);
      stats = new OperatorStats(100, 101, 0, allocator);
    }

    @Override
    public OperatorStats getStats() { return stats; }

    @Override
    public <RESULT> ListenableFuture<RESULT> runCallableAs(
        UserGroupInformation proxyUgi, Callable<RESULT> callable) {
      throw new UnsupportedOperationException("Not yet");
    }
  }

  @SuppressWarnings("resource")
  public OperatorContext newOperatorContext(PhysicalOperator popConfig) {
    BufferAllocator childAllocator = allocator.newChildAllocator(
        "test:" + popConfig.getClass().getSimpleName(),
        popConfig.getInitialAllocation(),
        popConfig.getMaxAllocation()
        );
    return new TestOperatorContext(context, childAllocator, popConfig);
  }

  public RowSet wrap(VectorContainer container, SelectionVector2 sv2) {
    if (sv2 == null) {
      assert container.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE;
      return DirectRowSet.fromContainer(container);
    } else {
      assert container.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE;
      return IndirectRowSet.fromSv2(container, sv2);
    }
  }
}
