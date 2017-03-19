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
package org.apache.drill.exec.ops;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ExecutionControls;

/**
 * Implementation of the context used by low-level operator
 * tasks.
 */

public class OperExecContextImpl implements OperExecContext {

  private FragmentExecContext fragmentContext;
  private PhysicalOperator operDefn;
  private ControlsInjector injector;
  private BufferAllocator allocator;
  private OperatorStatReceiver stats;

  public OperExecContextImpl(FragmentExecContext fragContext, OperatorContext opContext, PhysicalOperator opDefn, ControlsInjector injector) {
    this(fragContext, opContext.getAllocator(), opContext.getStats(), opDefn, injector);
  }

  public OperExecContextImpl(FragmentExecContext fragContext, BufferAllocator allocator, OperatorStatReceiver stats, PhysicalOperator opDefn, ControlsInjector injector) {
    this.fragmentContext = fragContext;
    this.operDefn = opDefn;
    this.injector = injector;
    this.allocator = allocator;
    this.stats = stats;
  }

  @Override
  public FunctionImplementationRegistry getFunctionRegistry() {
    return fragmentContext.getFunctionRegistry();
  }

  @Override
  public OptionSet getOptionSet() {
    return fragmentContext.getOptionSet();
  }

  @Override
  public <T> T getImplementationClass(ClassGenerator<T> cg)
      throws ClassTransformationException, IOException {
    return fragmentContext.getImplementationClass(cg);
  }

  @Override
  public <T> T getImplementationClass(CodeGenerator<T> cg)
      throws ClassTransformationException, IOException {
    return fragmentContext.getImplementationClass(cg);
  }

  @Override
  public <T> List<T> getImplementationClass(ClassGenerator<T> cg,
      int instanceCount) throws ClassTransformationException, IOException {
    return fragmentContext.getImplementationClass(cg, instanceCount);
  }

  @Override
  public <T> List<T> getImplementationClass(CodeGenerator<T> cg,
      int instanceCount) throws ClassTransformationException, IOException {
    return fragmentContext.getImplementationClass(cg, instanceCount);
  }

  @Override
  public boolean shouldContinue() {
    return fragmentContext.shouldContinue();
  }

  @Override
  public ExecutionControls getExecutionControls() {
    return fragmentContext.getExecutionControls();
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public OperatorStatReceiver getStats() {
    return stats;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends PhysicalOperator> T getOperatorDefn() {
    return (T) operDefn;
  }

  @Override
  public DrillConfig getConfig() {
    return fragmentContext.getConfig();
  }

  @Override
  public ControlsInjector getInjector() {
    return injector;
  }

  @Override
  public void injectUnchecked(String desc) {
    ExecutionControls executionControls = fragmentContext.getExecutionControls();
    if (injector != null  &&  executionControls != null) {
      injector.injectUnchecked(executionControls, desc);
    }
  }

  @Override
  public <T extends Throwable> void injectChecked(String desc, Class<T> exceptionClass)
      throws T {
    ExecutionControls executionControls = fragmentContext.getExecutionControls();
    if (injector != null  &&  executionControls != null) {
      injector.injectChecked(executionControls, desc, exceptionClass);
    }
  }

}
