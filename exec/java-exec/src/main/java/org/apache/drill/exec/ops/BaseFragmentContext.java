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

import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.physical.impl.common.CodeGenMemberInjector;

import io.netty.buffer.DrillBuf;

/**
 * Common implementation for both the test and production versions
 * of the fragment context.
 */
public abstract class BaseFragmentContext implements FragmentContext {

  private final FunctionImplementationRegistry funcRegistry;

  public BaseFragmentContext(final FunctionImplementationRegistry funcRegistry) {
    this.funcRegistry = funcRegistry;
  }

  @Override
  public FunctionImplementationRegistry getFunctionRegistry() {
    return funcRegistry;
  }

  @Override
  public <T> T getImplementationClass(final ClassGenerator<T> cg)
      throws ClassTransformationException, IOException {
    return getImplementationClass(cg.getCodeGenerator());
  }

  @Override
  public <T> T getImplementationClass(final CodeGenerator<T> cg)
      throws ClassTransformationException, IOException {
    T instance = getCompiler().createInstance(cg);
    CodeGenMemberInjector.injectMembers(cg.getRoot(), instance, this);
    return instance;
  }

  @Override
  public <T> List<T> getImplementationClass(final ClassGenerator<T> cg, final int instanceCount) throws ClassTransformationException, IOException {
    return getImplementationClass(cg.getCodeGenerator(), instanceCount);
  }

  @Override
  public <T> List<T> getImplementationClass(final CodeGenerator<T> cg, final int instanceCount) throws ClassTransformationException, IOException {
    List<T> instances = getCompiler().createInstances(cg, instanceCount);
    instances.forEach(instance -> CodeGenMemberInjector.injectMembers(cg.getRoot(), instance, this));
    return instances;
  }

  protected abstract BufferManager getBufferManager();

  @Override
  public DrillBuf replace(final DrillBuf old, final int newSize) {
    return getBufferManager().replace(old, newSize);
  }

  @Override
  public DrillBuf getManagedBuffer() {
    return getBufferManager().getManagedBuffer();
  }

  @Override
  public DrillBuf getManagedBuffer(final int size) {
    return getBufferManager().getManagedBuffer(size);
  }

}
