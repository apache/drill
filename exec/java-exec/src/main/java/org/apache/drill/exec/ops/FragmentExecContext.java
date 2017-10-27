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
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.testing.ExecutionControls;

/**
 * Services passed to fragments that deal only with execution details
 * such as the function registry, options, code generation and the like.
 * Does not include top-level services such as network endpoints. Code
 * written to use this interface can be unit tested quite easily using
 * the {@link OperatorContext} class. Code that uses the wider,
 * more global {@link FragmentContext} must be tested in the context
 * of the entire Drill server, or using mocks for the global services.
 */

public interface FragmentExecContext {
  /**
   * Returns the UDF registry.
   * @return the UDF registry
   */
  FunctionImplementationRegistry getFunctionRegistry();
  /**
   * Returns a read-only version of the session options.
   * @return the session options
   */
  OptionSet getOptionSet();

  /**
   * Generates code for a class given a {@link ClassGenerator},
   * and returns a single instance of the generated class. (Note
   * that the name is a misnomer, it would be better called
   * <tt>getImplementationInstance</tt>.)
   *
   * @param cg the class generator
   * @return an instance of the generated class
   */

  <T> T getImplementationClass(final ClassGenerator<T> cg)
      throws ClassTransformationException, IOException;

  /**
   * Generates code for a class given a {@link CodeGenerator},
   * and returns a single instance of the generated class. (Note
   * that the name is a misnomer, it would be better called
   * <tt>getImplementationInstance</tt>.)
   *
   * @param cg the code generator
   * @return an instance of the generated class
   */

  <T> T getImplementationClass(final CodeGenerator<T> cg)
      throws ClassTransformationException, IOException;

  /**
   * Generates code for a class given a {@link ClassGenerator}, and returns the
   * specified number of instances of the generated class. (Note that the name
   * is a misnomer, it would be better called
   * <tt>getImplementationInstances</tt>.)
   *
   * @param cg
   *          the class generator
   * @return list of instances of the generated class
   */

  <T> List<T> getImplementationClass(final ClassGenerator<T> cg, final int instanceCount)
      throws ClassTransformationException, IOException;

  /**
   * Generates code for a class given a {@link CodeGenerator}, and returns the
   * specified number of instances of the generated class. (Note that the name
   * is a misnomer, it would be better called
   * <tt>getImplementationInstances</tt>.)
   *
   * @param cg
   *          the code generator
   * @return list of instances of the generated class
   */

  <T> List<T> getImplementationClass(final CodeGenerator<T> cg, final int instanceCount)
      throws ClassTransformationException, IOException;

  /**
   * Determine if fragment execution has been interrupted.
   * @return true if execution should continue, false if an interruption has
   * occurred and fragment execution should halt
   */

  boolean shouldContinue();

  /**
   * Return the set of execution controls used to inject faults into running
   * code for testing.
   *
   * @return the execution controls
   */
  ExecutionControls getExecutionControls();

  /**
   * Returns the Drill configuration for this run. Note that the config is
   * global and immutable.
   *
   * @return the Drill configuration
   */

  DrillConfig getConfig();
}
