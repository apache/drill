/**
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
package org.apache.drill.exec.expr.fn.interpreter;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.DrillSimpleFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionConverter;
import org.apache.drill.exec.server.options.SystemOptionManager;

import java.util.Set;

public class InterpreterBuilder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InterpreterBuilder.class);


  public static void main(String args[])  {
    InterpreterBuilder builder = new InterpreterBuilder();
    if (args.length != 1) {
      System.err.println("Usage: InterpreterBuilder  targetSrcDir\n");
      System.exit(-1);
    }
    builder.build(DrillConfig.create(), args[0]);
  }

  private void build(DrillConfig config, String targetSrcDir) {
    FunctionConverter converter = new FunctionConverter();
    Set<Class<? extends DrillFunc>> providerClasses = PathScanner.scanForImplementations(DrillFunc.class, config.getStringList(ExecConstants.FUNCTION_PACKAGES));

    int count = 0;
    for (Class<? extends DrillFunc> clazz : providerClasses) {
      try {
        DrillFuncHolder holder = converter.getHolder(clazz);

        if (holder != null && holder instanceof DrillSimpleFuncHolder) {
          InterpreterGenerator generator = new InterpreterGenerator((DrillSimpleFuncHolder)holder, clazz.getSimpleName() + InterpreterGenerator.INTERPRETER_CLASSNAME_POSTFIX, targetSrcDir);
          generator.build();
          count ++;
        }
      } catch (Exception ex) {
        failure("Failure while creating function interpreter.", ex, clazz);
      }
    }

    logger.debug("Total interpreter class generated : " + count);
  }

  private void failure(String message, Throwable t, Class<?> clazz) {
    logger.error("Failure loading function class [{}]. Message: {}", clazz.getName(), message, t);
  }
}