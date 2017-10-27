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
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.io.IOException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.ops.OperExecContext;

/**
 * Base class for code-generation-based tasks.
 */

public abstract class BaseWrapper {

  protected OperExecContext context;

  public BaseWrapper(OperExecContext context) {
    this.context = context;
  }

  protected <T> T getInstance(CodeGenerator<T> cg, org.slf4j.Logger logger) {
    try {
      return context.getImplementationClass(cg);
    } catch (ClassTransformationException e) {
      throw UserException.unsupportedError(e)
            .message("Code generation error - likely code error.")
            .build(logger);
    } catch (IOException e) {
      throw UserException.resourceError(e)
            .message("IO Error during code generation.")
            .build(logger);
    }
  }

}
