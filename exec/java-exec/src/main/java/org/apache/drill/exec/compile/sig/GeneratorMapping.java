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
package org.apache.drill.exec.compile.sig;

import org.apache.drill.exec.expr.ClassGenerator.BlockType;

import com.google.common.base.Preconditions;

public class GeneratorMapping {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GeneratorMapping.class);

  private final String setup;
  private final String eval;
  private final String reset;
  private final String cleanup;

  public GeneratorMapping(final String setup, final String eval, final String reset, final String cleanup) {
    super();
    this.setup = setup;
    this.eval = eval;
    this.reset = reset;
    this.cleanup = cleanup;
  }

  public GeneratorMapping(final GeneratorMapping gm) {
    super();
    this.setup = gm.setup;
    this.eval = gm.eval;
    this.reset = gm.reset;
    this.cleanup = gm.cleanup;
  }

  public static GeneratorMapping GM(final String setup, final String eval) {
    return create(setup, eval, null, null);
  }

  public static GeneratorMapping GM(
      final String setup, final String eval, final String reset, final String cleanup) {
    return create(setup, eval, reset, cleanup);
  }

  public static GeneratorMapping create(
      final String setup, final String eval, final String reset, final String cleanup) {
    return new GeneratorMapping(setup, eval, reset, cleanup);
  }

  public String getMethodName(final BlockType type) {
    switch(type) {
    case CLEANUP:
      Preconditions.checkNotNull(cleanup, "The current mapping does not have a cleanup method defined.");
      return cleanup;
    case EVAL:
      Preconditions.checkNotNull(eval, "The current mapping does not have an eval method defined.");
      return eval;
    case RESET:
      Preconditions.checkNotNull(reset, "The current mapping does not have a reset method defined.");
      return reset;
    case SETUP:
      Preconditions.checkNotNull(setup, "The current mapping does not have a setup method defined.");
      return setup;
    default:
      throw new IllegalStateException();
    }
  }
}
