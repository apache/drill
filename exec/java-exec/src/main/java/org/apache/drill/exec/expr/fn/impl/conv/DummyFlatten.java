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
package org.apache.drill.exec.expr.fn.impl.conv;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.RepeatedMapHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

/**
 * This class merely acts as a placeholder so that Calcite allows the 'flatten()' function in SQL.
 *
 * Calcite 1.35+ requires function signatures to match during validation, so we define
 * the expected parameter here. The actual flatten operation is performed by the
 * FlattenRecordBatch at execution time.
 */
@FunctionTemplate(name = "flatten", scope = FunctionScope.SIMPLE)
public class DummyFlatten implements DrillSimpleFunc {

  @Param RepeatedMapHolder in;
  @Output BaseWriter.ComplexWriter out;

  @Override
  public void setup() { }

  @Override
  public void eval() { }
}
