/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.fn.output;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.fn.FunctionAttributes;

import java.util.List;

/**
 * Return type calculation interface for functions that have return type set as with enum
 * {@link org.apache.drill.exec.expr.annotations.FunctionTemplate.ReturnType}.
 */
public interface ReturnTypeInference {

  TypeProtos.MajorType getType(List<LogicalExpression> logicalExpressions, FunctionAttributes attributes);

}
