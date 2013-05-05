/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.common.expression;

import org.apache.drill.common.expression.types.DataType;

import com.google.common.collect.Range;

public interface ErrorCollector {

    public void addGeneralError(String expr, String s);

    public void addUnexpectedArgumentType(String expr, String name, DataType actual, DataType[] expected, int argumentIndex);

    public void addUnexpectedArgumentCount(String expr, int actual, Range<Integer> expected);

    public void addUnexpectedArgumentCount(String expr, int actual, int expected);

    public void addNonNumericType(String expr, DataType actual);

    public void addUnexpectedType(String expr, int index, DataType actual);

    public void addExpectedConstantValue(String expr, int actual, String s);

    boolean hasErrors();

    String toErrorString();
}
