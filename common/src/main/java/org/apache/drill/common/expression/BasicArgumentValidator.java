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

import java.util.List;

import org.apache.drill.common.types.TypeProtos.MajorType;

public class BasicArgumentValidator implements ArgumentValidator {

  private final Arg[] args;

  public BasicArgumentValidator(MajorType... types) {
    this.args = new Arg[] { new Arg("single", types) };
  }

  public BasicArgumentValidator(Arg... args) {
    this.args = args;
  }

  @Override
  public void validateArguments(ExpressionPosition expr, List<LogicalExpression> expressions, ErrorCollector errors) {
    if (expressions.size() != args.length) errors.addUnexpectedArgumentCount(expr, expressions.size(), args.length);

    int i = 0;
    for (LogicalExpression e : expressions) {
      args[i].confirmDataType(expr, i, e, errors);

      i++;
    }
  }

  public Arg arg(String name, MajorType... allowedTypes) {
    return new Arg(name, allowedTypes);
  }

  @Override
  public String[] getArgumentNamesByPosition() {
    String[] names = new String[args.length];
    for(int i =0; i < names.length; i++){
      names[i] = args[i].getName();
    }
    return names;
  }

  
}
