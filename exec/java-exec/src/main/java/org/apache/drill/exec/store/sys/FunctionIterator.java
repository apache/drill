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
package org.apache.drill.exec.store.sys;

import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.ops.FragmentContext;

import java.util.Arrays;
import java.util.Iterator;

/**
 *
 */
public class FunctionIterator implements Iterator<Object> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionIterator.class);

  private final Iterator<DrillFuncHolder> functions;

  public FunctionIterator(FragmentContext context) {
    this.functions = context.getFunctionRegistry().getAllMethods().iterator();
  }

  public static class FunctionInfo {
    public final String names;
    public final String returnType;
    public final String nullHandling;
    public final boolean isDeterministic;
    public final boolean isAggregating;

    public FunctionInfo(String names, String returnType, String nullHandling, boolean isDeterministic,
                        boolean isAggregating) {
      this.names = names;
      this.returnType = returnType;
      this.nullHandling = nullHandling;
      this.isDeterministic = isDeterministic;
      this.isAggregating = isAggregating;
    }
  }

  @Override
  public boolean hasNext() {
    return functions.hasNext();
  }

  @Override
  public Object next() {
    final DrillFuncHolder funcHolder = functions.next();
    String name = Arrays.toString(funcHolder.getRegisteredNames());
//    if (name.length() > 20) {
//      name = name.substring(0, 20) + "... ]";
//    }
    return new FunctionInfo(name, Types.toString(funcHolder.getReturnType()), funcHolder.getNullHandling().toString(),
        funcHolder.isDeterministic(), funcHolder.isAggregating());
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
