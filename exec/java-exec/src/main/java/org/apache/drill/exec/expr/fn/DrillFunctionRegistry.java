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
package org.apache.drill.exec.expr.fn;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.planner.sql.DrillSqlAggOperator;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;
import org.eigenbase.sql.SqlOperator;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;

public class DrillFunctionRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFunctionRegistry.class);

  private ArrayListMultimap<String, DrillFuncHolder> methods = ArrayListMultimap.create();

  public DrillFunctionRegistry(DrillConfig config) {
    FunctionConverter converter = new FunctionConverter();
    Set<Class<? extends DrillFunc>> providerClasses = PathScanner.scanForImplementations(DrillFunc.class, config.getStringList(ExecConstants.FUNCTION_PACKAGES));
    for (Class<? extends DrillFunc> clazz : providerClasses) {
      DrillFuncHolder holder = converter.getHolder(clazz);
      if (holder != null) {
        // register handle for each name the function can be referred to
        String[] names = holder.getRegisteredNames();
        for (String name : names) {
          methods.put(name.toLowerCase(), holder);
        }
      } else {
        logger.warn("Unable to initialize function for class {}", clazz.getName());
      }
    }
    if (logger.isTraceEnabled()) {
      StringBuilder allFunctions = new StringBuilder();
      for (DrillFuncHolder method: methods.values()) {
        allFunctions.append(method.toString()).append("\n");
      }
      logger.trace("Registered functions: [\n{}]", allFunctions);
    }
  }

  /** Returns functions with given name. Function name is case insensitive. */
  public List<DrillFuncHolder> getMethods(String name) {
    return this.methods.get(name.toLowerCase());
  }

  public void register(DrillOperatorTable operatorTable) {
    SqlOperator op;
    for (Entry<String, Collection<DrillFuncHolder>> function : methods.asMap().entrySet()) {
      Set<Integer> argCounts = Sets.newHashSet();
      String name = function.getKey().toUpperCase();
      for (DrillFuncHolder f : function.getValue()) {
        if (argCounts.add(f.getParamCount())) {
          if (f.isAggregating()) {
            op = new DrillSqlAggOperator(name, f.getParamCount());
          } else {
            op = new DrillSqlOperator(name, f.getParamCount());
          }
          operatorTable.add(function.getKey(), op);
        }
      }
    }
  }

}
