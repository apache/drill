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
package org.apache.drill.exec.planner.sql.handlers;

import java.util.Collection;
import java.util.Map.Entry;

import org.apache.calcite.tools.RuleSet;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.planner.sql.SqlConverter;
import org.apache.drill.exec.store.StoragePlugin;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class SqlHandlerConfig {

  private final QueryContext context;
  private final SqlConverter converter;

  public SqlHandlerConfig(QueryContext context, SqlConverter converter) {
    super();
    this.context = context;
    this.converter = converter;
  }

  public QueryContext getContext() {
    return context;
  }

  public RuleSet getRules(PlannerPhase phase) {
    Collection<StoragePlugin> plugins = Lists.newArrayList();
    for (Entry<String, StoragePlugin> k : context.getStorage()) {
      plugins.add(k.getValue());
    }
    return phase.getRules(context, plugins);
  }

  public SqlConverter getConverter() {
    return converter;
  }
}
