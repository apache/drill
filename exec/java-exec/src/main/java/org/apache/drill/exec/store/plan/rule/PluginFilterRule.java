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
package org.apache.drill.exec.store.plan.rule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.drill.exec.store.plan.PluginImplementor;
import org.apache.drill.exec.store.plan.rel.PluginFilterRel;

import java.util.Collections;

/**
 * The rule that converts provided filter operator to plugin-specific implementation.
 */
public class PluginFilterRule extends PluginConverterRule {

  public PluginFilterRule(RelTrait in, Convention out, PluginImplementor pluginImplementor) {
    super(Filter.class, in, out, "PluginFilterRule", pluginImplementor);
  }

  @Override
  public RelNode convert(RelNode rel) {
    Filter filter = (Filter) rel;
    PluginFilterRel pluginFilterRel = new PluginFilterRel(
      getOutConvention(),
      rel.getCluster(),
      filter.getTraitSet().replace(getOutConvention()),
      convert(filter.getInput(), filter.getTraitSet().replace(getOutConvention())),
      filter.getCondition());
    if (getPluginImplementor().artificialFilter()) {
      return filter.copy(filter.getTraitSet(), Collections.singletonList(pluginFilterRel));
    }
    return pluginFilterRel;
  }
}
