/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.Lists;
import org.apache.drill.exec.planner.physical.ExchangePrel;
import org.apache.drill.exec.planner.physical.HashToRandomExchangePrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.UnorderedDeMuxExchangePrel;
import org.apache.drill.exec.planner.physical.UnorderedMuxExchangePrel;
import org.apache.drill.exec.server.options.OptionManager;
import org.eigenbase.rel.RelNode;

import java.util.Collections;
import java.util.List;

public class InsertLocalExchangeVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {

  private final boolean isMuxEnabled;
  private final boolean isDeMuxEnabled;

  public static Prel insertLocalExchanges(Prel prel, OptionManager options) {
    boolean isMuxEnabled = options.getOption(PlannerSettings.MUX_EXCHANGE.getOptionName()).bool_val;
    boolean isDeMuxEnabled = options.getOption(PlannerSettings.DEMUX_EXCHANGE.getOptionName()).bool_val;

    if (isMuxEnabled || isDeMuxEnabled) {
      return prel.accept(new InsertLocalExchangeVisitor(isMuxEnabled, isDeMuxEnabled), null);
    }

    return prel;
  }

  public InsertLocalExchangeVisitor(boolean isMuxEnabled, boolean isDeMuxEnabled) {
    this.isMuxEnabled = isMuxEnabled;
    this.isDeMuxEnabled = isDeMuxEnabled;
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, Void value) throws RuntimeException {
    Prel child = ((Prel)prel.getChild()).accept(this, null);
    // Whenever we encounter a HashToRandomExchangePrel:
    //   If MuxExchange is enabled, insert a UnorderedMuxExchangePrel before HashToRandomExchangePrel.
    //   If DeMuxExchange is enabled, insert a UnorderedDeMuxExchangePrel after HashToRandomExchangePrel.
    if (prel instanceof HashToRandomExchangePrel) {
      Prel newPrel = child;
      if (isMuxEnabled) {
        newPrel = new UnorderedMuxExchangePrel(prel.getCluster(), prel.getTraitSet(), child);
      }

      newPrel = new HashToRandomExchangePrel(prel.getCluster(),
          prel.getTraitSet(), newPrel, ((HashToRandomExchangePrel) prel).getFields());

      if (isDeMuxEnabled) {
        HashToRandomExchangePrel hashExchangePrel = (HashToRandomExchangePrel) newPrel;
        // Insert a DeMuxExchange to narrow down the number of receivers
        newPrel = new UnorderedDeMuxExchangePrel(prel.getCluster(), prel.getTraitSet(), hashExchangePrel,
            hashExchangePrel.getFields());
      }

      return newPrel;
    }

    return (Prel)prel.copy(prel.getTraitSet(), Collections.singletonList(((RelNode)child)));
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }
}
