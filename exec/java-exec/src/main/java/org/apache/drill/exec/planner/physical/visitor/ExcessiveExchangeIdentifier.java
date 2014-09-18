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
package org.apache.drill.exec.planner.physical.visitor;

import java.util.Collections;
import java.util.List;

import org.apache.drill.exec.planner.physical.ExchangePrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.ScreenPrel;
import org.eigenbase.rel.RelNode;

import com.google.common.collect.Lists;

public class ExcessiveExchangeIdentifier extends BasePrelVisitor<Prel, ExcessiveExchangeIdentifier.MajorFragmentStat, RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcessiveExchangeIdentifier.class);

  private final long targetSliceSize;

  public ExcessiveExchangeIdentifier(long targetSliceSize) {
    this.targetSliceSize = targetSliceSize;
  }

  public static Prel removeExcessiveEchanges(Prel prel, long targetSliceSize) {
    ExcessiveExchangeIdentifier exchange = new ExcessiveExchangeIdentifier(targetSliceSize);
    return prel.accept(exchange, exchange.getNewStat());
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, MajorFragmentStat parent) throws RuntimeException {
    parent.add(prel);
    MajorFragmentStat newFrag = new MajorFragmentStat();
    Prel newChild = ((Prel) prel.getChild()).accept(this, newFrag);

    if (newFrag.isSingular() && parent.isSingular()) {
      return newChild;
    } else {
      return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList((RelNode) newChild));
    }
  }

  @Override
  public Prel visitScreen(ScreenPrel prel, MajorFragmentStat s) throws RuntimeException {
    s.setSingular();
    RelNode child = ((Prel)prel.getChild()).accept(this, s);
    return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList(child));
  }

  @Override
  public Prel visitScan(ScanPrel prel, MajorFragmentStat s) throws RuntimeException {
    s.addScan(prel);
    return prel;
  }

  @Override
  public Prel visitPrel(Prel prel, MajorFragmentStat s) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    s.add(prel);
    for(Prel p : prel) {
      children.add(p.accept(this, s));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  public MajorFragmentStat getNewStat() {
    return new MajorFragmentStat();
  }

  class MajorFragmentStat {
    private double maxRows = 0d;
    private int maxWidth = Integer.MAX_VALUE;

    public void add(Prel prel) {
      maxRows = Math.max(prel.getRows(), maxRows);
    }

    public void setSingular() {
      maxWidth = 1;
    }

    public void addScan(ScanPrel prel) {
      maxWidth = Math.min(maxWidth, prel.getGroupScan().getMaxParallelizationWidth());
      add(prel);
    }

    public boolean isSingular() {
      int suggestedWidth = (int) Math.ceil((maxRows+1)/targetSliceSize);

      int w = Math.min(maxWidth, suggestedWidth);
      if (w < 1) {
        w = 1;
      }
      return w == 1;
    }
  }

}
