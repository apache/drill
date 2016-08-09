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

import org.apache.drill.exec.planner.fragment.DistributionAffinity;
import org.apache.drill.exec.planner.physical.ExchangePrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.ScreenPrel;
import org.apache.drill.exec.planner.physical.UnionAllPrel;
import org.apache.calcite.rel.RelNode;

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
    Prel newChild = ((Prel) prel.getInput()).accept(this, newFrag);

    if (newFrag.isSingular() && parent.isSingular() &&
        // if one of them has strict distribution or none, we can remove the exchange
        (!newFrag.isDistributionStrict() || !parent.isDistributionStrict())
        ) {
      return newChild;
    } else {
      return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList((RelNode) newChild));
    }
  }

  @Override
  public Prel visitScreen(ScreenPrel prel, MajorFragmentStat s) throws RuntimeException {
    s.addScreen(prel);
    RelNode child = ((Prel)prel.getInput()).accept(this, s);
    return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList(child));
  }

  @Override
  public Prel visitScan(ScanPrel prel, MajorFragmentStat s) throws RuntimeException {
    s.addScan(prel);
    return prel;
  }

  /**
   * A union-all should be treated differently compared to a join operator because joins impose
   * a co-location requirement and therefore insert an exchange on both sides of the
   * join (e.g HashToRandomExchange or BroadcastExchange), thus the major fragment of the join itself
   * is different from the major fragment of its children.  Union-All does not impose the co-location
   * requirement on its children, hence the major fragment of the union-all may be the same as that of
   * its children. Thus, we should take an 'aggregate' view of all its children to decide the parallelism.
   */
  @Override
  public Prel visitUnionAll(UnionAllPrel prel, MajorFragmentStat s) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    s.add(prel);

    List<MajorFragmentStat> statList = Lists.newArrayList();
    for (Prel p : prel) {
      // for each input of union-all, create a temporary MajorFragmentStat instance
      MajorFragmentStat childStat = new MajorFragmentStat(s /* use existing stat to initialize */);
      statList.add(childStat);
      childStat.add(p);
    }

    int i = 0;
    for(Prel p : prel) {
      children.add(p.accept(this, statList.get(i++)));
    }

    MajorFragmentStat maxStat = statList.get(0);
    // get the max width of all child stats
    for (int j=1; j < statList.size(); j++) {
      if (statList.get(j).getMaxWidth() > maxStat.getMaxWidth()) {
        maxStat = statList.get(j);
      }
    }

    // width of the major fragment that contains union-all should be the maximum
    // width of all its inputs
    s.setMaxWidth(maxStat.getMaxWidth());

    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitPrel(Prel prel, MajorFragmentStat s) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    s.add(prel);

    // Add all children to MajorFragmentStat, before we visit each child.
    // Since MajorFramentStat keeps track of maxRows of Prels in MajorFrag, it's fine to add prel multiple times.
    // Doing this will ensure MajorFragmentStat is same when visit each individual child, in order to make
    // consistent decision.
    for (Prel p : prel) {
      s.add(p);
    }

    for(Prel p : prel) {
      children.add(p.accept(this, s));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  public MajorFragmentStat getNewStat() {
    return new MajorFragmentStat();
  }

  class MajorFragmentStat {
    private DistributionAffinity distributionAffinity = DistributionAffinity.NONE;
    private double maxRows = 0d;
    private int maxWidth = Integer.MAX_VALUE;
    private boolean isMultiSubScan = false;

    public MajorFragmentStat() {
    }

    public MajorFragmentStat(MajorFragmentStat that) {
      this.distributionAffinity = that.distributionAffinity;
      this.maxRows = that.maxRows;
      this.maxWidth = that.maxWidth;
      this.isMultiSubScan = that.isMultiSubScan;
    }

    public void add(Prel prel) {
      maxRows = Math.max(prel.getRows(), maxRows);
    }

    public void addScreen(ScreenPrel screenPrel) {
      maxWidth = 1;
      distributionAffinity = screenPrel.getDistributionAffinity();
    }

    public void addScan(ScanPrel prel) {
      maxWidth = Math.min(maxWidth, prel.getGroupScan().getMaxParallelizationWidth());
      isMultiSubScan = prel.getGroupScan().getMinParallelizationWidth() > 1;
      distributionAffinity = prel.getDistributionAffinity();
      add(prel);
    }

    public int getMaxWidth() {
      return maxWidth;
    }

    public void setMaxWidth(int w) {
      maxWidth = w;
    }

    public boolean isSingular() {
      // do not remove exchanges when a scan has more than one subscans (e.g. SystemTableScan)
      if (isMultiSubScan) {
        return false;
      }

      int suggestedWidth = (int) Math.ceil((maxRows+1)/targetSliceSize);

      int w = Math.min(maxWidth, suggestedWidth);
      if (w < 1) {
        w = 1;
      }
      return w == 1;
    }

    public boolean isDistributionStrict() {
      return distributionAffinity == DistributionAffinity.HARD;
    }
  }

}
