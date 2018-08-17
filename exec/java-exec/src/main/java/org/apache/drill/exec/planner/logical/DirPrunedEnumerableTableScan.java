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
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.schema.Table;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * This class extends from EnumerableTableScan. It puts the file selection string into it's digest.
 * When directory-based partition pruning applied, file selection could be different for the same
 * table.
 */
public class DirPrunedEnumerableTableScan extends EnumerableTableScan {
  private final String digestFromSelection;

  public DirPrunedEnumerableTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, Class elementType, String digestFromSelection) {
    super(cluster, traitSet, table, elementType);
    this.digestFromSelection = digestFromSelection;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    final Table tbl = this.table.unwrap(Table.class);
    Class elementType = EnumerableTableScan.deduceElementType(tbl);

    return new DirPrunedEnumerableTableScan(getCluster(), traitSet, table, elementType, digestFromSelection);
  }

  /** Creates an DirPrunedEnumerableTableScan. */
  public static EnumerableTableScan create(RelOptCluster cluster,
      RelOptTable relOptTable, String digestFromSelection) {
    Table table = relOptTable.unwrap(Table.class);
    Class elementType = EnumerableTableScan.deduceElementType(table);
    List<RelCollation> relCollations = Optional.ofNullable(table)
        .map(t -> t.getStatistic().getCollations())
        .orElse(Collections.emptyList());
    RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, () -> relCollations);
    return new DirPrunedEnumerableTableScan(cluster, traitSet, relOptTable, elementType, digestFromSelection);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("selection", this.digestFromSelection);
  }

}
