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
package org.apache.drill.exec.planner.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.ModifyTableEntry;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.record.BatchSchema;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TableModifyPrel extends TableModify implements Prel {

  protected TableModifyPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
    Prepare.CatalogReader catalogReader, RelNode input, Operation operation,
    List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened) {
    super(cluster, traitSet, table, catalogReader, input, operation, updateColumnList,
      sourceExpressionList, flattened);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TableModifyPrel(getCluster(), traitSet, getTable(), getCatalogReader(),
      inputs.get(0), getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened());
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    List<String> tablePath = getTable().getQualifiedName();
    List<String> schemaPath = tablePath.size() > 1
      ? tablePath.subList(0, tablePath.size() - 1)
      : Collections.emptyList();
    String tableName = tablePath.get(tablePath.size() - 1);
    SchemaPlus schema = ((CalciteCatalogReader) getTable().getRelOptSchema()).getRootSchema().plus();

    ModifyTableEntry modifyTableEntry = SchemaUtilites.resolveToDrillSchema(schema, schemaPath)
      .modifyTable(tableName);

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    PhysicalOperator p = modifyTableEntry.getWriter(childPOP);
    return creator.addMetadata(this, p);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitTableModify(this, value);
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.DEFAULT;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }

  @NotNull
  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);
    double inputRowCount = mq.getRowCount(getInput());
    double dIo = inputRowCount + 1; // ensure non-zero cost
    return planner.getCostFactory().makeCost(rowCount, 0, dIo).multiplyBy(10);
  }
}
