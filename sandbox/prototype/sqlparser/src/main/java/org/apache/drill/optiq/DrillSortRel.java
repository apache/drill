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
package org.apache.drill.optiq;

import java.util.List;

import net.hydromatic.linq4j.Ord;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Sort implemented in Drill.
 */
public class DrillSortRel extends SortRel implements DrillRel {
  /** Creates a DrillSortRel. */
  public DrillSortRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation) {
    super(cluster, traits, input, collation);
  }

  @Override
  public DrillSortRel copy(RelTraitSet traitSet, RelNode input, RelCollation collation) {
    return new DrillSortRel(getCluster(), traitSet, input, collation);
  }

  @Override
  public int implement(DrillImplementor implementor) {
    int inputId = implementor.visitChild(this, 0, getChild());
    final List<String> childFields = getChild().getRowType().getFieldNames();
    /*
     * E.g. { op: "order", input: 4, ordering: [ {order: "asc", expr: "deptId"} ] }
     */
    final ObjectNode order = implementor.mapper.createObjectNode();
    order.put("op", "order");
    order.put("input", inputId);
    final ArrayNode orderings = implementor.mapper.createArrayNode();
    order.put("orderings", orderings);
    for (Ord<RelFieldCollation> fieldCollation : Ord.zip(this.collation.getFieldCollations())) {
      final ObjectNode ordering = implementor.mapper.createObjectNode();
      ordering.put("order", toDrill(fieldCollation.e));
      ordering.put("expr", childFields.get(fieldCollation.e.getFieldIndex()));
      switch (fieldCollation.e.nullDirection) {
      case FIRST:
        ordering.put("nullCollation", "first");
        break;
      default:
        ordering.put("nullCollation", "last");
        break;
      }
      orderings.add(ordering);
    }

    return implementor.add(order);
  }

  private static String toDrill(RelFieldCollation collation) {
    switch (collation.getDirection()) {
    case Ascending:
      return "asc";
    case Descending:
      return "desc";
    default:
      throw new AssertionError(collation.getDirection());
    }
  }
}
