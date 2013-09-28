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
package org.apache.drill.optiq;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.drill.common.logical.data.Limit;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;

import java.util.List;

public class DrillLimitRel extends SingleRel implements DrillRel {
  private RexNode offset;
  private RexNode fetch;

  public DrillLimitRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, child);
    this.offset = offset;
    this.fetch = fetch;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillLimitRel(getCluster(), traitSet, sole(inputs), offset, fetch);
  }

  @Override
  public int implement(DrillImplementor implementor) {
    int inputId = implementor.visitChild(this, 0, getChild());


    // First offset to include into results (inclusive). Null implies it is starting from offset 0
    int first = offset != null ? Math.max(0, RexLiteral.intValue(offset)) : 0;

    // Last offset to stop including into results (exclusive), translating fetch row counts into an offset.
    // Null value implies including entire remaining result set from first offset
    Integer last = fetch != null ? Math.max(0, RexLiteral.intValue(fetch)) + first : null;
    return implementor.add(new Limit(first, last), inputId);
  }

}
