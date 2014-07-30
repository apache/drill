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
package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.planner.common.DrillWriterRelBase;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.relopt.volcano.RelSubset;

public class WriterPrule extends Prule{
  public static final RelOptRule INSTANCE = new WriterPrule();

  public WriterPrule() {
    super(RelOptHelper.some(DrillWriterRel.class, DrillRel.DRILL_LOGICAL, RelOptHelper.any(RelNode.class)),
        "Prel.WriterPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillWriterRelBase writer = call.rel(0);
    final RelNode input = call.rel(1);

    final RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    final RelNode convertedInput = convert(input, traits);

    if (!new WriteTraitPull(call).go(writer, convertedInput)) {
      DrillWriterRelBase newWriter = new WriterPrel(writer.getCluster(), convertedInput.getTraitSet(),
          convertedInput, writer.getCreateTableEntry());

      call.transformTo(newWriter);
    }
  }

  private class WriteTraitPull extends SubsetTransformer<DrillWriterRelBase, RuntimeException> {

    public WriteTraitPull(RelOptRuleCall call) {
      super(call);
    }

    @Override
    public RelNode convertChild(DrillWriterRelBase writer, RelNode rel) throws RuntimeException {
      DrillDistributionTrait childDist = rel.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE);
      RelCollation childCollation = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

      return new WriterPrel(writer.getCluster(),
          writer.getTraitSet().plus(childDist).plus(childCollation).plus(Prel.DRILL_PHYSICAL),
          rel, writer.getCreateTableEntry());
    }

  }
}
