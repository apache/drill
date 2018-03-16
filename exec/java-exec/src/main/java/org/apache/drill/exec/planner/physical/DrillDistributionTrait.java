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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

public class DrillDistributionTrait implements RelTrait {
  public static enum DistributionType {SINGLETON, HASH_DISTRIBUTED, RANGE_DISTRIBUTED, RANDOM_DISTRIBUTED,
                                       ROUND_ROBIN_DISTRIBUTED, BROADCAST_DISTRIBUTED, ANY};

  public static DrillDistributionTrait SINGLETON = new DrillDistributionTrait(DistributionType.SINGLETON);
  public static DrillDistributionTrait RANDOM_DISTRIBUTED = new DrillDistributionTrait(DistributionType.RANDOM_DISTRIBUTED);
  public static DrillDistributionTrait ANY = new DrillDistributionTrait(DistributionType.ANY);

  public static DrillDistributionTrait DEFAULT = ANY;

  private DistributionType type;
  private final ImmutableList<DistributionField> fields;
  private PartitionFunction partitionFunction = null;

  public DrillDistributionTrait(DistributionType type) {
    assert (type == DistributionType.SINGLETON || type == DistributionType.RANDOM_DISTRIBUTED || type == DistributionType.ANY
            || type == DistributionType.ROUND_ROBIN_DISTRIBUTED || type == DistributionType.BROADCAST_DISTRIBUTED);
    this.type = type;
    this.fields = ImmutableList.<DistributionField>of();
  }

  public DrillDistributionTrait(DistributionType type, ImmutableList<DistributionField> fields) {
    assert (type == DistributionType.HASH_DISTRIBUTED || type == DistributionType.RANGE_DISTRIBUTED);
    this.type = type;
    this.fields = fields;
  }

  public DrillDistributionTrait(DistributionType type, ImmutableList<DistributionField> fields,
      PartitionFunction partitionFunction) {
    assert (type == DistributionType.HASH_DISTRIBUTED || type == DistributionType.RANGE_DISTRIBUTED);
    this.type = type;
    this.fields = fields;
    this.partitionFunction = partitionFunction;
  }

  @Override
  public void register(RelOptPlanner planner) {
  }

  @Override
  public boolean satisfies(RelTrait trait) {

    if (trait instanceof DrillDistributionTrait) {
      DistributionType requiredDist = ((DrillDistributionTrait) trait).getType();
      if (requiredDist == DistributionType.ANY) {
        return true;
      }

      if (this.type == DistributionType.HASH_DISTRIBUTED) {
        if (requiredDist == DistributionType.HASH_DISTRIBUTED) {
          // A subset of the required distribution columns can satisfy (subsume) the requirement
          // e.g: required distribution: {a, b, c}
          // Following can satisfy the requirements: {a}, {b}, {c}, {a, b}, {b, c}, {a, c} or {a, b, c}

          // New: Use equals for subsumes check of hash distribution. If we uses subsumes,
          // a join may end up with hash-distributions using different keys. This would
          // cause incorrect query result.
          return this.equals(trait);
        }
        else if (requiredDist == DistributionType.RANDOM_DISTRIBUTED) {
          return true; // hash distribution subsumes random distribution and ANY distribution
        }
      }

      if(this.type == DistributionType.RANGE_DISTRIBUTED) {
        if (requiredDist == DistributionType.RANDOM_DISTRIBUTED) {
          return true; // RANGE_DISTRIBUTED distribution subsumes random distribution and ANY distribution
        }
      }
    }

    return this.equals(trait);
  }

  public RelTraitDef<DrillDistributionTrait> getTraitDef() {
    return DrillDistributionTraitDef.INSTANCE;
  }

  public DistributionType getType() {
    return this.type;
  }

  public ImmutableList<DistributionField> getFields() {
    return fields;
  }

  public PartitionFunction getPartitionFunction() {
    return partitionFunction;
  }

  private boolean arePartitionFunctionsSame(PartitionFunction f1, PartitionFunction f2) {
    if (f1 != null && f2 != null) {
      return f1.equals(f2);
    } else if (f2 == null && f2 == null) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return  fields == null? type.hashCode(): type.hashCode() | fields.hashCode() << 4;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DrillDistributionTrait) {
      DrillDistributionTrait that = (DrillDistributionTrait) obj;
      return this.type == that.type && this.fields.equals(that.fields) &&
          arePartitionFunctionsSame(this.partitionFunction, that.partitionFunction);
    }
    return false;
  }

  @Override
  public String toString() {
    return fields == null ? this.type.toString() : this.type.toString() + "(" + fields + ")";
  }


  public static class DistributionField {
    /**
     * 0-based index of field being DISTRIBUTED.
     */
    private final int fieldId;

    public DistributionField (int fieldId) {
      this.fieldId = fieldId;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DistributionField)) {
        return false;
      }
      DistributionField other = (DistributionField) obj;
      return this.fieldId == other.fieldId;
    }

    @Override
    public int hashCode() {
      return this.fieldId;
    }

    public int getFieldId() {
      return this.fieldId;
    }

    @Override
    public String toString() {
      return String.format("[$%s]", this.fieldId);
    }
  }

}
