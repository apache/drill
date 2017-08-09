/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.drill.exec.planner.cost;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.drill.exec.planner.physical.AbstractPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;

/**
 * DrillRelMdMaxRowCount supplies a specific implementation of
 * {@link RelMetadataQuery#getMaxRowCount} for Drill.
 */
public class DrillRelMdMaxRowCount extends RelMdMaxRowCount {

  private static final DrillRelMdMaxRowCount INSTANCE = new DrillRelMdMaxRowCount();

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.MAX_ROW_COUNT.method, INSTANCE);

  public Double getMaxRowCount(ScanPrel rel, RelMetadataQuery mq) {
    // the actual row count is known so returns its value
    return rel.estimateRowCount(mq);
  }

  public Double getMaxRowCount(SingleRel rel, RelMetadataQuery mq) {
    return mq.getMaxRowCount(rel.getInput());
  }

  public Double getMaxRowCount(AbstractPrel rel, RelMetadataQuery mq) {
    return rel.getMaxRowCount();
  }

  public Double getMaxRowCount(RelSubset rel, RelMetadataQuery mq) {
    Double maxRowCount;
    // calculates and returns getMaxRowCount() for the best known plan
    if (rel.getBest() != null) {
      maxRowCount = mq.getMaxRowCount(rel.getBest());
    } else {
      maxRowCount = mq.getMaxRowCount(rel.getOriginal());
    }
    if (maxRowCount == null) {
      return Double.POSITIVE_INFINITY;
    }
    return maxRowCount;
  }

  public Double getMaxRowCount(TableScan rel, RelMetadataQuery mq) {
    // the actual row count is known so returns its value
    return rel.estimateRowCount(mq);
  }
}
