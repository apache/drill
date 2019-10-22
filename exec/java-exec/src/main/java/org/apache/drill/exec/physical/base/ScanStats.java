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
package org.apache.drill.exec.physical.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ScanStats {

  public static final ScanStats TRIVIAL_TABLE = new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, 20, 1, 1);

  public static final ScanStats ZERO_RECORD_TABLE = new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, 0, 1, 1);

  @JsonProperty
  private final GroupScanProperty groupScanProperty;
  @JsonProperty
  private final double recordCount;
  @JsonProperty
  private final double cpuCost;
  @JsonProperty
  private final double diskCost;

  @JsonCreator
  public ScanStats(@JsonProperty("groupScanProperty") GroupScanProperty groupScanProperty,
                   @JsonProperty("recordCount") double recordCount,
                   @JsonProperty("cpuCost") double cpuCost,
                   @JsonProperty("diskCost") double diskCost) {
    this.groupScanProperty = groupScanProperty;
    this.recordCount = recordCount;
    this.cpuCost = cpuCost;
    this.diskCost = diskCost;
  }

  /**
   * Return if GroupScan knows the exact row count in the result of getSize() call.
   * By default, group scan does not know the exact row count, before it scans every rows.
   * Currently, parquet group scan will return the exact row count.
   *
   * @return group scan property
   */
  public GroupScanProperty getGroupScanProperty() {
    return groupScanProperty;
  }

  public double getRecordCount() {
    return recordCount;
  }

  public double getCpuCost() {
    return cpuCost;
  }

  public double getDiskCost() {
    return diskCost;
  }

  @Override
  public String toString() {
    return "ScanStats{" +
      "recordCount=" + recordCount +
      ", cpuCost=" + cpuCost +
      ", diskCost=" + diskCost +
      ", groupScanProperty=" + groupScanProperty +
      '}';
  }

  public enum GroupScanProperty {
    NO_EXACT_ROW_COUNT(false, false),
    EXACT_ROW_COUNT(true, true);

    private boolean hasExactRowCount, hasExactColumnValueCount;

    GroupScanProperty (boolean hasExactRowCount, boolean hasExactColumnValueCount) {
      this.hasExactRowCount = hasExactRowCount;
      this.hasExactColumnValueCount = hasExactColumnValueCount;
    }

    public boolean hasExactRowCount() {
      return hasExactRowCount;
    }

    public boolean hasExactColumnValueCount() {
      return hasExactColumnValueCount;
    }
  }
}
