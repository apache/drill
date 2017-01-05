/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import io.indexr.segment.helper.RangeWork;
import io.indexr.segment.rc.RCOperator;

public class IndexRSubScanSpec {
  @JsonProperty("scanId")
  public final String scanId;
  @JsonProperty("tableName")
  public final String tableName;
  @JsonProperty("scanCount")
  public final Integer scanCount;
  @JsonProperty("scanIndex")
  public final Integer scanIndex;
  @JsonProperty("endpointWorks")
  public final List<RangeWork> endpointWorks;
  @JsonProperty("rsFilter")
  public final RCOperator rsFilter;

  @JsonCreator
  public IndexRSubScanSpec(@JsonProperty("scanId") String scanId, //
                           @JsonProperty("tableName") String tableName, //
                           @JsonProperty("scanCount") Integer scanCount, //
                           @JsonProperty("scanIndex") Integer scanIndex,//
                           @JsonProperty("endpointWorks") List<RangeWork> endpointWorks,//
                           @JsonProperty("rsFilter") RCOperator rsFilter) {
    this.scanId = scanId;
    this.tableName = tableName;
    this.scanCount = scanCount;
    this.scanIndex = scanIndex;
    this.endpointWorks = endpointWorks;
    this.rsFilter = rsFilter;
  }

  @Override
  public String toString() {
    return "IndexRSubScanSpec{" +
        "scanId='" + scanId + '\'' +
        ", tableName='" + tableName + '\'' +
        ", scanCount=" + scanCount +
        ", scanIndex=" + scanIndex +
        ", endpointWorks=" + endpointWorks +
        ", rsFilter=" + rsFilter +
        '}';
  }
}
