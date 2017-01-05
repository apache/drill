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

import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;

import io.indexr.segment.helper.RangeWork;

public class ScanCompleteWork extends RangeWork implements CompleteWork {
  @JsonIgnore
  public long totalBytes;
  @JsonIgnore
  public EndpointByteMap byteMap;

  public ScanCompleteWork(@JsonProperty("segmentName") String segmentName, //
                          @JsonProperty("startPackId") int startPackId, //
                          @JsonProperty("endPackId") int endPackId, //
                          @JsonProperty("totalBytes") long totalBytes, //
                          @JsonProperty("byteMap") EndpointByteMap byteMap) {
    super(segmentName, startPackId, endPackId);
    this.totalBytes = totalBytes;
    this.byteMap = byteMap;
  }

  @JsonIgnore
  @Override
  public long getTotalBytes() {
    return totalBytes;
  }

  @JsonIgnore
  @Override
  public EndpointByteMap getByteMap() {
    return byteMap;
  }

  @JsonIgnore
  @Override
  public int compareTo(CompleteWork o) {
    return Long.compare(getTotalBytes(), o.getTotalBytes());
  }

  @JsonIgnore
  @Override
  public String toString() {
    String byteMapStr = "";
    for (ObjectLongCursor<CoordinationProtos.DrillbitEndpoint> cursor : byteMap) {
      byteMapStr += cursor.key.getAddress() + ",";
    }
    return "ScanCompleteWork{" +
        "segment=" + segment +
        ", startPackId=" + startPackId +
        ", endPackId=" + endPackId +
        ", totalBytes=" + totalBytes +
        ", hosts:=" + byteMapStr +
        '}';
  }
}
