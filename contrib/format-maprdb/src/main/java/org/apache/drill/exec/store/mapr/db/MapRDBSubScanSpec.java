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
package org.apache.drill.exec.store.mapr.db;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mapr.fs.jni.MapRConstants;
import com.mapr.org.apache.hadoop.hbase.util.Bytes;

public class MapRDBSubScanSpec {

  protected String tableName;
  protected String regionServer;
  protected byte[] startRow;
  protected byte[] stopRow;
  protected byte[] serializedFilter;

  @JsonCreator
  public MapRDBSubScanSpec(@JsonProperty("tableName") String tableName,
                           @JsonProperty("regionServer") String regionServer,
                           @JsonProperty("startRow") byte[] startRow,
                           @JsonProperty("stopRow") byte[] stopRow,
                           @JsonProperty("serializedFilter") byte[] serializedFilter,
                           @JsonProperty("filterString") String filterString) {
    if (serializedFilter != null && filterString != null) {
      throw new IllegalArgumentException("The parameters 'serializedFilter' or 'filterString' cannot be specified at the same time.");
    }
    this.tableName = tableName;
    this.regionServer = regionServer;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.serializedFilter = serializedFilter;
  }

  /* package */ MapRDBSubScanSpec() {
    // empty constructor, to be used with builder pattern;
  }

  public String getTableName() {
    return tableName;
  }

  public MapRDBSubScanSpec setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public String getRegionServer() {
    return regionServer;
  }

  public MapRDBSubScanSpec setRegionServer(String regionServer) {
    this.regionServer = regionServer;
    return this;
  }

  /**
   * @return the raw (not-encoded) start row key for this sub-scan
   */
  public byte[] getStartRow() {
    return startRow == null ? MapRConstants.EMPTY_BYTE_ARRAY: startRow;
  }

  public MapRDBSubScanSpec setStartRow(byte[] startRow) {
    this.startRow = startRow;
    return this;
  }

  /**
   * @return the raw (not-encoded) stop row key for this sub-scan
   */
  public byte[] getStopRow() {
    return stopRow == null ? MapRConstants.EMPTY_BYTE_ARRAY : stopRow;
  }

  public MapRDBSubScanSpec setStopRow(byte[] stopRow) {
    this.stopRow = stopRow;
    return this;
  }

  public byte[] getSerializedFilter() {
    return serializedFilter;
  }

  public MapRDBSubScanSpec setSerializedFilter(byte[] serializedFilter) {
    this.serializedFilter = serializedFilter;
    return this;
  }

  @Override
  public String toString() {
    return "MapRDBSubScanSpec [tableName=" + tableName
        + ", startRow=" + (startRow == null ? null : Bytes.toStringBinary(startRow))
        + ", stopRow=" + (stopRow == null ? null : Bytes.toStringBinary(stopRow))
        + ", filter=" + (getSerializedFilter() == null ? null : Bytes.toBase64(getSerializedFilter()))
        + ", regionServer=" + regionServer + "]";
  }

}
