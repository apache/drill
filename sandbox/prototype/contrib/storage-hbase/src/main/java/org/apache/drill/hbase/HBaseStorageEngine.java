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
package org.apache.drill.hbase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfigBase;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.drill.exec.ref.rse.RSEBase;
import org.apache.drill.exec.ref.rse.RecordReader;
import org.apache.drill.exec.ref.rse.RecordRecorder;
import org.apache.drill.hbase.table.HBaseTableRecordReader;
import org.apache.drill.hbase.table.HBaseTableScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Collection;

/**
 * An implementation of ReferenceStorageEngine for HBase.
 */
public class HBaseStorageEngine extends RSEBase {

  @JsonTypeName("hbase")
  public static class HBaseStorageEngineConfig extends StorageEngineConfigBase {

    @JsonCreator
    public HBaseStorageEngineConfig(@JsonProperty("name") String name) {
      super(name);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof HBaseStorageEngineConfig)) return false;
      HBaseStorageEngineConfig that = (HBaseStorageEngineConfig) o;
      if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return getName() != null ? getName().hashCode() : 0;
    }
  }

  public static class HBaseStorageEngineInputConfig {

    public String table;

  }

  private Configuration config;

  public HBaseStorageEngine(HBaseStorageEngineConfig engineConfig, DrillConfig config) {
    this.config = HBaseConfiguration.create();
  }

  /**
   * Allows to override HBase config. Mostly for testing purposes.
   */
  public void setHBaseConfiguration(Configuration config) {
    this.config = config;
  }


  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public Collection<ReadEntry> getReadEntries(org.apache.drill.common.logical.data.Scan scan) throws IOException {
    HBaseStorageEngineInputConfig engine = scan.getSelection().getWith(HBaseStorageEngineInputConfig.class);
    return ImmutableSet.<ReadEntry>of(new HBaseTableScanner(new HTable(config, engine.table), new Scan()));
  }

  @Override
  public RecordReader getReader(ReadEntry readEntry, ROP parentROP) throws IOException {
    HBaseTableScanner entry = getReadEntry(HBaseTableScanner.class, readEntry);
    return new HBaseTableRecordReader(entry, parentROP);
  }

  @Override
  public RecordRecorder getWriter(Store store) throws IOException {
    throw new UnsupportedOperationException();
  }
}
