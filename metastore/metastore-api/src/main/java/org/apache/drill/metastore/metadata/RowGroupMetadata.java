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
package org.apache.drill.metastore.metadata;

import org.apache.hadoop.fs.Path;

import java.util.Map;
import java.util.Objects;

/**
 * Metadata which corresponds to the row group level of table.
 */
public class RowGroupMetadata extends BaseMetadata implements LocationProvider {
  private Map<String, Float> hostAffinity;
  private int rowGroupIndex;
  private Path path;

  private RowGroupMetadata(RowGroupMetadataBuilder builder) {
    super(builder);
    this.hostAffinity = builder.hostAffinity;
    this.rowGroupIndex = builder.rowGroupIndex;
    this.path = builder.path;
  }

  @Override
  public Path getPath() {
    return path;
  }

  public Path getLocation() {
    return path.getParent();
  }

  /**
   * Returns index of current row group within its file.
   *
   * @return row group index
   */
  public int getRowGroupIndex() {
    return rowGroupIndex;
  }

  /**
   * Returns the host affinity for a row group.
   *
   * @return host affinity for the row group
   */
  public Map<String, Float> getHostAffinity() {
    return hostAffinity;
  }

  public static RowGroupMetadataBuilder builder() {
    return new RowGroupMetadataBuilder();
  }

  public static class RowGroupMetadataBuilder extends BaseMetadataBuilder<RowGroupMetadataBuilder> {
    private Map<String, Float> hostAffinity;
    private int rowGroupIndex;
    private Path path;

    public RowGroupMetadataBuilder hostAffinity(Map<String, Float> hostAffinity) {
      this.hostAffinity = hostAffinity;
      return self();
    }

    public RowGroupMetadataBuilder rowGroupIndex(int rowGroupIndex) {
      this.rowGroupIndex = rowGroupIndex;
      return self();
    }

    public RowGroupMetadataBuilder path(Path path) {
      this.path = path;
      return self();
    }

    @Override
    protected void checkRequiredValues() {
      super.checkRequiredValues();
      Objects.requireNonNull(path, "path was not set");
    }

    @Override
    public RowGroupMetadata build() {
      checkRequiredValues();
      return new RowGroupMetadata(this);
    }

    @Override
    protected RowGroupMetadataBuilder self() {
      return this;
    }
  }
}
