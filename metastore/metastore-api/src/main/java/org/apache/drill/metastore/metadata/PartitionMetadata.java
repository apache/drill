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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a metadata for the table part, which corresponds to the specific partition key.
 */
public class PartitionMetadata extends BaseMetadata {
  private final SchemaPath column;
  private final List<String> partitionValues;
  private final Set<Path> locations;
  private final long lastModifiedTime;

  private PartitionMetadata(PartitionMetadataBuilder builder) {
    super(builder);
    this.column = builder.column;
    this.partitionValues = builder.partitionValues;
    this.locations = builder.locations;
    this.lastModifiedTime = builder.lastModifiedTime;
  }

  /**
   * It allows to obtain the column path for this partition
   *
   * @return column path
   */
  public SchemaPath getColumn() {
    return column;
  }

  /**
   * File locations for this partition
   *
   * @return file locations
   */
  public Set<Path> getLocations() {
    return locations;
  }

  /**
   * Allows to check the time, when any files were modified.
   * It is in Unix Timestamp, unit of measurement is millisecond.
   *
   * @return last modified time of files
   */
  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public List<String> getPartitionValues() {
    return partitionValues;
  }

  public static PartitionMetadataBuilder builder() {
    return new PartitionMetadataBuilder();
  }

  public static class PartitionMetadataBuilder extends BaseMetadataBuilder<PartitionMetadataBuilder> {
    private SchemaPath column;
    private List<String> partitionValues;
    private Set<Path> locations;
    private long lastModifiedTime = BaseTableMetadata.NON_DEFINED_LAST_MODIFIED_TIME;

    public PartitionMetadataBuilder locations(Set<Path> locations) {
      this.locations = locations;
      return self();
    }

    public PartitionMetadataBuilder lastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
      return self();
    }

    public PartitionMetadataBuilder partitionValues(List<String> partitionValues) {
      this.partitionValues = partitionValues;
      return self();
    }

    public PartitionMetadataBuilder column(SchemaPath column) {
      this.column = column;
      return self();
    }

    @Override
    protected void checkRequiredValues() {
      super.checkRequiredValues();
      Objects.requireNonNull(column, "column was not set");
      Objects.requireNonNull(partitionValues, "partitionValues were not set");
      Objects.requireNonNull(locations, "locations were not set");
    }

    @Override
    public PartitionMetadata build() {
      checkRequiredValues();
      return new PartitionMetadata(this);
    }

    @Override
    protected PartitionMetadataBuilder self() {
      return this;
    }
  }
}
