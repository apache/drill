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

/**
 * Metadata which corresponds to the segment level of table.
 */
public class SegmentMetadata extends BaseMetadata implements LocationProvider {
  private final SchemaPath column;
  private final Path path;
  private final List<String> partitionValues;
  private final List<Path> locations;
  private final long lastModifiedTime;

  private SegmentMetadata(SegmentMetadataBuilder builder) {
    super(builder);
    this.column = builder.column;
    this.path = builder.path;
    this.partitionValues = builder.partitionValues;
    this.locations = builder.locations;
    this.lastModifiedTime = builder.lastModifiedTime;
  }

  public SchemaPath getSegmentColumn() {
    return column;
  }

  public Path getPath() {
    return path;
  }

  public List<String> getPartitionValues() {
    return partitionValues;
  }

  public List<Path> getLocations() {
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

  public static SegmentMetadataBuilder builder() {
    return new SegmentMetadataBuilder();
  }

  public static class SegmentMetadataBuilder extends BaseMetadataBuilder<SegmentMetadataBuilder> {
    private SchemaPath column;
    private List<String> partitionValues;
    private Path path;
    private List<Path> locations;
    private long lastModifiedTime = BaseTableMetadata.NON_DEFINED_LAST_MODIFIED_TIME;

    public SegmentMetadataBuilder locations(List<Path> locations) {
      this.locations = locations;
      return self();
    }

    public SegmentMetadataBuilder path(Path path) {
      this.path = path;
      return self();
    }

    public SegmentMetadataBuilder lastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
      return self();
    }

    public SegmentMetadataBuilder partitionValues(List<String> partitionValues) {
      this.partitionValues = partitionValues;
      return self();
    }

    public SegmentMetadataBuilder column(SchemaPath column) {
      this.column = column;
      return self();
    }

    @Override
    protected void checkRequiredValues() {
      super.checkRequiredValues();
      Objects.requireNonNull(column, "column was not set");
      Objects.requireNonNull(partitionValues, "partitionValues were not set");
      Objects.requireNonNull(locations, "locations were not set");
      Objects.requireNonNull(path, "path was not set");
    }

    @Override
    public SegmentMetadata build() {
      checkRequiredValues();
      return new SegmentMetadata(this);
    }

    @Override
    protected SegmentMetadataBuilder self() {
      return this;
    }
  }
}
