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
package org.apache.drill.exec.store.direct;

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.store.RecordReader;

import java.util.Collection;
import java.util.List;

/**
 * Represents direct scan based on metadata information.
 * For example, for parquet files it can be obtained from parquet footer (total row count)
 * or from parquet metadata files (column counts).
 * Contains reader, statistics and list of scanned files if present.
 */
@JsonTypeName("metadata-direct-scan")
public class MetadataDirectGroupScan extends DirectGroupScan {

  private final Collection<String> files;

  public MetadataDirectGroupScan(RecordReader reader, Collection<String> files) {
    super(reader);
    this.files = files;
  }

  public MetadataDirectGroupScan(RecordReader reader, Collection<String> files, ScanStats stats) {
    super(reader, stats);
    this.files = files;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return new MetadataDirectGroupScan(reader, files, stats);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return this;
  }

  /**
   * <p>
   * Returns string representation of group scan data.
   * Includes list of files if present.
   * </p>
   *
   * <p>
   * Example: [files = [/tmp/0_0_0.parquet], numFiles = 1]
   * </p>
   *
   * @return string representation of group scan data
   */
  @Override
  public String getDigest() {
    if (files != null) {
      StringBuilder builder = new StringBuilder();
      builder.append("files = ").append(files).append(", ");
      builder.append("numFiles = ").append(files.size()).append(", ");
      return builder.append(super.getDigest()).toString();
    }
    return super.getDigest();
  }

}
