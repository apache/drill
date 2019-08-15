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
package org.apache.drill.exec.physical.impl.scan;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataColumnDefn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager;
import org.apache.drill.exec.physical.impl.scan.file.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ScanFrameworkBuilder;
import org.apache.drill.exec.physical.impl.scan.project.ReaderLevelProjection.ReaderProjectionResolver;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.ScanSchemaOrchestrator.ScanOrchestratorBuilder;
import org.apache.drill.exec.physical.resultSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.test.OperatorFixture;

public class ScanTestUtils {

  // Default file metadata column names; primarily for testing.

  public static final String FILE_NAME_COL = "filename";
  public static final String FULLY_QUALIFIED_NAME_COL = "fqn";
  public static final String FILE_PATH_COL = "filepath";
  public static final String SUFFIX_COL = "suffix";
  public static final String PARTITION_COL = "dir";

  public static abstract class ScanFixtureBuilder {

    public final OperatorFixture opFixture;
    // All tests are designed to use the schema batch
    public boolean enableSchemaBatch = true;

    public ScanFixtureBuilder(OperatorFixture opFixture) {
      this.opFixture = opFixture;
    }

    public abstract ScanFrameworkBuilder builder();

    public void projectAll() {
      builder().setProjection(RowSetTestUtils.projectAll());
    }

    public void projectAllWithMetadata(int dirs) {
      builder().setProjection(ScanTestUtils.projectAllWithMetadata(dirs));
    }

    public void setProjection(String... projCols) {
      builder().setProjection(RowSetTestUtils.projectList(projCols));
    }

    public void setProjection(List<SchemaPath> projection) {
      builder().setProjection(projection);
    }

    public ScanFixture build() {
      builder().enableSchemaBatch(enableSchemaBatch);
      ScanOperatorExec scanOp = builder().buildScan();
      Scan scanConfig = new AbstractSubScan("bob") {

        @Override
        public int getOperatorType() {
          return 0;
        }
      };
      OperatorContext opContext = opFixture.newOperatorContext(scanConfig);
      scanOp.bind(opContext);
      return new ScanFixture(opContext, scanOp);
    }
  }

  public static class ScanFixture {

    private OperatorContext opContext;
    public ScanOperatorExec scanOp;

    public ScanFixture(OperatorContext opContext, ScanOperatorExec scanOp) {
      this.opContext = opContext;
      this.scanOp = scanOp;
    }

    public void close() {
      try {
        scanOp.close();
      } finally {
        opContext.close();
      }
    }
  }

  public static class MockScanBuilder extends ScanOrchestratorBuilder {

    @Override
    public ScanOperatorEvents buildEvents() {
      throw new IllegalStateException("Not used in this test.");
    }

  }

  /**
   * Type-safe way to define a list of parsers.
   * @param parsers as a varArgs list convenient for testing
   * @return parsers as a Java List for input to the scan
   * projection framework
   */

  public static List<ScanProjectionParser> parsers(ScanProjectionParser... parsers) {
    return ImmutableList.copyOf(parsers);
  }

  public static List<ReaderProjectionResolver> resolvers(ReaderProjectionResolver... resolvers) {
    return ImmutableList.copyOf(resolvers);
  }

  /**
   * Mimic legacy wildcard expansion of metadata columns. Is not a full
   * emulation because this version only works if the wildcard was at the end
   * of the list (or alone.)
   * @param scanProj scan projection definition (provides the partition column names)
   * @param base the table part of the expansion
   * @param dirCount number of partition directories
   * @return schema with the metadata columns appended to the table columns
   */

  public static TupleMetadata expandMetadata(TupleMetadata base, FileMetadataManager metadataProj, int dirCount) {
    TupleMetadata metadataSchema = new TupleSchema();
    for (ColumnMetadata col : base) {
      metadataSchema.addColumn(col);
    }
    for (FileMetadataColumnDefn fileColDefn : metadataProj.fileMetadataColDefns()) {
      metadataSchema.add(MaterializedField.create(fileColDefn.colName(), fileColDefn.dataType()));
    }
    for (int i = 0; i < dirCount; i++) {
      metadataSchema.add(MaterializedField.create(metadataProj.partitionName(i),
          PartitionColumn.dataType()));
    }
    return metadataSchema;
  }

  public static String partitionColName(int partition) {
    return PARTITION_COL + partition;
  }

  public static TupleMetadata schema(ResolvedTuple output) {
    final TupleMetadata schema = new TupleSchema();
    for (final ResolvedColumn col : output.columns()) {
      MaterializedField field = col.schema();
      if (field.getType() == null) {

        // Convert from internal format of null columns (unset type)
        // to a usable form (explicit minor type of NULL.)

        field = MaterializedField.create(field.getName(),
            Types.optional(MinorType.NULL));
      }
      schema.add(field);
    }
    return schema;
  }

  public static List<SchemaPath> expandMetadata(int dirCount) {
    List<String> selected = Lists.newArrayList(
        FULLY_QUALIFIED_NAME_COL,
        FILE_PATH_COL,
        FILE_NAME_COL,
        SUFFIX_COL);

    for (int i = 0; i < dirCount; i++) {
      selected.add(PARTITION_COL + Integer.toString(i));
    }
    return RowSetTestUtils.projectList(selected);
  }

  public static List<SchemaPath> projectAllWithMetadata(int dirCount) {
    return RowSetTestUtils.concat(
        RowSetTestUtils.projectAll(),
        expandMetadata(dirCount));
  }
}
