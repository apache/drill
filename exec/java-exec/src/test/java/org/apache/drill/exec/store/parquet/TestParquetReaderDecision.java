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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.parquet.AbstractParquetScanBatchCreator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.ParquetFileReader;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * DRILL-5797 introduces more granularity on new reader use cases. This test is aimed at
 * checking correctness of function used for new reader usage decision making.
 */
public class TestParquetReaderDecision {

  private static final String path = "src/test/resources/store/parquet/complex/complex.parquet";
  private static Configuration conf;
  private static ParquetMetadata footer;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();

    try {
      footer = ParquetFileReader.readFooter(conf, new Path(path));
    } catch (IOException ioe) {
      fail("Could not read Parquet file '" + path + "', error message: " + ioe.getMessage()
          + " cwd: " + Paths.get(".").toAbsolutePath().normalize().toString());
      throw(ioe);
    }
  }

  @Test
  public void testParquetReaderDecision() {
    List<SchemaPath> caseOldReader1 = new ArrayList<>();
    List<SchemaPath> caseOldReader2 = new ArrayList<>();
    List<SchemaPath> caseOldReader3 = new ArrayList<>();
    List<SchemaPath> caseNewReader1 = new ArrayList<>();
    List<SchemaPath> caseNewReader2 = new ArrayList<>();
    List<SchemaPath> caseNewReader3 = new ArrayList<>();

    SchemaPath topNestedPath = SchemaPath.getCompoundPath("marketing_info");
    SchemaPath nestedColumnPath = SchemaPath.getCompoundPath("marketing_info", "camp_id");
    SchemaPath topPath1 = SchemaPath.getCompoundPath("trans_id");
    SchemaPath topPath2 = SchemaPath.getCompoundPath("amount");
    SchemaPath nonExistentPath = SchemaPath.getCompoundPath("nonexistent");

    caseOldReader1.add(topNestedPath);
    caseOldReader2.add(nestedColumnPath);
    caseOldReader3.add(topPath1);
    caseOldReader3.add(nestedColumnPath);

    caseNewReader1.add(topPath1);
    caseNewReader2.add(topPath1);
    caseNewReader2.add(topPath2);

    assertTrue("Complex column not detected",
        AbstractParquetScanBatchCreator.containsComplexColumn(footer, caseOldReader1));
    assertTrue("Complex column not detected",
        AbstractParquetScanBatchCreator.containsComplexColumn(footer, caseOldReader2));
    assertTrue("Complex column not detected",
        AbstractParquetScanBatchCreator.containsComplexColumn(footer, caseOldReader3));

    assertFalse("No complex column, but complex column is detected",
        AbstractParquetScanBatchCreator.containsComplexColumn(footer, caseNewReader1));
    assertFalse("No complex column, but complex column is detected",
        AbstractParquetScanBatchCreator.containsComplexColumn(footer, caseNewReader2));
    assertFalse("No complex column, but complex column is detected",
        AbstractParquetScanBatchCreator.containsComplexColumn(footer, caseNewReader3));
  }
}
