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
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.ParquetFileReader;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

public class TestParquetReaderUtility {

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
  public void testSchemaElementsMap() {
    assertTrue(footer != null);
    Map<String, SchemaElement> schemaElements = ParquetReaderUtility.getColNameToSchemaElementMapping(footer);
    assertTrue(schemaElements.size() == 14);

    SchemaElement se = schemaElements.get("`marketing_info`.`camp_id`");
    assertTrue(se != null);
    assertTrue("camp_id".equals(se.getName()));

    se = schemaElements.get("`marketing_info`");
    assertTrue(se != null);
    assertTrue("marketing_info".equals(se.getName()));
  }

  @Test
  public void testColumnDescriptorMap() {
    assertTrue(footer != null);
    Map<String, ColumnDescriptor> colDescMap = ParquetReaderUtility.getColNameToColumnDescriptorMapping(footer);
    assertTrue(colDescMap.size() == 11);

    ColumnDescriptor cd = colDescMap.get("`marketing_info`.`camp_id`");
    assertTrue(cd != null);

    cd = colDescMap.get("`marketing_info`");
    assertTrue(cd == null);
  }
}
