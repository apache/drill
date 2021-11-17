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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;

public class TestParquetWriterConfig extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    dirTestWatcher.copyResourceToRoot(Paths.get("parquet", "users"));
    startCluster(builder);
  }

  @Test
  public void testFormatConfigOpts() throws Exception {
    cluster.defineFormat("dfs", "parquet", new ParquetFormatConfig(
      false,
      true,
      123,
      456,
      true,
      "snappy",
      "binary",
      true,
      "PARQUET_V_2"
      )
    );
    client.alterSession(ExecConstants.OUTPUT_FORMAT_OPTION, "parquet_test");
    String query = "select * from dfs.`parquet/users`";
    queryBuilder()
      .sql(query)
      .jsonPlanMatcher()
      .include("\"autoCorrectCorruptDates\" : false")
      .include("\"enableStringsSignedMinMax\" : true")
      .include("\"blockSize\" : 123")
      .include("\"pageSize\" : 456")
      .include("\"useSingleFSBlock\" : true")
      .include("\"writerCompressionType\" : \"snappy\"")
      .include("\"writerUsePrimitivesForDecimals\" : true")
      .include("\"writerFormatVersion\" : \"PARQUET_V_2\"")
      .match();
  }
}
