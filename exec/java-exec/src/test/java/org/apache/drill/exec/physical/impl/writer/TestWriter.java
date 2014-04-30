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
package org.apache.drill.exec.physical.impl.writer;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestWriter extends BaseTestQuery {

  @Test
  public void testSimpleCsv() throws Exception {
    // before executing the test deleting the existing CSV files in /tmp/csvtest
    Configuration conf = new Configuration();
    conf.set("fs.name.default", "local");

    FileSystem fs = FileSystem.get(conf);
    Path path = new Path("/tmp/csvtest");
    if (fs.exists(path)) {
      fs.delete(path, true);
    }

    String plan = Files.toString(FileUtils.getResourceAsFile("/writer/simple_csv_writer.json"), Charsets.UTF_8);

    List<QueryResultBatch> results = testPhysicalWithResults(plan);

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());

    QueryResultBatch batch = results.get(0);
    assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

    VarCharVector fragmentIdV = (VarCharVector) batchLoader.getValueAccessorById(0, VarCharVector.class).getValueVector();
    BigIntVector recordWrittenV = (BigIntVector) batchLoader.getValueAccessorById(1, BigIntVector.class).getValueVector();

    // expected only one row in output
    assertEquals(1, batchLoader.getRecordCount());

    assertEquals("0_0", fragmentIdV.getAccessor().getObject(0).toString());
    assertEquals(132000, recordWrittenV.getAccessor().get(0));

    // now verify csv files are written to disk
    assertTrue(fs.exists(path));

    // expect two files
    FileStatus[] fileStatuses = fs.globStatus(new Path(path.toString(), "*.csv"));
    assertTrue(2 == fileStatuses.length);

    for(QueryResultBatch b : results){
      b.release();
    }
    batchLoader.clear();
  }
}
