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
package org.apache.drill.exec.physical.impl.window;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestWindowFrame extends PopUnitTestBase {

  @Test
  public void testWindowFrameWithOneKeyCount() throws Throwable {
    try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
         Drillbit bit = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      // run query.
      bit.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserBitShared.QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile("/window/oneKeyCount.json"), Charsets.UTF_8)
              .replace("#{DATA_FILE}", FileUtils.getResourceAsFile("/window/oneKeyCountData.json").toURI().toString())
      );

      long[] cntArr = {1, 2, 1, 2};
      long[] sumArr = {100, 150, 25, 75};

      // look at records
      RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());
      int recordCount = 0;

      assertEquals(2, results.size());

      QueryResultBatch batch = results.get(0);
      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));
      batchLoader.load(batch.getHeader().getDef(), batch.getData());

      for (int r = 0; r < batchLoader.getRecordCount(); r++) {
        recordCount++;
        VectorWrapper<?> wrapper = batchLoader.getValueAccessorById(
            BigIntVector.class,
            batchLoader.getValueVectorId(new SchemaPath(new PathSegment.NameSegment("cnt"))).getFieldIds()[0]
        );
        assertEquals(cntArr[r], wrapper.getValueVector().getAccessor().getObject(r));
        wrapper = batchLoader.getValueAccessorById(
            NullableBigIntVector.class,
            batchLoader.getValueVectorId(new SchemaPath(new PathSegment.NameSegment("sum"))).getFieldIds()[0]
        );
        assertEquals(sumArr[r], wrapper.getValueVector().getAccessor().getObject(r));
      }
      batchLoader.clear();
      batch.release();

      assertEquals(4, recordCount);
    }
  }

  @Test
  public void testWindowFrameWithOneKeyMultipleBatches() throws Throwable {
    try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
         Drillbit bit = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      // run query.
      bit.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserBitShared.QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile("/window/oneKeyCountMultiBatch.json"), Charsets.UTF_8)
              .replace("#{DATA_FILE}", FileUtils.getResourceAsFile("/window/mediumData.json").toURI().toString()));

      // look at records
      RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());
      int recordCount = 0;

      assertEquals(2, results.size());

      QueryResultBatch batch = results.get(0);
      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));
      batchLoader.load(batch.getHeader().getDef(), batch.getData());
      ValueVector.Accessor output = batchLoader.getValueAccessorById(NullableBigIntVector.class,
          batchLoader.getValueVectorId(
              new SchemaPath(new PathSegment.NameSegment("output"))).getFieldIds()[0]
      ).getValueVector().getAccessor();
      ValueVector.Accessor sum = batchLoader.getValueAccessorById(
          BigIntVector.class,
          batchLoader.getValueVectorId(
              new SchemaPath(new PathSegment.NameSegment("sum"))).getFieldIds()[0]
      ).getValueVector().getAccessor();
      ValueVector.Accessor cnt = batchLoader.getValueAccessorById(
          BigIntVector.class,
          batchLoader.getValueVectorId(
              new SchemaPath(new PathSegment.NameSegment("cnt"))).getFieldIds()[0]
      ).getValueVector().getAccessor();
      int lastGroup = -1;
      long groupCounter = 0;
      long s = 0;
      for (int r = 1; r <= batchLoader.getRecordCount(); r++) {
        recordCount++;
        int group = r / 4;
        if(lastGroup != group) {
          lastGroup = group;
          groupCounter = 1;
          s = 0;
        } else {
          groupCounter++;
        }

        s += group * 8 + r % 4;

        assertEquals("Count, Row " + r, groupCounter, cnt.getObject(r - 1));
        assertEquals("Sum, Row " + r, s, sum.getObject(r - 1));
        assertEquals("Output, Row " + r, s, output.getObject(r - 1));
      }
      batchLoader.clear();
      batch.release();

      assertEquals(1000, recordCount);
    }
  }

  @Test
  public void testWindowFrameWithTwoKeys() throws Throwable {
    try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
         Drillbit bit = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      // run query.
      bit.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserBitShared.QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile("/window/twoKeys.json"), Charsets.UTF_8)
              .replace("#{DATA_FILE}", FileUtils.getResourceAsFile("/window/twoKeysData.json").toURI().toString())
      );

      long[] cntArr = {1, 2, 1, 2, 1, 2, 1, 2};
      long[] sumArr = {5, 15, 15, 35, 25, 55, 35, 75};

      // look at records
      RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());
      int recordCount = 0;

      assertEquals(2, results.size());

      QueryResultBatch batch = results.get(0);
      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));
      batchLoader.load(batch.getHeader().getDef(), batch.getData());

      for (int r = 0; r < batchLoader.getRecordCount(); r++) {
        recordCount++;
        VectorWrapper<?> wrapper = batchLoader.getValueAccessorById(
            BigIntVector.class,
            batchLoader.getValueVectorId(new SchemaPath(new PathSegment.NameSegment("cnt"))).getFieldIds()[0]
        );
        assertEquals(cntArr[r], wrapper.getValueVector().getAccessor().getObject(r));
        wrapper = batchLoader.getValueAccessorById(
            NullableBigIntVector.class,
            batchLoader.getValueVectorId(new SchemaPath(new PathSegment.NameSegment("sum"))).getFieldIds()[0]
        );
        assertEquals(sumArr[r], wrapper.getValueVector().getAccessor().getObject(r));
      }
      batchLoader.clear();
      batch.release();

      assertEquals(8, recordCount);
    }
  }
}
