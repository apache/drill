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
package org.apache.drill.exec.store.text;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import mockit.Injectable;
import mockit.NonStrictExpectations;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TextRecordReaderTest extends PopUnitTestBase {

  @Test
  public void testFullExecution() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
              Files.toString(
                      FileUtils.getResourceAsFile("/store/text/test.json"), Charsets.UTF_8)
                      .replace("#{DATA_FILE}", FileUtils.getResourceAsFile("/store/text/data/regions.csv").toURI().toString()));
      int count = 0;
      RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for(QueryResultBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        loader.load(b.getHeader().getDef(), b.getData());
        VectorUtil.showVectorAccessibleContent(loader);
      }
      assertEquals(5, count);
    }
  }

}
