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
package org.apache.drill.exec.physical.impl.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.compile.CodeCompilerTestFactory;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.sys.local.LocalPStoreProvider;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import mockit.Injectable;
import mockit.NonStrictExpectations;


public class TestHashJoin extends PopUnitTestBase {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestMergeJoin.class);

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(100000);

  private final DrillConfig c = DrillConfig.create();

  private void testHJMockScanCommon(final DrillbitContext bitContext, UserServer.UserClientConnection connection, String physicalPlan, int expectedRows) throws Throwable {
    final LocalPStoreProvider provider = new LocalPStoreProvider(c);
    provider.start();
    final SystemOptionManager opt = new SystemOptionManager(PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(c), provider);
    opt.init();
    new NonStrictExpectations() {{
        bitContext.getMetrics(); result = new MetricRegistry();
        bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
        bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
        bitContext.getConfig(); result = c;
        bitContext.getOptionManager(); result = opt;
        bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
        bitContext.getLpPersistence(); result = new LogicalPlanPersistence(c, ClassPathScanner.fromPrescan(c));
    }};

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    final PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile(physicalPlan), Charsets.UTF_8));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    while (exec.next()) {
      totalRecordCount += exec.getRecordCount();
    }
    exec.close();
    assertEquals(expectedRows, totalRecordCount);
    System.out.println("Total Record Count: " + totalRecordCount);
    if (context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }

  @Test
  public void multiBatchEqualityJoin(@Injectable final DrillbitContext bitContext,
                                 @Injectable UserServer.UserClientConnection connection) throws Throwable {

    testHJMockScanCommon(bitContext, connection, "/join/hash_join_multi_batch.json", 200000);
  }

  @Test
  public void multiBatchRightOuterJoin(@Injectable final DrillbitContext bitContext,
                                       @Injectable UserServer.UserClientConnection connection) throws Throwable {

    testHJMockScanCommon(bitContext, connection, "/join/hj_right_outer_multi_batch.json", 100000);
  }

  @Test
  public void multiBatchLeftOuterJoin(@Injectable final DrillbitContext bitContext,
                                      @Injectable UserServer.UserClientConnection connection) throws Throwable {

    testHJMockScanCommon(bitContext, connection, "/join/hj_left_outer_multi_batch.json", 100000);
  }

  @Test
  public void simpleEqualityJoin() throws Throwable {
    // Function checks hash join with single equality condition
    try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
       Drillbit bit = new Drillbit(CONFIG, serviceSet);
       DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      // run query.
      bit.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/join/hash_join.json"), Charsets.UTF_8)
                      .replace("#{TEST_FILE_1}", FileUtils.getResourceAsFile("/build_side_input.json").toURI().toString())
                      .replace("#{TEST_FILE_2}", FileUtils.getResourceAsFile("/probe_side_input.json").toURI().toString()));

      RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

      QueryDataBatch batch = results.get(1);
      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

      Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

      // Just test the join key
      long colA[] = {1, 1, 2, 2, 1, 1};

      // Check the output of decimal9
      ValueVector.Accessor intAccessor1 = itr.next().getValueVector().getAccessor();


      for (int i = 0; i < intAccessor1.getValueCount(); i++) {
        assertEquals(intAccessor1.getObject(i), colA[i]);
      }
      assertEquals(6, intAccessor1.getValueCount());

      batchLoader.clear();
      for (QueryDataBatch result : results) {
        result.release();
      }
    }
  }

  @Test
  public void hjWithExchange(@Injectable final DrillbitContext bitContext,
                             @Injectable UserServer.UserClientConnection connection) throws Throwable {

    // Function tests with hash join with exchanges
    try (final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
      final Drillbit bit = new Drillbit(CONFIG, serviceSet);
      final DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      // run query.
      bit.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/join/hj_exchanges.json"), Charsets.UTF_8));

      int count = 0;
      for (final QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }

      System.out.println("Total records: " + count);
      assertEquals(25, count);
    }
  }

  @Test
  public void multipleConditionJoin(@Injectable final DrillbitContext bitContext,
                                    @Injectable UserServer.UserClientConnection connection) throws Throwable {

    // Function tests hash join with multiple join conditions
    try (final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
      final Drillbit bit = new Drillbit(CONFIG, serviceSet);
      final DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      // run query.
      bit.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/join/hj_multi_condition_join.json"), Charsets.UTF_8)
                      .replace("#{TEST_FILE_1}", FileUtils.getResourceAsFile("/build_side_input.json").toURI().toString())
                      .replace("#{TEST_FILE_2}", FileUtils.getResourceAsFile("/probe_side_input.json").toURI().toString()));

      final RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

      final QueryDataBatch batch = results.get(1);
      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

      final Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

      // Just test the join key
      final long colA[] = {1, 2, 1};
      final long colC[] = {100, 200, 500};

      // Check the output of decimal9
      final ValueVector.Accessor intAccessor1 = itr.next().getValueVector().getAccessor();
      final ValueVector.Accessor intAccessor2 = itr.next().getValueVector().getAccessor();


      for (int i = 0; i < intAccessor1.getValueCount(); i++) {
        assertEquals(intAccessor1.getObject(i), colA[i]);
        assertEquals(intAccessor2.getObject(i), colC[i]);
      }
      assertEquals(3, intAccessor1.getValueCount());

      batchLoader.clear();
      for (final QueryDataBatch result : results) {
        result.release();
      }
    }
  }

  @Test
  public void hjWithExchange1(@Injectable final DrillbitContext bitContext,
                              @Injectable UserServer.UserClientConnection connection) throws Throwable {

    // Another test for hash join with exchanges
    try (final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
         final Drillbit bit = new Drillbit(CONFIG, serviceSet);
         final DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator())) {

      // run query.
      bit.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile("/join/hj_exchanges1.json"), Charsets.UTF_8));

      int count = 0;
      for (final QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }

      System.out.println("Total records: " + count);
      assertEquals(272, count);
    }
  }

  @Test
  public void testHashJoinExprInCondition() throws Exception {
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (final Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        final DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/join/hashJoinExpr.json"), Charsets.UTF_8));
      int count = 0;
      for (final QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(10, count);
    }
  }
}
