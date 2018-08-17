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
package org.apache.drill.exec.physical.impl.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.FragmentContextImpl;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.physical.impl.aggregate.HashAggBatch;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.UserClientConnection;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistryImpl;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.categories.SlowTest;
import org.junit.Ignore;
import org.junit.Test;

import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({SlowTest.class, OperatorTest.class})
public class TestMergeJoin extends PopUnitTestBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggBatch.class);
  private final DrillConfig c = DrillConfig.create();

  @Test
  @Ignore
  // this doesn't have a sort.  it also causes an infinite loop.  these may or may not be related.
  public void simpleEqualityJoin() throws Throwable {
    DrillbitContext bitContext = mockDrillbitContext();
    UserClientConnection connection = Mockito.mock(UserClientConnection.class);

    PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    PhysicalPlan plan = reader.readPhysicalPlan(DrillFileUtils.getResourceAsString("/join/merge_join.json"));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContextImpl context = new FragmentContextImpl(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    StringBuilder sb = new StringBuilder();
    while (exec.next()) {
      totalRecordCount += exec.getRecordCount();
      for (ValueVector v : exec) {
        sb.append("[" + v.getField().getName() + "]        ");
      }
      sb.append("\n\n");
      for (int valueIdx = 0; valueIdx < exec.getRecordCount(); valueIdx++) {
        List<Object> row = new ArrayList<>();
        for (ValueVector v : exec) {
          row.add(v.getAccessor().getObject(valueIdx));
        }
        for (Object cell : row) {
          if (cell == null) {
            sb.append("<null>          ");
            continue;
          }
          int len = cell.toString().length();
          sb.append(cell);
          for (int i = 0; i < (14 - len); ++i) {
            sb.append(" ");
          }
        }
        sb.append("\n");
      }
      sb.append("\n");
    }

    logger.info(sb.toString());
    assertEquals(100, totalRecordCount);
    if (context.getExecutorState().getFailureCause() != null) {
      throw context.getExecutorState().getFailureCause();
    }
    assertTrue(!context.getExecutorState().isFailed());
  }

  @Test
  @Ignore
  public void orderedEqualityLeftJoin() throws Throwable {
    DrillbitContext bitContext = mockDrillbitContext();
    UserClientConnection connection = Mockito.mock(UserClientConnection.class);

    PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c,
      new StoragePluginRegistryImpl(bitContext));
    PhysicalPlan plan = reader.readPhysicalPlan(
      DrillFileUtils.getResourceAsString("/join/merge_single_batch.json")
        .replace("#{LEFT_FILE}", DrillFileUtils.getResourceAsFile("/join/merge_single_batch.left.json").toURI().toString())
        .replace("#{RIGHT_FILE}", DrillFileUtils.getResourceAsFile("/join/merge_single_batch.right.json").toURI().toString()));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContextImpl context = new FragmentContextImpl(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    StringBuilder sb = new StringBuilder();

    while (exec.next()) {
      totalRecordCount += exec.getRecordCount();
      sb.append(String.format("got next with record count: %d (total: %d):\n", exec.getRecordCount(), totalRecordCount));
      sb.append("       t1                 t2\n");

      for (int valueIdx = 0; valueIdx < exec.getRecordCount(); valueIdx++) {
        List<Object> row = new ArrayList<>();
        for (ValueVector v : exec) {
          row.add(v.getField().getName() + ":" + v.getAccessor().getObject(valueIdx));
        }
        for (Object cell : row) {
          if (cell == null) {
            sb.append("<null>    ");
            continue;
          }
          int len = cell.toString().length();
          sb.append(cell).append(" ");
          for (int i = 0; i < (10 - len); ++i) {
            sb.append(" ");
          }
        }
        sb.append('\n');
      }
    }

    logger.info(sb.toString());
    assertEquals(25, totalRecordCount);

    if (context.getExecutorState().getFailureCause() != null) {
      throw context.getExecutorState().getFailureCause();
    }
    assertTrue(!context.getExecutorState().isFailed());
  }

  @Test
  @Ignore
  public void orderedEqualityInnerJoin() throws Throwable {
    DrillbitContext bitContext = mockDrillbitContext();
    UserClientConnection connection = Mockito.mock(UserClientConnection.class);

    PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c,
      new StoragePluginRegistryImpl(bitContext));
    PhysicalPlan plan = reader.readPhysicalPlan(
      DrillFileUtils.getResourceAsString("/join/merge_inner_single_batch.json")
        .replace("#{LEFT_FILE}", DrillFileUtils.getResourceAsFile("/join/merge_single_batch.left.json").toURI().toString())
        .replace("#{RIGHT_FILE}", DrillFileUtils.getResourceAsFile("/join/merge_single_batch.right.json").toURI().toString()));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContextImpl context = new FragmentContextImpl(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    StringBuilder sb = new StringBuilder();

    while (exec.next()) {
      totalRecordCount += exec.getRecordCount();
      sb.append(String.format("got next with record count: %d (total: %d):\n", exec.getRecordCount(), totalRecordCount));
      sb.append("       t1                 t2\n");

      for (int valueIdx = 0; valueIdx < exec.getRecordCount(); valueIdx++) {
        List<Object> row = new ArrayList<>();
        for (ValueVector v : exec) {
          row.add(v.getField().getName() + ":" + v.getAccessor().getObject(valueIdx));
        }
        for (Object cell : row) {
          if (cell == null) {
            sb.append("<null>    ");
            continue;
          }
          int len = cell.toString().length();
          sb.append(cell).append(" ");
          for (int i = 0; i < (10 - len); ++i) {
            sb.append(" ");
          }
        }
        sb.append('\n');
      }
    }
    assertEquals(23, totalRecordCount);

    if (context.getExecutorState().getFailureCause() != null) {
      throw context.getExecutorState().getFailureCause();
    }
    assertTrue(!context.getExecutorState().isFailed());
  }

  @Test
  @Ignore
  public void orderedEqualityMultiBatchJoin() throws Throwable {
    DrillbitContext bitContext = mockDrillbitContext();
    UserClientConnection connection = Mockito.mock(UserClientConnection.class);

    PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c,
      new StoragePluginRegistryImpl(bitContext));
    PhysicalPlan plan = reader.readPhysicalPlan(
      DrillFileUtils.getResourceAsString("/join/merge_multi_batch.json")
        .replace("#{LEFT_FILE}", DrillFileUtils.getResourceAsFile("/join/merge_multi_batch.left.json").toURI().toString())
        .replace("#{RIGHT_FILE}", DrillFileUtils.getResourceAsFile("/join/merge_multi_batch.right.json").toURI().toString()));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContextImpl context = new FragmentContextImpl(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    StringBuilder sb = new StringBuilder();

    while (exec.next()) {
      totalRecordCount += exec.getRecordCount();
      sb.append(String.format("got next with record count: %d (total: %d):\n", exec.getRecordCount(), totalRecordCount));

      for (int valueIdx = 0; valueIdx < exec.getRecordCount(); valueIdx++) {
        List<Object> row = new ArrayList<>();
        for (ValueVector v : exec) {
          row.add(v.getField().getName() + ":" + v.getAccessor().getObject(valueIdx));
        }
        for (Object cell : row) {
          if (cell == null) {
            sb.append("<null>    ");
            continue;
          }
          int len = cell.toString().length();
          sb.append(cell).append(" ");
          for (int i = 0; i < (10 - len); ++i) {
            sb.append(" ");
          }
        }
        sb.append('\n');
      }
    }
    assertEquals(25, totalRecordCount);

    if (context.getExecutorState().getFailureCause() != null) {
      throw context.getExecutorState().getFailureCause();
    }
    assertTrue(!context.getExecutorState().isFailed());
  }

  @Test
  public void testJoinBatchSize() throws Throwable {
    DrillbitContext bitContext = mockDrillbitContext();
    UserClientConnection connection = Mockito.mock(UserClientConnection.class);

    PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    PhysicalPlan plan = reader.readPhysicalPlan(DrillFileUtils.getResourceAsString("/join/join_batchsize.json"));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContextImpl context = new FragmentContextImpl(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));
    exec.next(); // skip schema batch
    while (exec.next()) {
      assertEquals(100, exec.getRecordCount());
    }

    if (context.getExecutorState().getFailureCause() != null) {
      throw context.getExecutorState().getFailureCause();
    }
    assertTrue(!context.getExecutorState().isFailed());
  }

  @Test
  public void testMergeJoinInnerEmptyBatch() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
          DrillFileUtils.getResourceAsString("/join/merge_join_empty_batch.json")
              .replace("${JOIN_TYPE}", "INNER"));
      int count = 0;
      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(0, count);
    }
  }

  @Test
  public void testMergeJoinLeftEmptyBatch() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
          DrillFileUtils.getResourceAsString("/join/merge_join_empty_batch.json")
              .replace("${JOIN_TYPE}", "LEFT"));
      int count = 0;
      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(50, count);
    }
  }

  @Test
  public void testMergeJoinRightEmptyBatch() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
          DrillFileUtils.getResourceAsString("/join/merge_join_empty_batch.json")
              .replace("${JOIN_TYPE}", "RIGHT"));
      int count = 0;
      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(0, count);
    }
  }

  @Test
  public void testMergeJoinExprInCondition() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
        DrillFileUtils.getResourceAsString("/join/mergeJoinExpr.json"));
      int count = 0;
      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(10, count);
    }
  }
}
