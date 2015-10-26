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
package org.apache.drill.exec.physical.impl;

import java.util.List;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.opt.BasicOptimizer;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionCreator;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.local.LocalPStoreProvider;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.junit.Ignore;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;

@Ignore
public class TestOptiqPlans extends ExecTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOptiqPlans.class);
  private final DrillConfig config = DrillConfig.create();

  @Test
  public void orderBy(@Injectable final BootStrapContext ctxt, @Injectable UserClientConnection connection,
      @Injectable ClusterCoordinator coord, @Injectable DataConnectionCreator com,
      @Injectable Controller controller, @Injectable WorkEventBus workBus) throws Throwable {
    final SimpleRootExec exec = doLogicalTest(ctxt, connection, "/logical_order.json", coord, com, controller, workBus);
  }

  @Test
  public void stringFilter(@Injectable final BootStrapContext ctxt, @Injectable UserClientConnection connection,
      @Injectable ClusterCoordinator coord, @Injectable DataConnectionCreator com,
      @Injectable Controller controller, @Injectable WorkEventBus workBus) throws Throwable {
    final SimpleRootExec exec = doLogicalTest(ctxt, connection, "/logical_string_filter.json", coord, com, controller, workBus);
  }

  @Test
  public void groupBy(@Injectable final BootStrapContext bitContext, @Injectable UserClientConnection connection,
      @Injectable ClusterCoordinator coord, @Injectable DataConnectionCreator com,
      @Injectable Controller controller, @Injectable WorkEventBus workBus) throws Throwable {
    final SimpleRootExec exec = doLogicalTest(bitContext, connection, "/logical_group.json", coord, com, controller, workBus);
  }

  private SimpleRootExec doLogicalTest(final BootStrapContext context, UserClientConnection connection, String file,
      ClusterCoordinator coord, DataConnectionCreator com, Controller controller, WorkEventBus workBus) throws Exception {
    new NonStrictExpectations() {
      {
        context.getMetrics();
        result = new MetricRegistry();
        context.getAllocator();
        result = RootAllocatorFactory.newRoot(config);
        context.getConfig();
        result = config;
      }
    };
    final RemoteServiceSet lss = RemoteServiceSet.getLocalServiceSet();
    final DrillbitContext bitContext = new DrillbitContext(DrillbitEndpoint.getDefaultInstance(), context, coord, controller,
        com, workBus, new LocalPStoreProvider(config), null);
    final QueryContext qc = new QueryContext(UserSession.Builder.newBuilder().setSupportComplexTypes(true).build(),
        bitContext);
    final PhysicalPlanReader reader = bitContext.getPlanReader();
    final LogicalPlan plan = reader.readLogicalPlan(Files.toString(FileUtils.getResourceAsFile(file), Charsets.UTF_8));
    final PhysicalPlan pp = new BasicOptimizer(qc, connection).optimize(new BasicOptimizer.BasicOptimizationContext(qc), plan);

    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(config);
    final FragmentContext fctxt = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(fctxt, (FragmentRoot) pp.getSortedOperators(false)
        .iterator().next()));
    return exec;

  }

  @Test
  public void testFilterPlan() throws Exception {
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (final Drillbit bit1 = new Drillbit(config, serviceSet);
        final DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {
      bit1.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
          Resources.toString(Resources.getResource("physical_filter.json"), Charsets.UTF_8));
      final RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (final QueryDataBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        loader.load(b.getHeader().getDef(), b.getData());
        for (final VectorWrapper<?> vw : loader) {
          System.out.println(vw.getValueVector().getField().toExpr());
          final ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            final Object o = vv.getAccessor().getObject(i);
            System.out.println(o);
          }
        }
        loader.clear();
        b.release();
      }
      client.close();
    }
  }

  @Test
  public void testJoinPlan() throws Exception {
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (final Drillbit bit1 = new Drillbit(config, serviceSet);
        final DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {
      bit1.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
          Resources.toString(Resources.getResource("physical_join.json"), Charsets.UTF_8));
      final RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (final QueryDataBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        loader.load(b.getHeader().getDef(), b.getData());
        for (final VectorWrapper<?> vw : loader) {
          System.out.println(vw.getValueVector().getField().toExpr());
          final ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            final Object o = vv.getAccessor().getObject(i);
            System.out.println(o);
          }
        }
        loader.clear();
        b.release();
      }
      client.close();
    }
  }

  @Test
  public void testFilterString() throws Exception {
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (final Drillbit bit1 = new Drillbit(config, serviceSet);
        final DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {
      bit1.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.LOGICAL,
          Resources.toString(Resources.getResource("logical_string_filter.json"), Charsets.UTF_8));
      final RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (final QueryDataBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        loader.load(b.getHeader().getDef(), b.getData());
        for (final VectorWrapper<?> vw : loader) {
          System.out.println(vw.getValueVector().getField().toExpr());
          final ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            final Object o = vv.getAccessor().getObject(i);
            if (vv instanceof VarBinaryVector) {
              final VarBinaryVector.Accessor x = ((VarBinaryVector) vv).getAccessor();
              final VarBinaryHolder vbh = new VarBinaryHolder();
              x.get(i, vbh);
              System.out.printf("%d..%d", vbh.start, vbh.end);

              System.out.println("[" + new String((byte[]) vv.getAccessor().getObject(i)) + "]");
            } else {
              System.out.println(vv.getAccessor().getObject(i));
            }

          }
        }
        loader.clear();
        b.release();
      }
      client.close();
    }
  }

  @Test
  public void testLogicalJsonScan() throws Exception {
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (final Drillbit bit1 = new Drillbit(config, serviceSet);
        final DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {
      bit1.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.LOGICAL,
          Resources.toString(Resources.getResource("logical_json_scan.json"), Charsets.UTF_8));
      final RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (final QueryDataBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        loader.load(b.getHeader().getDef(), b.getData());
        for (final VectorWrapper vw : loader) {
          System.out.println(vw.getValueVector().getField().toExpr());
          final ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            final Object o = vv.getAccessor().getObject(i);
            if (vv instanceof VarBinaryVector) {
              final VarBinaryVector.Accessor x = ((VarBinaryVector) vv).getAccessor();
              final VarBinaryHolder vbh = new VarBinaryHolder();
              x.get(i, vbh);
              System.out.printf("%d..%d", vbh.start, vbh.end);

              System.out.println("[" + new String((byte[]) vv.getAccessor().getObject(i)) + "]");
            } else {
              System.out.println(vv.getAccessor().getObject(i));
            }

          }
        }
        loader.clear();
        b.release();
      }
      client.close();
    }
  }

  @Test
  public void testOrderVarbinary() throws Exception {
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (final Drillbit bit1 = new Drillbit(config, serviceSet);
        final DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {
      bit1.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
          Resources.toString(Resources.getResource("physical_order_varbinary.json"), Charsets.UTF_8));
      final RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (final QueryDataBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        loader.load(b.getHeader().getDef(), b.getData());
        for (final VectorWrapper vw : loader) {
          System.out.println(vw.getValueVector().getField().toExpr());
          final ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            final Object o = vv.getAccessor().getObject(i);
            if (vv instanceof VarBinaryVector) {
              final VarBinaryVector.Accessor x = ((VarBinaryVector) vv).getAccessor();
              final VarBinaryHolder vbh = new VarBinaryHolder();
              x.get(i, vbh);
              System.out.printf("%d..%d", vbh.start, vbh.end);

              System.out.println("[" + new String((byte[]) vv.getAccessor().getObject(i)) + "]");
            } else {
              System.out.println(vv.getAccessor().getObject(i));
            }

          }
        }
        loader.clear();
        b.release();
      }
      client.close();
    }
  }

  private SimpleRootExec doPhysicalTest(final DrillbitContext bitContext, UserClientConnection connection, String file)
      throws Exception {
    new NonStrictExpectations() {
      {
        bitContext.getMetrics();
        result = new MetricRegistry();
        bitContext.getAllocator();
        result = RootAllocatorFactory.newRoot(config);
        bitContext.getConfig();
        result = config;
      }
    };

    final StoragePluginRegistry reg = new StoragePluginRegistry(bitContext);

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(config, reg);
    final PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile(file), Charsets.UTF_8));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(config);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false)
        .iterator().next()));
    return exec;
  }
}
