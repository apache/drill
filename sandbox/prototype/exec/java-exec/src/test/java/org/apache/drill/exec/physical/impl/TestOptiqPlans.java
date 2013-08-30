package org.apache.drill.exec.physical.impl;

import java.util.List;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.opt.BasicOptimizer;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.bit.BitCom;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StorageEngineRegistry;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryHolder;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.yammer.metrics.MetricRegistry;

@Ignore
public class TestOptiqPlans {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOptiqPlans.class);
  DrillConfig c = DrillConfig.create();
  
  @Test
  public void orderBy(@Injectable final BootStrapContext ctxt, @Injectable UserClientConnection connection, @Injectable ClusterCoordinator coord, @Injectable BitCom com, @Injectable DistributedCache cache) throws Throwable{
    SimpleRootExec exec = doLogicalTest(ctxt, connection, "/logical_order.json", coord, com, cache);
  }
  
  @Test
  public void stringFilter(@Injectable final BootStrapContext ctxt, @Injectable UserClientConnection connection, @Injectable ClusterCoordinator coord, @Injectable BitCom com, @Injectable DistributedCache cache) throws Throwable{
    SimpleRootExec exec = doLogicalTest(ctxt, connection, "/logical_string_filter.json", coord, com, cache);
  }
  
  @Test
  public void groupBy(@Injectable final BootStrapContext bitContext, @Injectable UserClientConnection connection, @Injectable ClusterCoordinator coord, @Injectable BitCom com, @Injectable DistributedCache cache) throws Throwable{
    SimpleRootExec exec = doLogicalTest(bitContext, connection, "/logical_group.json", coord, com, cache);
  }
  
  private SimpleRootExec doLogicalTest(final BootStrapContext context, UserClientConnection connection, String file, ClusterCoordinator coord, BitCom com, DistributedCache cache) throws Exception{
    new NonStrictExpectations(){{
      context.getMetrics(); result = new MetricRegistry("test");
      context.getAllocator(); result = BufferAllocator.getAllocator(c);
      context.getConfig(); result = c;
    }};
    RemoteServiceSet lss = RemoteServiceSet.getLocalServiceSet();
    DrillbitContext bitContext = new DrillbitContext(DrillbitEndpoint.getDefaultInstance(), context, coord, com, cache);
    QueryContext qc = new QueryContext(QueryId.getDefaultInstance(), bitContext);
    PhysicalPlanReader reader = bitContext.getPlanReader();
    LogicalPlan plan = reader.readLogicalPlan(Files.toString(FileUtils.getResourceAsFile(file), Charsets.UTF_8));
    PhysicalPlan pp = new BasicOptimizer(DrillConfig.create(), qc).optimize(new BasicOptimizer.BasicOptimizationContext(), plan);
    

    
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext fctxt = new FragmentContext(bitContext, FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(fctxt, (FragmentRoot) pp.getSortedOperators(false).iterator().next()));
    return exec;
    
  }
  
  @Test
  public void testFilterPlan() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, Resources.toString(Resources.getResource("physical_filter.json"),Charsets.UTF_8));
      RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (QueryResultBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        loader.load(b.getHeader().getDef(), b.getData());
        for (VectorWrapper vw : loader) {
          System.out.println(vw.getValueVector().getField().getName());
          ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            Object o = vv.getAccessor().getObject(i);
            System.out.println(vv.getAccessor().getObject(i));
          }
        }
      }
      client.close();
    }
  }

  @Test
  public void testJoinPlan() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, Resources.toString(Resources.getResource("physical_join.json"),Charsets.UTF_8));
      RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (QueryResultBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        loader.load(b.getHeader().getDef(), b.getData());
        for (VectorWrapper vw : loader) {
          System.out.println(vw.getValueVector().getField().getName());
          ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            Object o = vv.getAccessor().getObject(i);
            System.out.println(vv.getAccessor().getObject(i));
          }
        }
      }
      client.close();
    }
  }
  
  @Test
  public void testFilterString() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.LOGICAL, Resources.toString(Resources.getResource("logical_string_filter.json"),Charsets.UTF_8));
      RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (QueryResultBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        loader.load(b.getHeader().getDef(), b.getData());
        for (VectorWrapper vw : loader) {
          System.out.println(vw.getValueVector().getField().getName());
          ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            Object o = vv.getAccessor().getObject(i);
            if(vv instanceof VarBinaryVector){
              VarBinaryVector.Accessor x = ((VarBinaryVector) vv).getAccessor();
              VarBinaryHolder vbh = new VarBinaryHolder();
              x.get(i, vbh);
              System.out.printf("%d..%d", vbh.start, vbh.end);
              
              System.out.println("[" + new String( (byte[]) vv.getAccessor().getObject(i)) + "]");  
            }else{
              System.out.println(vv.getAccessor().getObject(i));
            }
            
          }
        }
      }
      client.close();
    }
  }
  
  
  @Test
  public void testLogicalJsonScan() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.LOGICAL, Resources.toString(Resources.getResource("logical_json_scan.json"),Charsets.UTF_8));
      RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (QueryResultBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        loader.load(b.getHeader().getDef(), b.getData());
        for (VectorWrapper vw : loader) {
          System.out.println(vw.getValueVector().getField().getName());
          ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            Object o = vv.getAccessor().getObject(i);
            if(vv instanceof VarBinaryVector){
              VarBinaryVector.Accessor x = ((VarBinaryVector) vv).getAccessor();
              VarBinaryHolder vbh = new VarBinaryHolder();
              x.get(i, vbh);
              System.out.printf("%d..%d", vbh.start, vbh.end);
              
              System.out.println("[" + new String( (byte[]) vv.getAccessor().getObject(i)) + "]");  
            }else{
              System.out.println(vv.getAccessor().getObject(i));
            }
            
          }
        }
      }
      client.close();
    }
  }
  
  @Test
  public void testOrderVarbinary() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, Resources.toString(Resources.getResource("physical_order_varbinary.json"),Charsets.UTF_8));
      RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (QueryResultBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        loader.load(b.getHeader().getDef(), b.getData());
        for (VectorWrapper vw : loader) {
          System.out.println(vw.getValueVector().getField().getName());
          ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            Object o = vv.getAccessor().getObject(i);
            if(vv instanceof VarBinaryVector){
              VarBinaryVector.Accessor x = ((VarBinaryVector) vv).getAccessor();
              VarBinaryHolder vbh = new VarBinaryHolder();
              x.get(i, vbh);
              System.out.printf("%d..%d", vbh.start, vbh.end);
              
              System.out.println("[" + new String( (byte[]) vv.getAccessor().getObject(i)) + "]");  
            }else{
              System.out.println(vv.getAccessor().getObject(i));
            }
            
          }
        }
      }
      client.close();
    }
  }
  
  private SimpleRootExec doPhysicalTest(final DrillbitContext bitContext, UserClientConnection connection, String file) throws Exception{
    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry("test");
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
      bitContext.getConfig(); result = c;
    }};
    
    StorageEngineRegistry reg = new StorageEngineRegistry(bitContext);
    
    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance(), reg);
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile(file), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));
    return exec;
  }
  
  @AfterClass
  public static void tearDown() throws Exception{
    // pause to get logger to catch up.
    Thread.sleep(1000);
  }
}
