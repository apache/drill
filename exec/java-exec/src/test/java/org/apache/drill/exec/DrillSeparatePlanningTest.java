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
package org.apache.drill.exec;

import static org.junit.Assert.*;
import io.netty.buffer.DrillBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.DrillAutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.PrintingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter.Format;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.AwaitableUserResultsListener;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Class to test different planning use cases (separate form query execution)
 *
 */
public class DrillSeparatePlanningTest extends BaseTestQuery {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSeparatePlanningTest.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  //final String query = "SELECT sales_city, COUNT(*) cnt FROM cp.`region.json` GROUP BY sales_city";
  //final String query = "SELECT * FROM cp.`employee.json` where  employee_id > 1 and  employee_id < 1000";
  //final String query = "SELECT o_orderkey, o_custkey FROM dfs.tmp.`multilevel` where dir0 = 1995 and o_orderkey > 100 and o_orderkey < 1000 limit 5";
  //final String query = "SELECT sum(o_totalprice) FROM dfs.tmp.`multilevel` where dir0 = 1995 and o_orderkey > 100 and o_orderkey < 1000";
  //final String query = "SELECT o_orderkey FROM dfs.tmp.`multilevel` order by o_orderkey";
  //final String query = "SELECT dir1, sum(o_totalprice) FROM dfs.tmp.`multilevel` where dir0 = 1995 group by dir1 order by dir1";
  //final String query = String.format("SELECT dir0, sum(o_totalprice) FROM dfs_test.`%s/multilevel/json` group by dir0 order by dir0", TEST_RES_PATH);


  @Test(timeout=30000)
  public void testSingleFragmentQuery() throws Exception {
    final String query = "SELECT * FROM cp.`employee.json` where  employee_id > 1 and  employee_id < 1000";

    QueryPlanFragments planFragments = getFragmentsHelper(query);

    assertNotNull(planFragments);

    assertEquals(1, planFragments.getFragmentsCount());
    assertTrue(planFragments.getFragments(0).getLeafFragment());

    getResultsHelper(planFragments);
  }

  @Test(timeout=30000)
  public void testMultiMinorFragmentSimpleQuery() throws Exception {
    final String query = String.format("SELECT o_orderkey FROM dfs_test.`%s/multilevel/json`", TEST_RES_PATH);

    QueryPlanFragments planFragments = getFragmentsHelper(query);

    assertNotNull(planFragments);

    assertTrue((planFragments.getFragmentsCount() > 1));

    for ( PlanFragment planFragment : planFragments.getFragmentsList()) {
      assertTrue(planFragment.getLeafFragment());
    }

    getResultsHelper(planFragments);
  }

  @Test(timeout=30000)
  public void testMultiMinorFragmentComplexQuery() throws Exception {
    final String query = String.format("SELECT dir0, sum(o_totalprice) FROM dfs_test.`%s/multilevel/json` group by dir0 order by dir0", TEST_RES_PATH);

    QueryPlanFragments planFragments = getFragmentsHelper(query);

    assertNotNull(planFragments);

    assertTrue((planFragments.getFragmentsCount() > 1));

    for ( PlanFragment planFragment : planFragments.getFragmentsList()) {
      assertTrue(planFragment.getLeafFragment());
    }

    getResultsHelper(planFragments);

  }

  @Test(timeout=30000)
  public void testPlanningNoSplit() throws Exception {
    final String query = String.format("SELECT dir0, sum(o_totalprice) FROM dfs_test.`%s/multilevel/json` group by dir0 order by dir0", TEST_RES_PATH);

    updateTestCluster(2, config);

    List<QueryDataBatch> results = client.runQuery(QueryType.SQL, "alter session set `planner.slice_target`=1");
    for(QueryDataBatch batch : results) {
      batch.release();
    }

    DrillRpcFuture<QueryPlanFragments> queryFragmentsFutures = client.planQuery(QueryType.SQL, query, false);

    final QueryPlanFragments planFragments = queryFragmentsFutures.get();

    assertNotNull(planFragments);

    assertTrue((planFragments.getFragmentsCount() > 1));

    PlanFragment rootFragment = planFragments.getFragments(0);
    assertFalse(rootFragment.getLeafFragment());

    getCombinedResultsHelper(planFragments);

  }

  @Test(timeout=30000)
  public void testPlanningNegative() throws Exception {
    final String query = String.format("SELECT dir0, sum(o_totalprice) FROM dfs_test.`%s/multilevel/json` group by dir0 order by dir0", TEST_RES_PATH);

    updateTestCluster(2, config);
    // LOGICAL is not supported
    DrillRpcFuture<QueryPlanFragments> queryFragmentsFutures = client.planQuery(QueryType.LOGICAL, query, false);

    final QueryPlanFragments planFragments = queryFragmentsFutures.get();

    assertNotNull(planFragments);

    assertNotNull(planFragments.getError());

    assertTrue(planFragments.getFragmentsCount()==0);

  }

  @Test(timeout=30000)
  public void testPlanning() throws Exception {
    final String query = String.format("SELECT dir0, columns[3] FROM dfs_test.`%s/multilevel/csv` order by dir0", TEST_RES_PATH);

    updateTestCluster(2, config);

    List<QueryDataBatch> results = client.runQuery(QueryType.SQL, "alter session set `planner.slice_target`=1");
    for(QueryDataBatch batch : results) {
      batch.release();
    }
    AwaitableUserResultsListener listener =
        new AwaitableUserResultsListener(new PrintingResultsListener(client.getConfig(), Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH));
    //AwaitableUserResultsListener listener =
    //    new AwaitableUserResultsListener(new SilentListener());
    client.runQuery(QueryType.SQL, query, listener);
    int rows = listener.await();
  }

  private QueryPlanFragments getFragmentsHelper(final String query) throws InterruptedException, ExecutionException, RpcException {
    updateTestCluster(2, config);

    List<QueryDataBatch> results = client.runQuery(QueryType.SQL, "alter session set `planner.slice_target`=1");
    for(QueryDataBatch batch : results) {
      batch.release();
    }

    DrillRpcFuture<QueryPlanFragments> queryFragmentsFutures = client.planQuery(QueryType.SQL, query, true);

    final QueryPlanFragments planFragments = queryFragmentsFutures.get();

    for (PlanFragment fragment : planFragments.getFragmentsList()) {
      System.out.println(fragment.getFragmentJson());
    }

    return planFragments;
  }

  private void getResultsHelper(final QueryPlanFragments planFragments) throws Exception {
    for (PlanFragment fragment : planFragments.getFragmentsList()) {
      DrillbitEndpoint assignedNode = fragment.getAssignment();
      DrillClient fragmentClient = new DrillClient(true);
      Properties props = new Properties();
      props.setProperty("drillbit", assignedNode.getAddress() + ":" + assignedNode.getUserPort());
      fragmentClient.connect(props);

      ShowResultsUserResultsListener myListener = new ShowResultsUserResultsListener(getAllocator());
      AwaitableUserResultsListener listenerBits =
          new AwaitableUserResultsListener(myListener);
      fragmentClient.runQuery(QueryType.SQL, "select hostname, user_port from sys.drillbits where `current`=true",
          listenerBits);
      int row = listenerBits.await();
      assertEquals(1, row);
      List<Map<String,String>> records = myListener.getRecords();
      assertEquals(1, records.size());
      Map<String,String> record = records.get(0);
      assertEquals(2, record.size());
      Iterator<Entry<String, String>> iter = record.entrySet().iterator();
      Entry<String, String> entry;
      String host = null;
      String port = null;
      for (int i = 0; i < 2; i++) {
       entry = iter.next();
       if (entry.getKey().equalsIgnoreCase("hostname")) {
          host = entry.getValue();
        } else if (entry.getKey().equalsIgnoreCase("user_port")) {
          port = entry.getValue();
        } else {
          fail("Unknown field: " + entry.getKey());
        }
       }
      assertTrue(props.getProperty("drillbit").equalsIgnoreCase(host+":" + port));

      List<PlanFragment> fragmentList = Lists.newArrayList();
      fragmentList.add(fragment);
      //AwaitableUserResultsListener listener =
     //     new AwaitableUserResultsListener(new PrintingResultsListener(client.getConfig(), Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH));
      AwaitableUserResultsListener listener =
          new AwaitableUserResultsListener(new SilentListener());
      fragmentClient.runQuery(QueryType.EXECUTION, fragmentList, listener);
      int rows = listener.await();
      fragmentClient.close();
    }
  }

  private void getCombinedResultsHelper(final QueryPlanFragments planFragments) throws Exception {
      ShowResultsUserResultsListener myListener = new ShowResultsUserResultsListener(getAllocator());
      AwaitableUserResultsListener listenerBits =
          new AwaitableUserResultsListener(myListener);

      //AwaitableUserResultsListener listener =
     //     new AwaitableUserResultsListener(new PrintingResultsListener(client.getConfig(), Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH));
      AwaitableUserResultsListener listener =
          new AwaitableUserResultsListener(new SilentListener());
      client.runQuery(QueryType.EXECUTION, planFragments.getFragmentsList(), listener);
      int rows = listener.await();
  }

  /**
   * Helper class to get results
   *
   */
  static class ShowResultsUserResultsListener implements UserResultsListener {

    private QueryId queryId;
    private final RecordBatchLoader loader;
    private final BufferAllocator allocator;
    private UserException ex;
    private List<Map<String,String>> records = Lists.newArrayList();

    public ShowResultsUserResultsListener(BufferAllocator allocator) {
      this.loader = new RecordBatchLoader(allocator);
      this.allocator = allocator;
    }

    public QueryId getQueryId() {
      return queryId;
    }

    public List<Map<String, String>> getRecords() {
      return records;
    }

    public UserException getEx() {
      return ex;
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
     this.queryId = queryId;
    }

    @Override
    public void submissionFailed(UserException ex) {
      DrillAutoCloseables.closeNoChecked(allocator);
      this.ex = ex;
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      QueryData queryHeader = result.getHeader();
      int rows = queryHeader.getRowCount();
      try {
        if ( result.hasData() ) {
          DrillBuf data = result.getData();
          loader.load(queryHeader.getDef(), data);
          for (int i = 0; i < rows; i++) {
             Map<String,String> record = Maps.newHashMap();
            for (VectorWrapper<?> vw : loader) {
              final String field = vw.getValueVector().getMetadata().getNamePart().getName();
              final ValueVector.Accessor accessor = vw.getValueVector().getAccessor();
              final Object value = i < accessor.getValueCount() ? accessor.getObject(i) : null;
              final String display = value == null ? null : value.toString();
              record.put(field, display);
            }
            records.add(record);
          }
          loader.clear();
        }
        result.release();
      } catch (SchemaChangeException e) {
        fail(e.getMessage());
      }

    }

    @Override
    public void queryCompleted(QueryState state) {
    }

  }
}
