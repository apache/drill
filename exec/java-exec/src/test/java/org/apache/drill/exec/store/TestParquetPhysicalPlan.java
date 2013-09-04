package org.apache.drill.exec.store;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.io.Resources;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestParquetPhysicalPlan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestParquetPhysicalPlan.class);

  public String fileName = "parquet/parquet_scan_filter_union_screen_physical.json";

  @Test
  @Ignore
  public void testParseParquetPhysicalPlan() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, Resources.toString(Resources.getResource(fileName),Charsets.UTF_8));
      RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      int count = 0;
      for (QueryResultBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        count += b.getHeader().getRowCount();
        loader.load(b.getHeader().getDef(), b.getData());
        for (VectorWrapper vw : loader) {
          System.out.print(vw.getValueVector().getField().getName() + ": ");
          ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            Object o = vv.getAccessor().getObject(i);
            if (o instanceof byte[]) {
              System.out.print(" [" + new String((byte[]) o) + "]");
            } else {
              System.out.print(" [" + vv.getAccessor().getObject(i) + "]");
            }
//            break;
          }
          System.out.println();
        }
      }
      client.close();
      System.out.println(String.format("Got %d total results", count));
    }
  }

  private class ParquetResultsListener implements UserResultsListener {
    AtomicInteger count = new AtomicInteger();
    private CountDownLatch latch = new CountDownLatch(1);
    @Override
    public void submissionFailed(RpcException ex) {
      logger.error("submission failed", ex);
      latch.countDown();
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      int rows = result.getHeader().getRowCount();
      System.out.println(String.format("Result batch arrived. Number of records: %d", rows));
      count.addAndGet(rows);
      if (result.getHeader().getIsLastChunk()) latch.countDown();
      result.release();
    }

    public int await() throws Exception {
      latch.await();
      return count.get();
    }
  }
  @Test
  @Ignore
  public void testParseParquetPhysicalPlanRemote() throws Exception {
    DrillConfig config = DrillConfig.create();

    try(DrillClient client = new DrillClient(config);){
      client.connect();
      ParquetResultsListener listener = new ParquetResultsListener();
      Stopwatch watch = new Stopwatch();
      watch.start();
      client.runQuery(UserProtos.QueryType.PHYSICAL, Resources.toString(Resources.getResource(fileName),Charsets.UTF_8), listener);
      System.out.println(String.format("Got %d total records in %d seconds", listener.await(), watch.elapsed(TimeUnit.SECONDS)));
      client.close();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception{
    // pause to get logger to catch up.
    Thread.sleep(1000);
  }

}
