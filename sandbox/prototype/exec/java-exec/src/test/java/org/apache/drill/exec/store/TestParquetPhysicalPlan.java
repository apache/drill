package org.apache.drill.exec.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import mockit.Injectable;
import mockit.NonStrictExpectations;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;

public class TestParquetPhysicalPlan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestParquetPhysicalPlan.class);

  //public String fileName = "/physical_test2.json";
  public String fileName = "parquet/parquet_scan_union_screen_physical.json";
//  public String fileName = "parquet-sample.json";


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
//          System.out.println(vw.getValueVector().getField().getName() + vw.getValueVector().getField().getType());
          System.out.println(vw.getValueVector().getField().getName());
          ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
            Object o = vv.getAccessor().getObject(i);
            if (o instanceof byte[]) {
              System.out.println(new String((byte[]) o));
            } else {
              System.out.println(vv.getAccessor().getObject(i));
            }
            break;
          }
        }
      }
      client.close();
      System.out.println(String.format("Got %d total results", count));
    }
  }

  private class ParquetResultsListener implements UserResultsListener {
    private CountDownLatch latch = new CountDownLatch(1);
    @Override
    public void submissionFailed(RpcException ex) {
      logger.error("submission failed", ex);
      latch.countDown();
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      System.out.printf("Result batch arrived. Number of records: %d", result.getHeader().getRowCount());
      if (result.getHeader().getIsLastChunk()) latch.countDown();
    }

    public void await() throws Exception {
      latch.await();
    }
  }
  @Test
  @Ignore
  public void testParseParquetPhysicalPlanRemote() throws Exception {
    DrillConfig config = DrillConfig.create();

    try(DrillClient client = new DrillClient(config);){
      client.connect();
      ParquetResultsListener listener = new ParquetResultsListener();
      client.runQuery(UserProtos.QueryType.PHYSICAL, Resources.toString(Resources.getResource(fileName),Charsets.UTF_8), listener);
      listener.await();
      client.close();
    }
  }
}
