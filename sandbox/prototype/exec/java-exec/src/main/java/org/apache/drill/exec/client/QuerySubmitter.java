package org.apache.drill.exec.client;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.io.Resources;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.coord.ZKClusterCoordinator;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RemoteRpcException;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QuerySubmitter {

  public static void main(String args[]) throws Exception {
    QuerySubmitter submitter = new QuerySubmitter();
    System.exit(submitter.submitQuery(args[0], args[1], args[2]));
  }

  public int submitQuery(String planLocation, String type, String zkQuorum) throws Exception {
    DrillConfig config = DrillConfig.create();
    ZKClusterCoordinator clusterCoordinator = new ZKClusterCoordinator(config, zkQuorum);
    clusterCoordinator.start(10000);
    DrillClient client = new DrillClient(config, clusterCoordinator);
    client.connect();
    QueryResultsListener listener = new QueryResultsListener();
    String plan = Charsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(Paths.get(planLocation)))).toString();
    UserProtos.QueryType queryType;
    type = type.toLowerCase();
    switch(type) {
      case "logical":
        queryType = UserProtos.QueryType.LOGICAL;
        break;
      case "physical":
        queryType = UserProtos.QueryType.PHYSICAL;
        break;
      default:
        System.out.println("Invalid query type: " + type);
        return -1;
    }
    Stopwatch watch = new Stopwatch();
    watch.start();
    client.runQuery(queryType, plan, listener);
    System.out.println(String.format("Got %d total records in %d seconds", listener.await(), watch.elapsed(TimeUnit.SECONDS)));
    return 0;
  }

  private class QueryResultsListener implements UserResultsListener {
    AtomicInteger count = new AtomicInteger();
    private CountDownLatch latch = new CountDownLatch(1);
    @Override
    public void submissionFailed(RpcException ex) {
      System.out.println(String.format("Query failed: %s", ex));
      latch.countDown();
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      int rows = result.getHeader().getRowCount();
      RecordBatchLoader loader = new RecordBatchLoader(new BootStrapContext(DrillConfig.create()).getAllocator());
      try {
        loader.load(result.getHeader().getDef(), result.getData());
      } catch (SchemaChangeException e) {
        submissionFailed(new RpcException(e));
      }
      List<String> columns = Lists.newArrayList();
      for (VectorWrapper vw : loader) {
        columns.add(vw.getValueVector().getField().getName());
      }
      for (int row = 0; row < rows; row++) {
        if (row%50 == 0) {
          for (String column : columns) {
            System.out.printf("| %-15s", column.length() <= 15 ? column : column.substring(0, 14));
          }
          System.out.printf("|\n");
        }
        for (VectorWrapper vw : loader) {
          Object o = vw.getValueVector().getAccessor().getObject(row);
          if (o instanceof byte[]) {
            String value = new String((byte[]) o);
            System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0, 14));
          } else {
            String value = o.toString();
            System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0,14));
          }
        }
        System.out.printf("|\n");
      }
      count.addAndGet(rows);
      if (result.getHeader().getIsLastChunk()) latch.countDown();
      result.release();
    }

    public int await() throws Exception {
      latch.await();
      return count.get();
    }
  }
}
