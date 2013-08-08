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
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
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

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;

public class TestParquetPhysicalPlan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestParquetPhysicalPlan.class);

  //public String fileName = "/physical_test2.json";
  public String fileName = "parquet_scan_union_screen_physical.json";

  @Test
  @Ignore
  public void testParseParquetPhysicalPlan() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, Resources.toString(Resources.getResource(fileName),Charsets.UTF_8));
      System.out.println(String.format("Got %d results", results.size()));
      client.close();
    }
  }
}
