package org.apache.drill.exec.client;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.drill.exec.DrillSystemTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author David Alves
 */
public class DrillClientSystemTest extends DrillSystemTestBase {

  private static String plan;

  @BeforeClass
  public static void setUp() throws Exception {
    DrillSystemTestBase.setUp();
    plan = Resources.toString(Resources.getResource("simple_plan.json"), Charsets.UTF_8);
  }

  @After
  public void tearDown() {
    stopCluster();
    stopZookeeper();
  }

  @Test
  public void testSubmitPlanSingleNode() throws Exception {
    startZookeeper(1);
    startCluster(1);
    DrillClient client = new DrillClient();
    client.connect();
    DrillRpcFuture<UserProtos.QueryHandle> result = client.submitPlan(plan);
    System.out.println(result.get());
    client.close();
  }

  @Test
  public void testSubmitPlanTwoNodes() throws Exception {
    startZookeeper(1);
    startCluster(2);
    DrillClient client = new DrillClient();
    client.connect();
    DrillRpcFuture<UserProtos.QueryHandle> result = client.submitPlan(plan);
    System.out.println(result.get());
    client.close();
  }
}
