package org.apache.drill.exec.ref;

import com.google.common.base.Charsets;
import com.google.common.collect.Queues;
import com.google.common.io.Files;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.JSONRecordReader;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.codehaus.jackson.node.ArrayNode;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUtils {
  public static RecordIterator jsonToRecordIterator(String schemaPath, String j) throws IOException {
    InputStream is = new ByteArrayInputStream(j.getBytes());
    JSONRecordReader reader = new JSONRecordReader(new SchemaPath(schemaPath), DrillConfig.create(), is, null);
    return reader.getIterator();
  }

  public static int getIteratorCount(RecordIterator out) {
    RecordIterator.NextOutcome next;
    int counter = 0;
    while((next = out.next()) != RecordIterator.NextOutcome.NONE_LEFT) {
      counter++;
      //RecordPointer rp = out.getRecordPointer();
      //System.out.println(rp);
    }
    return counter;
  }

  /**
   *
   * @param resourcePath path for json plan file
   * @param recordCount expected record count
   * @throws Exception
   */
  public static void assertProduceCount(String resourcePath, int recordCount) throws Exception {
    DrillConfig config = DrillConfig.create();
    final BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(100);
    config.setSinkQueues(0, queue);
    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile(resourcePath), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
    i.setup();
    Collection<RunOutcome> outcomes = i.run();
    assertEquals(outcomes.iterator().next().records, recordCount);
  }

  /**
   * 
   * @param logicPlanFile path for logical plan file
   * @param resultFile path for expected result file                     
   * @return true if we get the expected result
   * @throws Exception
   */
  public static boolean executePlan(String logicPlanFile, String resultFile) throws Exception{
    DrillConfig config = DrillConfig.create();
    BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
    config.setSinkQueues(0, queue);

    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile(logicPlanFile), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));

    i.setup();
    Collection<RunOutcome> outcomes = i.run();

    StringBuilder sb = new StringBuilder();
    while(queue.peek() != null && ! (queue.peek() instanceof RunOutcome.OutcomeType)){
      String record = new String((byte[])queue.poll());
      sb.append(record);
    }
    String result = Files.toString(FileUtils.getResourceAsFile(resultFile), Charsets.UTF_8);
    System.out.println(sb.toString());
    return sb.toString().equals(result);
  }
  
}
