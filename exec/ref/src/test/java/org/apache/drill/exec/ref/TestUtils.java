package org.apache.drill.exec.ref;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ref.RunOutcome.OutcomeType;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.JSONRecordReader;
import org.apache.drill.exec.ref.rse.RSERegistry;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestUtils {
  public static RecordIterator jsonToRecordIterator(String schemaPath, String j) throws IOException {
    InputStream is = new ByteArrayInputStream(j.getBytes());
    JSONRecordReader reader = new JSONRecordReader(new SchemaPath(schemaPath, ExpressionPosition.UNKNOWN), DrillConfig.create(), is, null);
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
    DrillConfig config = getConfigWithQueue(0);
    Collection<RunOutcome> outcomes = getOutcome(config, resourcePath);
    assertEquals(recordCount, outcomes.iterator().next().records);
  }

  /**
   * Runs a logical query plan and returns output
   * @param config DrillConfig to be ustilized.
   * @param resourcePath Path for JSON logical plan
   * @return A collection of RunOutcomes
   * @throws IOException
   */
  public static Collection<RunOutcome> getOutcome(DrillConfig config, String resourcePath) throws IOException{
    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile(resourcePath), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
    i.setup();
    return i.run();
  }
  
  private static DrillConfig getConfigWithQueue(int queueNumber){
    DrillConfig config = DrillConfig.create();
    final BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(100);
    config.setSinkQueues(queueNumber, queue);
    return config;
  }
  
  
  
  public static List<UnbackedRecord> getResultAsUnbackedRecords(String resourcePath) throws Exception{
    DrillConfig config = getConfigWithQueue(0);
    Collection<RunOutcome> outcomes = getOutcome(config, resourcePath);
    if(outcomes.size() != 1) throw new Exception("Only supports logical plans that provide a single outcome.");
    RunOutcome out = outcomes.iterator().next();
    switch(out.outcome){
    case CANCELED:
    case FAILED:
      if(out.exception != null) throw (Exception) out.exception;
      throw new Exception("Failure while running query.");
    }
    Object o;
    Queue<Object> q = config.getQueue(0);
    List<UnbackedRecord> records = Lists.newArrayList();
    while( (o = q.poll()) != null){
      if(o instanceof OutcomeType) continue;
      if( !(o instanceof UnbackedRecord) ) throw new Exception(String.format("This method only works when the QueueRSE is configured to use RECORD encoding.  One of the queue objects was of type %s", o.getClass().getCanonicalName()));
      records.add( (UnbackedRecord) o);
    }

    return records;
    
  }
}
