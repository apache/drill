package org.apache.drill.sql.client.ref;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.hydromatic.linq4j.Enumerator;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.apache.drill.jdbc.DrillTable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DrillRefImpl<E> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRefImpl.class);

  private static final ObjectMapper mapper = createMapper();
  
  private final String plan;
  final BlockingQueue<Object> queue;
  final DrillConfig config;
  private final List<String> fields;

  
  public DrillRefImpl(String plan, DrillConfig config, List<String> fields, BlockingQueue<Object> queue) {
    super();
    this.plan = plan;
    this.config = config;
    this.fields = fields;
    this.queue = queue;
  }
  
  
  private static ObjectMapper createMapper() {
    return new ObjectMapper();
  }

  /**
   * Enumerator used for reference interpreter
   */
  private static class JsonEnumerator implements Enumerator {
    private final BlockingQueue<Object> queue;
    private final String holder;
    private final List<String> fields;
    private Object current;

    public JsonEnumerator(BlockingQueue<Object> queue, List<String> fields) {
      this.queue = queue;
      this.holder = null;
      this.fields = fields;
    }

    public void close(){
      
    }
    
    public Object current() {
      return current;
    }

    public boolean moveNext() {
      try {
        Object o = queue.take();
        if (o instanceof RunOutcome.OutcomeType) {
          switch ((RunOutcome.OutcomeType) o) {
            case SUCCESS:
              return false; // end of data
            case CANCELED:
              throw new RuntimeException("canceled");
            case FAILED:
            default:
              throw new RuntimeException("failed");
          }
        } else {
          Object o1 = parseJson((byte[]) o);
          if (holder != null) {
            o1 = ((Map<String, Object>) o1).get(holder);
          }
          if (fields == null) {
            current = o1;
          } else {
            final Map<String, Object> map = (Map<String, Object>) o1;
            if (fields.size() == 1) {
              current = map.get(fields.get(0));
            } else {
              Object[] os = new Object[fields.size()];
              for (int i = 0; i < os.length; i++) {
                os[i] = map.get(fields.get(i));
              }
              current = os;
            }
          }
          return true;
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException(e);
      }
    }

    public void reset() {
      throw new UnsupportedOperationException();
    }
  }


  /**
   * Runs the plan as a background task.
   */
  Future<Collection<RunOutcome>> runRefInterpreterPlan(
      CompletionService<Collection<RunOutcome>> service) {
    LogicalPlan parsedPlan = LogicalPlan.parse(DrillConfig.create(), plan);
    IteratorRegistry ir = new IteratorRegistry();
    DrillConfig config = DrillConfig.create();
    config.setSinkQueues(0, queue);
    final ReferenceInterpreter i =
        new ReferenceInterpreter(parsedPlan, ir, new BasicEvaluatorFactory(ir),
            new RSERegistry(config));
    try {
      i.setup();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return service.submit(
        new Callable<Collection<RunOutcome>>() {
          @Override
          public Collection<RunOutcome> call() throws Exception {
            Collection<RunOutcome> outcomes = i.run();

            for (RunOutcome outcome : outcomes) {
              System.out.println("============");
              System.out.println(outcome);
              if (outcome.outcome == RunOutcome.OutcomeType.FAILED
                  && outcome.exception != null) {
                outcome.exception.printStackTrace();
              }
            }
            return outcomes;
          }
        });
  }

  
  
  public Enumerator<E> enumerator() {
    // TODO: use a completion service from the container
    final ExecutorCompletionService<Collection<RunOutcome>> service = new ExecutorCompletionService<Collection<RunOutcome>>(
        new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(10)));

    // Run the plan using an executor. It runs in a different thread, writing
    // results to our queue.
    //
    // TODO: use the result of task, and check for exceptions
    final Future<Collection<RunOutcome>> task = runRefInterpreterPlan(service);

    return new JsonEnumerator(queue, fields);

  }
  
  /**
   * Converts a JSON document, represented as an array of bytes, into a Java
   * object (consisting of Map, List, String, Integer, Double, Boolean).
   */
  static Object parseJson(byte[] bytes) {
    try {
      return wrapper(mapper.readTree(bytes));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  


  /**
   * Converts a JSON node to Java objects ({@link List}, {@link Map},
   * {@link String}, {@link Integer}, {@link Double}, {@link Boolean}.
   */
  static Object wrapper(JsonNode node) {
    switch (node.asToken()) {
      case START_OBJECT:
        return map((ObjectNode) node);
      case START_ARRAY:
        return array((ArrayNode) node);
      case VALUE_STRING:
        return node.asText();
      case VALUE_NUMBER_INT:
        return node.asInt();
      case VALUE_NUMBER_FLOAT:
        return node.asDouble();
      case VALUE_TRUE:
        return Boolean.TRUE;
      case VALUE_FALSE:
        return Boolean.FALSE;
      case VALUE_NULL:
        return null;
      default:
        throw new AssertionError("unexpected: " + node + ": " + node.asToken());
    }
  }

  private static List<Object> array(ArrayNode node) {
    final List<Object> list = new ArrayList<>();
    for (JsonNode jsonNode : node) {
      list.add(wrapper(jsonNode));
    }
    return Collections.unmodifiableList(list);
  }

  private static SortedMap<String, Object> map(ObjectNode node) {
    // TreeMap makes the results deterministic.
    final TreeMap<String, Object> map = new TreeMap<>();
    final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> next = fields.next();
      map.put(next.getKey(), wrapper(next.getValue()));
    }
    return Collections.unmodifiableSortedMap(map);
  }

  
}
