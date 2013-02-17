/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.optiq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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

import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Runtime helper that executes a Drill query and converts it into an
 * {@link Enumerable}.
 */
public class EnumerableDrill<E>
    extends AbstractEnumerable<E>
    implements Enumerable<E> {
  private final LogicalPlan plan;
  final BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(100);
  final DrillConfig config;
  
  private static final ObjectMapper mapper = createMapper();

  /** Creates a DrillEnumerable.
   *
   * @param plan Logical plan
   * @param clazz Type of elements returned from enumerable
   */
  public EnumerableDrill(DrillConfig config, LogicalPlan plan, Class<E> clazz) {
    this.plan = plan;
    this.config = config;
    config.setSinkQueues(0, queue);
  }

  /** Creates a DrillEnumerable from a plan represented as a string. */
  public static <E extends JsonNode> EnumerableDrill<E> of(String plan,
      Class<E> clazz) {
    DrillConfig config = DrillConfig.create();
    return new EnumerableDrill<E>(config, LogicalPlan.parse(config, plan), clazz);
  }

  /** Runs the plan as a background task. */
  Future<Collection<RunOutcome>> runPlan(
      CompletionService<Collection<RunOutcome>> service) {
    IteratorRegistry ir = new IteratorRegistry();
    DrillConfig config = DrillConfig.create();
    config.setSinkQueues(0, queue);
    final ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
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

  @Override
  public Enumerator<E> enumerator() {
    // TODO: use a completion service from the container
    final ExecutorCompletionService<Collection<RunOutcome>> service =
        new ExecutorCompletionService<Collection<RunOutcome>>(
            new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>(10)));

    // Run the plan using an executor. It runs in a different thread, writing
    // results to our queue.
    //
    // TODO: use the result of task, and check for exceptions
    final Future<Collection<RunOutcome>> task = runPlan(service);

    return new Enumerator<E>() {
      private E current;

      @Override
      public E current() {
        return current;
      }

      @Override
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
            current = (E) parseJson((byte[]) o);
            return true;
          }
        } catch (InterruptedException e) {
          Thread.interrupted();
          throw new RuntimeException(e);
        }
      }

      @Override
      public void reset() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static ObjectMapper createMapper() {
    return new ObjectMapper();
  }

  /** Converts a JSON document, represented as an array of bytes, into a Java
   * object (consisting of Map, List, String, Integer, Double, Boolean). */
  static Object parseJson(byte[] bytes) {
    try {
      return wrapper(mapper.readTree(bytes));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Converts a JSON node to Java objects ({@link List}, {@link Map},
   * {@link String}, {@link Integer}, {@link Double}, {@link Boolean}. */
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

// End EnumerableDrill.java
