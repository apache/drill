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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Runtime helper that executes a Drill query and converts it into an
 * {@link Enumerable}.
 */
public class EnumerableDrill<E extends JsonNode>
    extends AbstractEnumerable<E>
    implements Enumerable<E> {
  private final LogicalPlan plan;
  final BlockingQueue queue = new ArrayBlockingQueue(100);

  /** Creates a DrillEnumerable.
   *
   * @param plan Logical plan
   * @param clazz Type of elements returned from enumerable
   */
  public EnumerableDrill(LogicalPlan plan, Class<E> clazz) {
    this.plan = plan;
  }

  /** Creates a DrillEnumerable from a plan represented as a string. */
  public static <E extends JsonNode> EnumerableDrill<E> of(String plan,
      Class<E> clazz) {
    return new EnumerableDrill<E>(LogicalPlan.parse(plan), clazz);
  }

  /** Runs the plan as a background task. */
  Future<Collection<RunOutcome>> runPlan(
      CompletionService<Collection<RunOutcome>> service) {
    IteratorRegistry ir = new IteratorRegistry();

    final ReferenceInterpreter i = new ReferenceInterpreter(plan, ir,
        new BasicEvaluatorFactory(ir), Collections.singletonList((Queue) queue));
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
            new ThreadPoolExecutor( 1, 1, 1, TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>(10)));

    // Run the plan using an executor. It runs in a different thread, writing
    // results to our queue.
    //
    // TODO: use the result of task, and check for exceptions
    final Future<Collection<RunOutcome>> task = runPlan(service);

    return new Enumerator<E>() {
      final ObjectMapper mapper = new ObjectMapper();
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

      JsonNode parseJson(byte[] bytes) {
        try {
          return mapper.readTree(bytes);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}

// End EnumerableDrill.java
