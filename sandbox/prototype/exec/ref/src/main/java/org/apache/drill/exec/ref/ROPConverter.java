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
package org.apache.drill.exec.ref;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Queue;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.Aggregate;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Union;
import org.apache.drill.common.logical.data.Write;
import org.apache.drill.common.logical.sources.DataSource;
import org.apache.drill.common.logical.sources.JSONDataSource;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.rops.*;

class ROPConverter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ROPConverter.class);

  private LogicalPlan plan;
  private IteratorRegistry registry;
  private EvaluatorFactory builder;
  private final List<Queue> sinkQueues;

  public ROPConverter(LogicalPlan plan, IteratorRegistry registry, EvaluatorFactory builder, List<Queue> sinkQueues) {
    this.plan = plan;
    this.registry = registry;
    this.builder = builder;
    this.sinkQueues = sinkQueues;
  }

  public void convert(LogicalOperator o) throws SetupException {

    try {
      Method m = ROPConverter.class.getMethod("convertSpecific", o.getClass());
      m.invoke(this, o);
      return;
    } catch (NoSuchMethodException e) {
      // noop
      logger.debug("There is no convertSpecific method for type {}.  Looking for a generic option...", o.getClass());
    } catch (IllegalAccessException | IllegalArgumentException | SecurityException e) {
      logger.debug("Failure while attempting to run convertSpecific value for class {}", o.getClass().getSimpleName(),
          e);
    } catch (InvocationTargetException e) {
      Throwable c = e.getCause();
      if (c instanceof SetupException) {
        throw (SetupException) c;
      } else {
        throw new RuntimeException("Failure while trying to run convertSpecifc conversion of operator type "
            + o.getClass().getSimpleName(), c);
      }
    }

    String name = ROP.class.getPackage().getName() + "." + o.getClass().getSimpleName() + "ROP";
    try {
      Class<?> c = Class.forName(name);
      if (!ROP.class.isAssignableFrom(c)) throw new IllegalArgumentException("Class does not derive from ROP.");
      Constructor<?> ctor = c.getConstructor(o.getClass());
      if (ctor == null)
        throw new IllegalArgumentException(
            "Class does not have an available constructor that supports a single argument of the class "
                + o.getClass().getSimpleName());
      ROP r = (ROP) ctor.newInstance(o);
      r.init(registry, builder);
      return;

    } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      logger.debug("No {} class that accepts a single parameter or type {}.", name, o.getClass().getCanonicalName());
    } catch (InvocationTargetException e) {
      Throwable c = e.getCause();
      if (c instanceof SetupException) throw (SetupException) c;
      throw new SetupException("Failure while trying to run Convert node of type " + o.getClass().getSimpleName(), c);
    }

    throw new UnsupportedOperationException("Unable to convert Logical Operator of type "
        + o.getClass().getCanonicalName());
  }

  public void convertSpecific(Write write) throws SetupException {
    if (write.getFileName().startsWith("socket:")) {
      final String ordinalString =
          write.getFileName().substring("socket:".length());
      final Queue queue;
      try {
        int ordinal = Integer.valueOf(ordinalString);
        queue = sinkQueues.get(ordinal);
      } catch (IndexOutOfBoundsException | NumberFormatException e) {
        throw new RuntimeException("bad socket ordinal " + ordinalString);
      }
      final QueueSinkROP queueSink = new QueueSinkROP(write, queue);
      queueSink.init(registry, builder);
    } else {
      JSONWriter writer = new JSONWriter(write);
      writer.init(registry, builder);
    }
  }

  public void convertSpecific(Aggregate agg) throws SetupException {
    if(agg.getAggregateType().equals("simple")){
      ROP r = new AggregateSimpleROP(agg);
      r.init(registry, builder);
    }else{
      throw new SetupException(String.format("Currently, the reference interpreter does not support aggregators of type '%s'.  It only supports simple aggregators.", agg.getAggregateType()));
    }
  }

  public void convertSpecific(Scan scan) throws SetupException {
    try {
      DataSource ds = plan.getDataSource(scan.getSourceName());

      if (ds instanceof JSONDataSource) {
        JSONDataSource js = (JSONDataSource) ds;
        List<String> files = js.files;
        if (files.size() > 1) {
          Union logOp = new Union(null);

          ROP parentUnion = new UnionROP(logOp);
          ROP[] scanners = new ROP[files.size()];
          for (int i = 0; i < files.size(); i++) {
            scanners[i] = new JSONScanner(scan, files.get(i));
            scanners[i].init(registry, builder);
          }

          parentUnion.init(registry, builder);
          registry.swap(logOp, scan); // make it so future things point to
        } else {
          JSONScanner scanner = new JSONScanner(scan, files.get(0));
          scanner.init(registry, builder);
        }
      } else {
        fail(ds);
      }
    } catch (IOException e) {
      throw new SetupException("Failure while trying to set up JSON reading node.", e);
    }
  }

  private void fail(Object o) {
    throw new UnsupportedOperationException(String.format(
        "Reference implementation doesn't support interactions with [%s].", o.getClass().getSimpleName()));
  }
}
