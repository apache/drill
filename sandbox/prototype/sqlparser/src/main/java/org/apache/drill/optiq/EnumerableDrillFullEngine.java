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

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.optiq.DataContext;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.jdbc.DrillHandler.FakeSchema;
import org.apache.drill.sql.client.full.DrillFullImpl;
import org.apache.drill.sql.client.ref.DrillRefImpl;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Runtime helper that executes a Drill query and converts it into an {@link Enumerable}.
 */
public class EnumerableDrillFullEngine<E> extends AbstractEnumerable<E> implements Enumerable<E> {
  private final String plan;
  final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(100);
  final DrillConfig config;
  private final List<String> fields;
  private DrillClient client;
  private DataContext context;
  
  /**
   * Creates a DrillEnumerable.
   * 
   * @param plan
   *          Logical plan
   * @param clazz
   *          Type of elements returned from enumerable
   * @param fields
   *          Names of fields, or null to return the whole blob
   */
  public EnumerableDrillFullEngine(DrillConfig config, String plan, Class<E> clazz, List<String> fields,
      DrillClient client, DataContext context) {
    this.plan = plan;
    this.config = config;
    this.fields = fields;
    this.client = client;
    this.context = context;
    config.setSinkQueues(0, queue);
  }

  /**
   * Creates a DrillEnumerable from a plan represented as a string. Each record returned is a {@link JsonNode}.
   */
  public static <E> EnumerableDrillFullEngine<E> of(String plan, final List<String> fieldNames, Class<E> clazz,
      DataContext context) {
    DrillConfig config = DrillConfig.create();
    return new EnumerableDrillFullEngine<>(config, plan, clazz, fieldNames, ((FakeSchema) context.getSubSchema("--FAKE--")).getClient(), context);
  }

  @Override
  public Enumerator<E> enumerator() {
    
    if (client == null) {
      DrillRefImpl<E> impl = new DrillRefImpl<E>(plan, config, fields, queue);
      return impl.enumerator();
    } else {
      DrillFullImpl<E> impl = new DrillFullImpl<E>(plan, config, fields, context);
      return impl.enumerator(client);
    }
  }

}
