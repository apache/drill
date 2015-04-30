/**
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
 */
package org.apache.drill.exec.store.store;

import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class TestAssignment {

  private static final long FILE_SIZE = 1000;
  private static List<DrillbitEndpoint> endpoints;

  @BeforeClass
  public static void setup() {
    endpoints = Lists.newArrayList();
    final String pattern = "node%d";
    for (int i = 2; i < 32; i++) {
      String host = String.format(pattern, i);
      endpoints.add(DrillbitEndpoint.newBuilder().setAddress(host).build());
    }
  }

  @Test
  public void manyFiles() throws Exception {
    List<CompleteFileWork> chunks = generateChunks(1000);

    Iterator<DrillbitEndpoint> incomingEndpointsIterator = Iterators.cycle(endpoints);

    List<DrillbitEndpoint> incomingEndpoints = Lists.newArrayList();

    final int width = 28 * 30;
    for (int i = 0; i < width; i++) {
      incomingEndpoints.add(incomingEndpointsIterator.next());
    }

    ListMultimap<Integer, CompleteFileWork> mappings = AssignmentCreator.getMappings(incomingEndpoints, chunks);
    System.out.println(mappings.keySet().size());
    for (int i = 0; i < width; i++) {
      Assert.assertTrue("no mapping for entry " + i, mappings.get(i) != null && mappings.get(i).size() > 0);
    }
  }

  private List<CompleteFileWork> generateChunks(int chunks) {
    List<CompleteFileWork> chunkList = Lists.newArrayList();
    for (int i = 0; i < chunks; i++) {
      CompleteFileWork chunk = new CompleteFileWork(createByteMap(), 0, FILE_SIZE, "file" + i);
      chunkList.add(chunk);
    }
    return chunkList;
  }

  private EndpointByteMap createByteMap() {
    EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
    Set<DrillbitEndpoint> usedEndpoints = Sets.newHashSet();
    while (usedEndpoints.size() < 3) {
      usedEndpoints.add(getRandom(endpoints));
    }
    for (DrillbitEndpoint ep : usedEndpoints) {
      endpointByteMap.add(ep, FILE_SIZE);
    }
    return endpointByteMap;
  }

  private <T> T getRandom(List<T> list) {
    int index = ThreadLocalRandom.current().nextInt(list.size());
    return list.get(index);
  }
}
