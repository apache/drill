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
package org.apache.drill.exec.store.schedule;

import java.util.Iterator;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Comparator;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;

public class EndpointByteMapImpl implements EndpointByteMap{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EndpointByteMapImpl.class);

  private final ObjectLongHashMap<DrillbitEndpoint> map = new ObjectLongHashMap<>();

  private long maxBytes;

  public boolean isSet(DrillbitEndpoint endpoint){
    return map.containsKey(endpoint);
  }

  public long get(DrillbitEndpoint endpoint){
    return map.get(endpoint);
  }

  public boolean isEmpty(){
    return map.isEmpty();
  }

  public void add(DrillbitEndpoint endpoint, long bytes){
    assert endpoint != null;
    maxBytes = Math.max(maxBytes, map.putOrAdd(endpoint, bytes, bytes)+1);
  }

  public long getMaxBytes() {
    return maxBytes;
  }

  @Override
  public Iterator<ObjectLongCursor<DrillbitEndpoint>> iterator() {
    return map.iterator();
  }

  /**
   * Comparator used to sort in order of decreasing affinity
   */
  private static Comparator<Entry<DrillbitEndpoint, Long>> comparator = new Comparator<Entry<DrillbitEndpoint, Long>>() {
    @Override
    public int compare(Entry<DrillbitEndpoint, Long> o1, Entry<DrillbitEndpoint, Long> o2) {
      return (int) (o1.getValue() - o2.getValue());
    }
  };

  /*
   * Returns the list of DrillbitEndpoints which have (equal) highest value
   */
  public List<DrillbitEndpoint> getTopEndpoints() {
    List<Map.Entry<DrillbitEndpoint, Long>> entries = Lists.newArrayList();
    for (ObjectLongCursor<DrillbitEndpoint> cursor : map) {
      final DrillbitEndpoint ep = cursor.key;
      final Long val = cursor.value;
      Map.Entry<DrillbitEndpoint, Long> entry = new Entry<DrillbitEndpoint, Long>() {

        @Override
        public DrillbitEndpoint getKey() {
          return ep;
        }

        @Override
        public Long getValue() {
          return val;
        }

        @Override
        public Long setValue(Long value) {
          throw new UnsupportedOperationException();
        }
      };
      entries.add(entry);
    }

    if (entries.size() == 0) {
      return null;
    }

    Collections.sort(entries, comparator);
    Long value = entries.get(0).getValue();
    List<DrillbitEndpoint> topEndpoints = Lists.newArrayList();

    for (Entry<DrillbitEndpoint, Long> entry : entries) {
      if (entry.getValue().equals(value)) {
        topEndpoints.add(entry.getKey());
      } else {
        break;
      }
    }

    return topEndpoints;
  }
}
