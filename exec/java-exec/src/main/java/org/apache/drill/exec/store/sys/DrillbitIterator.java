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
package org.apache.drill.exec.store.sys;

import java.util.Iterator;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

public class DrillbitIterator implements Iterator<Object> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillbitIterator.class);

  private Iterator<DrillbitEndpoint> endpoints;
  private DrillbitEndpoint current;

  public DrillbitIterator(FragmentContext c) {
    this.endpoints = c.getDrillbitContext().getBits().iterator();
    this.current = c.getIdentity();
  }

  public static class DrillbitInstance {
    public String hostname;
    public int user_port;
    public int control_port;
    public int data_port;
    public boolean current;
  }

  @Override
  public boolean hasNext() {
    return endpoints.hasNext();
  }

  @Override
  public Object next() {
    DrillbitEndpoint ep = endpoints.next();
    DrillbitInstance i = new DrillbitInstance();
    i.current = ep.equals(current);
    i.hostname = ep.getAddress();
    i.user_port = ep.getUserPort();
    i.control_port = ep.getControlPort();
    i.data_port = ep.getDataPort();
    return i;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
