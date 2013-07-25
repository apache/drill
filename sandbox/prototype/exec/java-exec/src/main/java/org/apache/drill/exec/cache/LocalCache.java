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
package org.apache.drill.exec.cache;

import java.io.IOException;
import java.util.Map;

import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;

import com.google.common.collect.Maps;

public class LocalCache implements DistributedCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalCache.class);

  private volatile Map<FragmentHandle, PlanFragment> handles;
  
  @Override
  public void close() throws IOException {
    handles = null;
  }

  @Override
  public void run() throws DrillbitStartupException {
    handles = Maps.newConcurrentMap();
  }

  @Override
  public PlanFragment getFragment(FragmentHandle handle) {
    logger.debug("looking for fragment with handle: {}", handle);
    return handles.get(handle);
  }

  @Override
  public void storeFragment(PlanFragment fragment) {
    logger.debug("Storing fragment: {}", fragment);
    handles.put(fragment.getHandle(), fragment);
  }
  
  
}
