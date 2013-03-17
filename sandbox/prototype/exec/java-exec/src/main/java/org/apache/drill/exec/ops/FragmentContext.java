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
package org.apache.drill.exec.ops;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.rpc.bit.BitCom;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StorageEngine;

public class FragmentContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentContext.class);

  private final DrillbitContext context;
  
  public FragmentContext(DrillbitContext context) {
    this.context = context;
  }

  public void fail(Throwable cause) {

  }

  public DrillbitContext getDrillbitContext(){
    return context;
  }
  
  public StorageEngine getStorageEngine(StorageEngineConfig config){
    return null;
  }
  
  public BitCom getCommunicator(){
    return null;
  }
}
