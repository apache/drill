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

import java.util.Collection;

import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.DrillbitContext;

import com.fasterxml.jackson.databind.ObjectMapper;

public class QueryContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryContext.class);
  
  private long queryId;
  private DrillbitContext drillbitContext;
  
  public QueryContext(long queryId, DrillbitContext drllbitContext) {
    super();
    this.queryId = queryId;
    this.drillbitContext = drllbitContext;
  }
  
  public long getQueryId() {
    return queryId;
  }
  
  public ObjectMapper getMapper(){
    return drillbitContext.getConfig().getMapper();
  }
  
  public Collection<DrillbitEndpoint> getActiveEndpoints(){
    return drillbitContext.getBits();
  }
  
}
