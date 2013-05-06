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
package org.apache.drill.exec.rpc.bit;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.RpcException;

import com.google.common.util.concurrent.CheckedFuture;

public class BitConnectionManager {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitConnectionManager.class);
  
  private final int maxAttempts;
  private final BitComImpl com;
  private final DrillbitEndpoint endpoint;
  private final AtomicReference<BitConnection> connection;
  private final AtomicReference<CheckedFuture<BitConnection, RpcException>> future;

  BitConnectionManager(DrillbitEndpoint endpoint, BitComImpl com, BitConnection connection, CheckedFuture<BitConnection, RpcException> future, int maxAttempts) {
    assert endpoint != null && endpoint.getAddress() != null && endpoint.getBitPort() > 0;
    this.com = com;
    this.connection =  new AtomicReference<BitConnection>(connection);
    this.future = new AtomicReference<CheckedFuture<BitConnection, RpcException>>(future);
    this.endpoint = endpoint;
    this.maxAttempts = maxAttempts;
  }
  
  BitConnection getConnection(int attempt) throws RpcException{
    BitConnection con = connection.get();
    
    if(con != null){
      if(con.isActive()) return con;
      connection.compareAndSet(con, null);
    }
    
    CheckedFuture<BitConnection, RpcException> fut = future.get();

    if(fut != null){
      try{
        return fut.checkedGet();
      }catch(RpcException ex){
        future.compareAndSet(fut, null);
        if(attempt < maxAttempts){
          return getConnection(attempt + 1);
        }else{
          throw ex;
        }
      }
    }
    
    // no checked future, let's make one.
    fut = com.getConnectionAsync(endpoint);
    future.compareAndSet(null, fut);
    return getConnection(attempt);
    
  }

  public DrillbitEndpoint getEndpoint() {
    return endpoint;
  }
  
  
}
