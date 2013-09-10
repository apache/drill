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
package org.apache.drill.sql.client.full;

import java.util.List;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.optiq.DataContext;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.RpcException;

/**
   * Enumerator used for full execution engine.
   */
  class ResultEnumerator implements Enumerator<Object> {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResultEnumerator.class);
    
    private final BatchLoaderMap loaderMap;
    private Object current;
    
    public ResultEnumerator(BatchListener listener, DrillClient client, List<String> fields) {
      this.loaderMap = new BatchLoaderMap(fields, listener, client);
    }

    public Object current() {
      return current;
    }

    public boolean moveNext() {
      
      try {
        boolean succ = loaderMap.next();
        if(succ){
          current = loaderMap.getCurrentObject();         
        }
        return succ;
        
      } catch (InterruptedException e) {
        Thread.interrupted();
        logger.error("Exception during query", e);
        throw new RuntimeException(e);
      } catch (RpcException | SchemaChangeException e) {
        logger.error("Exception during query", e);
        throw new RuntimeException(e);
      }
    }

    public void reset() {
      throw new UnsupportedOperationException();
    }
    
    public void close(){
      
    }
  }