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
package org.apache.drill.exec.work;

import java.util.UUID;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.slf4j.Logger;


public class ErrorHelper {
  
  public static DrillPBError logAndConvertError(DrillbitEndpoint endpoint, String message, Throwable t, Logger logger){
    String id = UUID.randomUUID().toString();
    DrillPBError.Builder builder = DrillPBError.newBuilder();
    builder.setEndpoint(endpoint);
    builder.setErrorId(id);
    StringBuilder sb = new StringBuilder();
    if(message != null){
      sb.append(message);
    }

    while (true) {
      sb.append(" < ");
      sb.append(t.getClass().getSimpleName());
      if(t.getMessage() != null){
        sb.append(":[ ");
        sb.append(t.getMessage());
        sb.append(" ]");
      }
      if (t.getCause() == null || t.getCause() == t) break;
      t = t.getCause();
    }
    
    builder.setMessage(sb.toString());
    

    builder.setErrorType(0);
    
    // record the error to the log for later reference.
    logger.error("Error {}: {}", id, message, t);
    
    
    return builder.build();
  }
}
