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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.UUID;

import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.planner.sql.parser.impl.ParseException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.eigenbase.sql.parser.SqlParseException;
import org.slf4j.Logger;


public class ErrorHelper {

//  public static DrillPBError logAndConvertError(DrillbitEndpoint endpoint, String message, Throwable t, Logger logger) {
//    return logAndConvertError(endpoint, message, t, logger, true);
//  }

  public static DrillPBError logAndConvertError(DrillbitEndpoint endpoint, String message, Throwable t, Logger logger,
                                                boolean verbose){
    String id = UUID.randomUUID().toString();
    DrillPBError.Builder builder = DrillPBError.newBuilder();
    builder.setEndpoint(endpoint);
    builder.setErrorId(id);
    StringBuilder sb = new StringBuilder();
    if (message != null) {
      sb.append(message).append(" ");
    }
    sb.append("%s ").append("[").append(id).append("]");
    if (verbose) {
      sb.append("\n")
        .append("Node details: ")
        .append(endpoint.getAddress())
        .append(":").append(endpoint.getControlPort())
        .append("/").append(endpoint.getDataPort());
    }

    if (verbose) {
      StringWriter errors = new StringWriter();
      errors.write("\n");
      t.printStackTrace(new PrintWriter(errors));
      sb.append(errors);
    }

    Throwable original = t;
    Throwable rootCause = null;
    while (true) {
      rootCause = t;
      if (t.getCause() == null || t.getCause() == t
        || (t instanceof SqlParseException && t.getCause() instanceof ParseException)) break;
      t = t.getCause();
    }

    String finalMsg = rootCause.getMessage() == null ? original.getMessage() : rootCause.getMessage();
    builder.setMessage(String.format(sb.toString(), finalMsg));
    builder.setErrorType(0);
    
    // record the error to the log for later reference.
    logger.error("Error {}: {}", id, message, t);

    return builder.build();
  }
}
