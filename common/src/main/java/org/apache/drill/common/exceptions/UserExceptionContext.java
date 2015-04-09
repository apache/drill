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
package org.apache.drill.common.exceptions;

import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Holds context information about a DrillUserException. We can add structured context information that will be used
 * to generate the error message displayed to the client. We can also specify which context information should only
 * be displayed in verbose mode
 */
public class UserExceptionContext {

  private final String errorId;
  private final List<String> contextList;

  private CoordinationProtos.DrillbitEndpoint endpoint;

  UserExceptionContext() {
    errorId = UUID.randomUUID().toString();
    contextList = new ArrayList<>();
  }

  UserExceptionContext(UserExceptionContext context) {
    this.errorId = context.errorId;
    this.contextList = context.contextList;
    this.endpoint = context.endpoint;
  }
  /**
   * adds a context line to the bottom of the context list
   * @param context context line
   */
  public UserExceptionContext add(String context) {
    contextList.add(context);
    return this;
  }

  public UserExceptionContext add(CoordinationProtos.DrillbitEndpoint endpoint) {
    //TODO should we allos the endpoint to change once set ?
    this.endpoint = endpoint;
    return this;
  }

  /**
   * adds an int to the bottom of the context list
   * @param context context prefix string
   * @param value int value
   */
  public UserExceptionContext add(String context, long value) {
    add(context + ": " + value);
    return this;
  }

  /**
   * adds a double to the bottom of the context list
   * @param context context prefix string
   * @param value double value
   */
  public UserExceptionContext add(String context, double value) {
    add(context + ": " + value);
    return this;
  }

  /**
   * adds a context line at the top of the context list
   * @param context context line
   */
  public UserExceptionContext push(String context) {
    contextList.add(0, context);
    return this;
  }

  /**
   * adds an int at the top of the context list
   * @param context context prefix string
   * @param value int value
   */
  public UserExceptionContext push(String context, long value) {
    push(context + ": " + value);
    return this;
  }

  /**
   * adds a double at the top of the context list
   * @param context context prefix string
   * @param value double value
   */
  public UserExceptionContext push(String context, double value) {
    push(context + ": " + value);
    return this;
  }

  String getErrorId() {
    return errorId;
  }

  CoordinationProtos.DrillbitEndpoint getEndpoint() {
    return endpoint;
  }

  /**
   * generate a context message
   * @return string containing all context information concatenated
   */
  String generateContextMessage() {
    StringBuilder sb = new StringBuilder();

    for (String context : contextList) {
      sb.append(context).append("\n");
    }

    if (errorId != null || endpoint != null) {
      // add identification infos
      sb.append("\n[");
      if (errorId != null) {
        sb.append(errorId).append(" ");
      }
      if(endpoint != null) {
        sb.append("on ")
          .append(endpoint.getAddress())
          .append(":")
          .append(endpoint.getUserPort());
      }
      sb.append("]\n");
    }

    return sb.toString();
  }
}
