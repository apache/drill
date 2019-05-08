/*
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
package org.apache.drill.common;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

/**
 * DrillNode encapsulates a drillendpoint. DrillbitEndpoint is a protobuf generated class which requires
 * all the member variables to be equal for DrillbitEndpoints to be equal. DrillNode relaxes this requirement
 * by only comparing required variables.
 */
public class DrillNode {
  private final DrillbitEndpoint endpoint;

  public DrillNode(DrillbitEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  public static DrillNode create(DrillbitEndpoint endpoint) {
    return new DrillNode(endpoint);
  }

  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof DrillNode)) {
      return super.equals(obj);
    }
    DrillbitEndpoint other = ((DrillNode)obj).endpoint;

    boolean result = true;
    result = result && (endpoint.hasAddress() == other.hasAddress());
    if (endpoint.hasAddress()) {
      result = result && endpoint.getAddress()
        .equals(other.getAddress());
    }
    result = result && (endpoint.hasUserPort() == other.hasUserPort());
    if (endpoint.hasUserPort()) {
      result = result && (endpoint.getUserPort() == other.getUserPort());
    }
    result = result && (endpoint.hasControlPort() == other.hasControlPort());
    if (endpoint.hasControlPort()) {
      result = result && (endpoint.getControlPort() == other.getControlPort());
    }
    result = result && (endpoint.hasDataPort() == other.hasDataPort());
    if (endpoint.hasDataPort()) {
      result = result && (endpoint.getDataPort() == other.getDataPort());
    }
    result = result && (endpoint.hasVersion() == other.hasVersion());
    if (endpoint.hasVersion()) {
      result = result && endpoint.getVersion().equals(other.getVersion());
    }
    return result;
  }

  @Override
  public int hashCode() {
    int hash = 41;
    hash = (19 * hash) + endpoint.getDescriptor().hashCode();
    if (endpoint.hasAddress()) {
      hash = (37 * hash) + endpoint.ADDRESS_FIELD_NUMBER;
      hash = (53 * hash) + endpoint.getAddress().hashCode();
    }
    if (endpoint.hasUserPort()) {
      hash = (37 * hash) + endpoint.USER_PORT_FIELD_NUMBER;
      hash = (53 * hash) + endpoint.getUserPort();
    }
    if (endpoint.hasControlPort()) {
      hash = (37 * hash) + endpoint.CONTROL_PORT_FIELD_NUMBER;
      hash = (53 * hash) + endpoint.getControlPort();
    }
    if (endpoint.hasDataPort()) {
      hash = (37 * hash) + endpoint.DATA_PORT_FIELD_NUMBER;
      hash = (53 * hash) + endpoint.getDataPort();
    }
    if (endpoint.hasVersion()) {
      hash = (37 * hash) + endpoint.VERSION_FIELD_NUMBER;
      hash = (53 * hash) + endpoint.getVersion().hashCode();
    }
    return hash;
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    return sb.append("endpoint address :")
      .append(endpoint.hasAddress() ? endpoint.getAddress() : "no-address")
      .append("endpoint user port: ")
      .append(endpoint.hasUserPort() ? endpoint.getUserPort() : "no-userport").toString();
  }
}
