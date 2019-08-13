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
package org.apache.drill.exec.udfs;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

public class ProtocolFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtocolFunctions.class);

  private ProtocolFunctions() {
  }

  /* This function takes a port number and protocol and returns the associated service name, and Unknown if there is an error */

  @FunctionTemplate(name = "get_service_name", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class ServiceNameLookup implements DrillSimpleFunc {
    @Param
    IntHolder portNumber;

    @Param
    VarCharHolder protocol;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    java.util.HashMap<String, String> serviceInfo;


    @Override
    public void setup() {
      serviceInfo = org.apache.drill.exec.udfs.SecurityHelperFunctions.getPortHashMap();
    }

    @Override
    public void eval() {
      String selectedProtocol = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(protocol.start, protocol.end, protocol.buffer);
      selectedProtocol = selectedProtocol.toLowerCase();
      int port = portNumber.value;

      String serviceName = "Unknown";
      try {
        serviceName = (String) serviceInfo.get(port + ":" + selectedProtocol);
        if (serviceName == null) {
          serviceName = "Unknown";
        }
      } catch (Exception e) {
        serviceName = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = serviceName.getBytes().length;
      buffer.setBytes(0, serviceName.getBytes());
    }
  }

  /**
   * This function accepts a port number that is a string and returns the service naem
   *
   * @Param portNumber
   * @Param protocol (Should be TCP or UDP)
   */
  @FunctionTemplate(name = "get_service_name", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class ServiceNameLookupString implements DrillSimpleFunc {
    @Param
    VarCharHolder portNumber;

    @Param
    VarCharHolder protocol;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    java.util.HashMap<String, String> serviceInfo;


    @Override
    public void setup() {
      serviceInfo = org.apache.drill.exec.udfs.SecurityHelperFunctions.getPortHashMap();
    }

    @Override
    public void eval() {
      String selectedProtocol = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(protocol.start, protocol.end, protocol.buffer);
      selectedProtocol = selectedProtocol.toLowerCase();

      String portNumString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(portNumber.start, portNumber.end, portNumber.buffer);

      int port = Integer.parseInt(portNumString);

      String serviceName = "Unknown";
      try {
        serviceName = (String) serviceInfo.get(port + ":" + selectedProtocol);
        if (serviceName == null) {
          serviceName = "Unknown";
        }
      } catch (Exception e) {
        serviceName = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = serviceName.getBytes().length;
      buffer.setBytes(0, serviceName.getBytes());
    }
  }

  /* This function takes a port number and protocol and returns the associated service name, and "Unknown if there is an error */

  @FunctionTemplate(name = "get_short_service_name", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class ShortServiceNameLookup implements DrillSimpleFunc {
    @Param
    IntHolder portNumber;

    @Param
    VarCharHolder protocol;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    java.util.HashMap<String, String> serviceInfo;


    @Override
    public void setup() {
      serviceInfo = org.apache.drill.exec.udfs.SecurityHelperFunctions.getPortHashMap();
    }

    @Override
    public void eval() {
      String selectedProtocol = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(protocol.start, protocol.end, protocol.buffer);
      selectedProtocol = selectedProtocol.toLowerCase();
      int port = portNumber.value;

      String serviceName = "Unknown";
      try {
        serviceName = (String) serviceInfo.get(port + ":" + selectedProtocol);
        if (serviceName == null) {
          serviceName = "Unknown";
        }
      } catch (Exception e) {
        serviceName = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = serviceName.getBytes().length;
      buffer.setBytes(0, serviceName.getBytes());
    }
  }

  /* This function takes a port number and protocol and returns the associated service name, and "Unknown if there is an error */

  @FunctionTemplate(name = "get_short_service_name", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class ShortStringServiceNameLookup implements DrillSimpleFunc {
    @Param
    VarCharHolder portNumber;

    @Param
    VarCharHolder protocol;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    java.util.HashMap<String, String> serviceInfo;


    @Override
    public void setup() {
      serviceInfo = org.apache.drill.exec.udfs.SecurityHelperFunctions.getPortHashMap();
    }

    @Override
    public void eval() {
      String selectedProtocol = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(protocol.start, protocol.end, protocol.buffer);
      selectedProtocol = selectedProtocol.toLowerCase();
      String portNumString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(portNumber.start, portNumber.end, portNumber.buffer);
      int port = Integer.parseInt(portNumString);

      String serviceName = "Unknown";
      try {
        serviceName = (String) serviceInfo.get(port + ":" + selectedProtocol);
        if (serviceName == null) {
          serviceName = "Unknown";
        }
      } catch (Exception e) {
        serviceName = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = serviceName.getBytes().length;
      buffer.setBytes(0, serviceName.getBytes());
    }
  }
}
