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

package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

public class NetworkFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NetworkFunctions.class);

  private NetworkFunctions() {
  }

  /**
   * This function takes two arguments, an input IPv4 and a CIDR, and returns true if the IP is in the given CIDR block
   */
  @FunctionTemplate(name = "in_network", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class InNetworkFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputIP;

    @Param
    VarCharHolder inputCIDR;

    @Output
    BitHolder out;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }


    public void eval() {

      String ipString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputIP.start, inputIP.end, inputIP.buffer);
      String cidrString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputCIDR.start, inputCIDR.end, inputCIDR.buffer);

      int result = 0;
      org.apache.commons.net.util.SubnetUtils utils = new org.apache.commons.net.util.SubnetUtils(cidrString);

      if (utils.getInfo().isInRange(ipString)) {
        result = 1;
      }

      out.value = result;
    }
  }


  /**
   * This function retunrs the number of IP addresses in the input CIDR block.
   */
  @FunctionTemplate(name = "address_count", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class AddressCountFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputCIDR;

    @Output
    BigIntHolder out;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }

    public void eval() {

      String cidrString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputCIDR.start, inputCIDR.end, inputCIDR.buffer);
      org.apache.commons.net.util.SubnetUtils utils = new org.apache.commons.net.util.SubnetUtils(cidrString);

      out.value = utils.getInfo().getAddressCount();

    }

  }

  /**
   * This function returns the broadcast address of a given CIDR block.
   */
  @FunctionTemplate(name = "broadcast_address", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class BroadcastAddressFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputCIDR;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }

    public void eval() {

      String cidrString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputCIDR.start, inputCIDR.end, inputCIDR.buffer);
      org.apache.commons.net.util.SubnetUtils utils = new org.apache.commons.net.util.SubnetUtils(cidrString);

      String outputValue = utils.getInfo().getBroadcastAddress();

      out.buffer = buffer;
      out.start = 0;
      out.end = outputValue.getBytes().length;
      buffer.setBytes(0, outputValue.getBytes());

    }

  }

  /**
   * This function gets the netmask of the input CIDR block.
   */

  @FunctionTemplate(name = "netmask", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class NetmaskFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputCIDR;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }

    public void eval() {

      String cidrString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputCIDR.start, inputCIDR.end, inputCIDR.buffer);
      org.apache.commons.net.util.SubnetUtils utils = new org.apache.commons.net.util.SubnetUtils(cidrString);

      String outputValue = utils.getInfo().getNetmask();

      out.buffer = buffer;
      out.start = 0;
      out.end = outputValue.getBytes().length;
      buffer.setBytes(0, outputValue.getBytes());

    }

  }

  /**
   * This function gets the numerically lowest IP address in an input CIDR block.
   */
  @FunctionTemplate(name = "low_address", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class LowAddressFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputCIDR;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }

    public void eval() {

      String cidrString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputCIDR.start, inputCIDR.end, inputCIDR.buffer);
      org.apache.commons.net.util.SubnetUtils utils = new org.apache.commons.net.util.SubnetUtils(cidrString);

      String outputValue = utils.getInfo().getLowAddress();

      out.buffer = buffer;
      out.start = 0;
      out.end = outputValue.getBytes().length;
      buffer.setBytes(0, outputValue.getBytes());

    }

  }

  /**
   * This function gets the numerically highest IP address in an input CIDR block.
   */
  @FunctionTemplate(name = "high_address", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class HighAddressFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputCIDR;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }

    public void eval() {

      String cidrString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputCIDR.start, inputCIDR.end, inputCIDR.buffer);
      org.apache.commons.net.util.SubnetUtils utils = new org.apache.commons.net.util.SubnetUtils(cidrString);

      String outputValue = utils.getInfo().getHighAddress();

      out.buffer = buffer;
      out.start = 0;
      out.end = outputValue.getBytes().length;
      buffer.setBytes(0, outputValue.getBytes());

    }
  }

  /**
   * This function encodes URL strings.
   */
  @FunctionTemplate(name = "url_encode", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class urlencodeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputString;

    @Output
    VarCharHolder outputString;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }

    public void eval() {

      String url = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputString.start, inputString.end, inputString.buffer);

      String outputValue = "";
      try {
        outputValue = java.net.URLEncoder.encode(url, "UTF-8");
      } catch (Exception e) {

      }
      outputString.buffer = buffer;
      outputString.start = 0;
      outputString.end = outputValue.getBytes().length;
      buffer.setBytes(0, outputValue.getBytes());

    }
  }

  /**
   * This function decodes URL strings.
   */
  @FunctionTemplate(name = "url_decode", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class urldecodeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputString;

    @Output
    VarCharHolder outputString;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }

    public void eval() {

      String url = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputString.start, inputString.end, inputString.buffer);

      String outputValue = "";
      try {
        outputValue = java.net.URLDecoder.decode(url, "UTF-8");
      } catch (Exception e) {

      }
      outputString.buffer = buffer;
      outputString.start = 0;
      outputString.end = outputValue.getBytes().length;
      buffer.setBytes(0, outputValue.getBytes());

    }
  }


  /**
   * This function converts a BigInt IPv4 into dotted decimal notation.  The opposite of inet_aton.
   */

  @FunctionTemplate(name = "inet_ntoa", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class InetNtoaFunction implements DrillSimpleFunc {

    @Param
    BigIntHolder in;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;


    public void setup() {
    }


    public void eval() {
      StringBuilder result = new StringBuilder(15);

      long inputInt = in.value;

      for (int i = 0; i < 4; i++) {

        result.insert(0, Long.toString(inputInt & 0xff));

        if (i < 3) {
          result.insert(0, '.');
        }

        inputInt = inputInt >> 8;
      }

      String outputValue = result.toString();

      out.buffer = buffer;
      out.start = 0;
      out.end = outputValue.getBytes().length;
      buffer.setBytes(0, outputValue.getBytes());
    }


  }

  /**
   * This function returns true if a given IPv4 address is private, false if not.
   */

  @FunctionTemplate(name = "is_private_ip", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class IsPrivateIP implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Inject
    DrillBuf buffer;


    public void setup() {
    }


    public void eval() {
      String ipString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);

      String[] ipAddressInArray = ipString.split("\\.");

      int result = 0;

      int[] octets = new int[3];

      for (int i = 0; i < 3; i++) {
        octets[i] = Integer.parseInt(ipAddressInArray[i]);
      }

      if (octets[0] == 192 && octets[1] == 168) {
        result = 1;
      } else if (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31) {
        result = 1;
      } else if (octets[0] == 10) {
        result = 1;
      } else {
        result = 0;
      }

      out.value = result;
    }
  }

  /**
   * This function converts an IPv4 address into a BigInt.  Useful for sorting IPs, or looking for IP ranges.
   * IE:
   * SELECT *
   * FROM <data>
   * ORDER BY inet_aton( ip ) ASC
   */
  @FunctionTemplate(name = "inet_aton", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class InetAtonFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BigIntHolder out;

    @Inject
    DrillBuf buffer;


    public void setup() {
    }


    public void eval() {
      String ipString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      if (ipString == null || ipString.isEmpty()) {
        out.value = 0;
      } else {
        String[] ipAddressInArray = ipString.split("\\.");

        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {
          int power = 3 - i;
          int ip = Integer.parseInt(ipAddressInArray[i]);
          result += ip * Math.pow(256, power);
        }

        out.value = result;
      }
    }
  }

  /**
   * Returns true if the input string is a valid IP address
   */
  @FunctionTemplate(name = "is_valid_IP", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class IsValidIPFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputIP;

    @Output
    BitHolder out;

    @Inject
    DrillBuf buffer;


    public void setup() {
    }


    public void eval() {
      String ipString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputIP.start, inputIP.end, inputIP.buffer);
      if (ipString == null || ipString.isEmpty()) {
        out.value = 0;
      } else {
        org.apache.commons.validator.routines.InetAddressValidator validator = org.apache.commons.validator.routines.InetAddressValidator.getInstance();

        boolean valid = validator.isValid(ipString);
        if (valid) {
          out.value = 1;
        } else {
          out.value = 0;
        }
      }

    }
  }

  /**
   * Returns true if the input string is a valid IPv4 address
   */
  @FunctionTemplate(name = "is_valid_IPv4", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class IsValidIPV4Function implements DrillSimpleFunc {

    @Param
    VarCharHolder inputIP;

    @Output
    BitHolder out;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }


    public void eval() {
      String ipString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputIP.start, inputIP.end, inputIP.buffer);
      if (ipString == null || ipString.isEmpty()) {
        out.value = 0;
      } else {
        org.apache.commons.validator.routines.InetAddressValidator validator = org.apache.commons.validator.routines.InetAddressValidator.getInstance();

        boolean valid = validator.isValidInet4Address(ipString);
        if (valid) {
          out.value = 1;
        } else {
          out.value = 0;
        }
      }
    }
  }

  /**
   * Returns true if the input string is a valid IP address
   */
  @FunctionTemplate(name = "is_valid_IPv6", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class IsValidIPv6Function implements DrillSimpleFunc {

    @Param
    VarCharHolder inputIP;

    @Output
    BitHolder out;

    @Inject
    DrillBuf buffer;


    public void setup() {
    }

    public void eval() {
      String ipString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputIP.start, inputIP.end, inputIP.buffer);
      if (ipString == null || ipString.isEmpty()) {
        out.value = 0;
      } else {
        org.apache.commons.validator.routines.InetAddressValidator validator = org.apache.commons.validator.routines.InetAddressValidator.getInstance();

        boolean valid = validator.isValidInet6Address(ipString);
        if (valid) {
          out.value = 1;
        } else {
          out.value = 0;
        }
      }
    }
  }
}