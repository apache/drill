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

package org.apache.drill.exec.fn.impl;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestNetworkFunctions extends BaseTestQuery {

  @Test
  public void testInetAton() throws Exception {
    final String query = "select inet_aton( '192.168.0.1') as inet from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("inet").baselineValues( Long.parseLong("3232235521") ).go();
  }

  @Test
  public void testInetNtoa() throws Exception {
    final String query = "select inet_ntoa( 3232235521 ) as inet from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("inet").baselineValues("192.168.0.1").go();
  }

  @Test
  public void testInNetwork() throws Exception {
    final String query = "select in_network( '192.168.0.1', '192.168.0.0/28' ) as in_net FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("in_net").baselineValues(true).go();
  }

  @Test
  public void testBroadcastAddress() throws Exception {
    final String query = "select getBroadcastAddress( '192.168.0.0/28' ) AS broadcastAddress FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("broadcastAddress").baselineValues("192.168.0.15").go();
  }
  @Test
  public void testNetmask() throws Exception {
    final String query = "select getNetmask( '192.168.0.0/28' ) AS netmask FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("netmask").baselineValues("255.255.255.240").go();
  }
  @Test
  public void testFunctions() throws Exception {
    final String query = "SELECT getLowAddress( '192.168.0.0/28' ) AS low, " +
      "getHighAddress( '192.168.0.0/28' ) AS high, " +
      "urlencode( 'http://www.test.com/login.php?username=Charles&password=12345' ) AS encoded_url, " +
      "urldecode( 'http%3A%2F%2Fwww.test.com%2Flogin.php%3Fusername%3DCharles%26password%3D12345' ) AS decoded_url, " +
      "is_private_ip( '8.8.8.8' ) AS is_private_ip, " +
      "is_valid_IP('258.257.234.23' ) AS isValidIP, " +
      "is_valid_IPv4( '192.168.0.1' ) AS isValidIP4, " +
      "is_valid_IPv6('1050:0:0:0:5:600:300c:326b') as isValidIPv6 " +
      "FROM (values(1))";

    testBuilder().
      sqlQuery(query).
      ordered().
      baselineColumns(
        "low",
        "high",
        "encoded_url",
        "decoded_url",
        "is_private_ip",
        "isValidIP",
        "IsValidIP4",
        "IsValidIPv6"
      ).
      baselineValues(
        "192.168.0.1",
        "192.168.0.14",
        "http%3A%2F%2Fwww.test.com%2Flogin.php%3Fusername%3DCharles%26password%3D12345",
        "http://www.test.com/login.php?username=Charles&password=12345",
        false,
        false,
        true,
        true
      ).go();
  }



}