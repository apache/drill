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
package org.apache.drill.exec.fn.impl;

import org.apache.drill.test.BaseTestQuery;
import org.junit.Test;

public class TestNetworkFunctions extends BaseTestQuery {

  @Test
  public void testInetAton() throws Exception {
    final String query = "select inet_aton('192.168.0.1') as inet from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("inet").baselineValues(Long.parseLong("3232235521")).go();
  }

  @Test
  public void testInetNtoa() throws Exception {
    final String query = "select inet_ntoa(3232235521) as inet from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("inet").baselineValues("192.168.0.1").go();
  }

  @Test
  public void testInNetwork() throws Exception {
    final String query = "select in_network('192.168.0.1', '192.168.0.0/28') as in_net FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("in_net").baselineValues(true).go();
  }

  @Test
  public void testNotInNetwork() throws Exception {
    final String query = "select in_network('10.10.10.10', '192.168.0.0/28') as in_net FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("in_net").baselineValues(false).go();
  }

  @Test
  public void testBroadcastAddress() throws Exception {
    final String query = "select broadcast_address( '192.168.0.0/28' ) AS broadcast_address FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("broadcast_address").baselineValues("192.168.0.15").go();
  }

  @Test
  public void testNetmask() throws Exception {
    final String query = "select netmask('192.168.0.0/28') AS netmask FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("netmask").baselineValues("255.255.255.240").go();
  }

  @Test
  public void testLowAddress() throws Exception {
    final String query = "SELECT low_address('192.168.0.0/28') AS low FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("low").baselineValues("192.168.0.1").go();
  }

  @Test
  public void testHighAddress() throws Exception {
    final String query = "SELECT high_address('192.168.0.0/28') AS high FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("high").baselineValues("192.168.0.14").go();
  }

  @Test
  public void testEncodeUrl() throws Exception {
    final String query = "SELECT url_encode('http://www.test.com/login.php?username=Charles&password=12345') AS encoded_url FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("encoded_url").baselineValues("http%3A%2F%2Fwww.test.com%2Flogin.php%3Fusername%3DCharles%26password%3D12345").go();
  }

  @Test
  public void testDecodeUrl() throws Exception {
    final String query = "SELECT url_decode('http%3A%2F%2Fwww.test.com%2Flogin.php%3Fusername%3DCharles%26password%3D12345') AS decoded_url FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("decoded_url").baselineValues("http://www.test.com/login.php?username=Charles&password=12345").go();
  }

  @Test
  public void testNotPrivateIP() throws Exception {
    final String query = "SELECT is_private_ip('8.8.8.8') AS is_private_ip FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("is_private_ip").baselineValues(false).go();
  }

  @Test
  public void testPrivateIP() throws Exception {
    final String query = "SELECT is_private_ip('192.168.0.1') AS is_private_ip FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("is_private_ip").baselineValues(true).go();
  }

  @Test
  public void testNotValidIP() throws Exception {
    final String query = "SELECT is_valid_IP('258.257.234.23') AS is_valid_IP FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("is_valid_IP").baselineValues(false).go();
  }

  @Test
  public void testIsValidIP() throws Exception {
    final String query = "SELECT is_valid_IP('10.10.10.10') AS is_valid_IP FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("is_valid_IP").baselineValues(true).go();
  }

  @Test
  public void testNotValidIPv4() throws Exception {
    final String query = "SELECT is_valid_IPv4( '192.168.0.257') AS is_valid_IP4 FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("is_valid_IP4").baselineValues(false).go();
  }

  @Test
  public void testIsValidIPv4() throws Exception {
    final String query = "SELECT is_valid_IPv4( '192.168.0.1') AS is_valid_IP4 FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("is_valid_IP4").baselineValues(true).go();
  }

  @Test
  public void testIsValidIPv6() throws Exception {
    final String query = "SELECT is_valid_IPv6('1050:0:0:0:5:600:300c:326b') AS is_valid_IP6 FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("is_valid_IP6").baselineValues(true).go();
  }

  @Test
  public void testNotValidIPv6() throws Exception {
    final String query = "SELECT is_valid_IPv6('1050:0:0:0:5:600:300c:326g') AS is_valid_IP6 FROM (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("is_valid_IP6").baselineValues(false).go();
  }

}