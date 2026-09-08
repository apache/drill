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
package org.apache.drill.exec.store.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Base64;

import org.apache.drill.test.BaseTest;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for DelegationTokenInfo.
 */
public class DelegationTokenInfoTest extends BaseTest {

  private static final String TEST_TOKEN_CLASS = "org.apache.accumulo.core.clientImpl.DelegationTokenImpl";

  @Test
  public void testBasicConstruction() {
    long now = System.currentTimeMillis();
    String serializedToken = Base64.getEncoder().encodeToString("test-token-data".getBytes());

    DelegationTokenInfo tokenInfo = new DelegationTokenInfo(
        "testUser", serializedToken, TEST_TOKEN_CLASS, now);

    assertEquals("testUser", tokenInfo.getUserName());
    assertEquals(serializedToken, tokenInfo.getSerializedToken());
    assertEquals(TEST_TOKEN_CLASS, tokenInfo.getTokenClassName());
    assertEquals(now, tokenInfo.getCreationTime());
  }

  @Test
  public void testGetAgeMillis() throws InterruptedException {
    long before = System.currentTimeMillis();
    DelegationTokenInfo tokenInfo = new DelegationTokenInfo(
        "user",
        Base64.getEncoder().encodeToString("data".getBytes()),
        TEST_TOKEN_CLASS,
        before
    );

    // Sleep a bit to let time pass
    Thread.sleep(50);

    long age = tokenInfo.getAgeMillis();
    assertTrue("Age should be at least 50ms", age >= 50);
  }

  @Test
  public void testIsOlderThan() {
    long now = System.currentTimeMillis();
    DelegationTokenInfo tokenInfo = new DelegationTokenInfo(
        "user",
        Base64.getEncoder().encodeToString("data".getBytes()),
        TEST_TOKEN_CLASS,
        now - 5000  // Created 5 seconds ago
    );

    assertTrue("Token should be older than 1 second", tokenInfo.isOlderThan(1000));
    assertFalse("Token should not be older than 1 hour", tokenInfo.isOlderThan(3600000));
  }

  @Test
  public void testEquality() {
    long time = System.currentTimeMillis();
    String token = Base64.getEncoder().encodeToString("token".getBytes());

    DelegationTokenInfo info1 = new DelegationTokenInfo("user", token, TEST_TOKEN_CLASS, time);
    DelegationTokenInfo info2 = new DelegationTokenInfo("user", token, TEST_TOKEN_CLASS, time);
    DelegationTokenInfo info3 = new DelegationTokenInfo("differentUser", token, TEST_TOKEN_CLASS, time);

    assertEquals(info1, info2);
    assertEquals(info1.hashCode(), info2.hashCode());
    assertNotEquals(info1, info3);
  }

  @Test
  public void testJsonSerialization() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    long time = 1234567890123L;
    String serializedToken = Base64.getEncoder().encodeToString("test-token".getBytes());

    DelegationTokenInfo original = new DelegationTokenInfo(
        "testUser", serializedToken, TEST_TOKEN_CLASS, time);

    // Serialize to JSON
    String json = mapper.writeValueAsString(original);
    assertNotNull(json);
    assertTrue(json.contains("testUser"));
    assertTrue(json.contains(serializedToken));
    assertTrue(json.contains("1234567890123"));
    assertTrue(json.contains("tokenClassName"));

    // Deserialize back
    DelegationTokenInfo deserialized = mapper.readValue(json, DelegationTokenInfo.class);

    assertEquals(original.getUserName(), deserialized.getUserName());
    assertEquals(original.getSerializedToken(), deserialized.getSerializedToken());
    assertEquals(original.getTokenClassName(), deserialized.getTokenClassName());
    assertEquals(original.getCreationTime(), deserialized.getCreationTime());
    assertEquals(original, deserialized);
  }

  @Test
  public void testJsonRoundTrip() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    // Create with current time
    DelegationTokenInfo original = new DelegationTokenInfo(
        "drillUser",
        Base64.getEncoder().encodeToString("serialized-delegation-token".getBytes()),
        TEST_TOKEN_CLASS,
        System.currentTimeMillis()
    );

    // Round-trip through JSON
    String json = mapper.writeValueAsString(original);
    DelegationTokenInfo roundTripped = mapper.readValue(json, DelegationTokenInfo.class);

    assertEquals(original, roundTripped);
  }

  @Test
  public void testToString() {
    String serializedToken = Base64.getEncoder().encodeToString("token-data".getBytes());
    DelegationTokenInfo tokenInfo = new DelegationTokenInfo(
        "user", serializedToken, TEST_TOKEN_CLASS, 1234567890L);

    String toString = tokenInfo.toString();

    assertTrue(toString.contains("user"));
    assertTrue(toString.contains("1234567890"));
    assertTrue(toString.contains("tokenClassName"));
    // Should contain token length, not the actual token
    assertTrue(toString.contains("tokenLength"));
    // Should not contain the actual serialized token for security
    assertFalse(toString.contains(serializedToken));
  }

  @Test
  public void testTokenLengthInToString() {
    String shortToken = Base64.getEncoder().encodeToString("short".getBytes());
    String longToken = Base64.getEncoder().encodeToString("this-is-a-much-longer-token".getBytes());

    DelegationTokenInfo shortInfo = new DelegationTokenInfo("user", shortToken, TEST_TOKEN_CLASS, 0);
    DelegationTokenInfo longInfo = new DelegationTokenInfo("user", longToken, TEST_TOKEN_CLASS, 0);

    // toString should show different token lengths
    assertTrue(shortInfo.toString().contains(String.valueOf(shortToken.length())));
    assertTrue(longInfo.toString().contains(String.valueOf(longToken.length())));
  }

  @Test
  public void testDifferentTokenClassNames() {
    String token = Base64.getEncoder().encodeToString("token".getBytes());
    long time = System.currentTimeMillis();

    DelegationTokenInfo info1 = new DelegationTokenInfo("user", token, "ClassA", time);
    DelegationTokenInfo info2 = new DelegationTokenInfo("user", token, "ClassB", time);

    // Different token class names should result in different objects
    assertNotEquals(info1, info2);
    assertNotEquals(info1.hashCode(), info2.hashCode());
  }
}
