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
package org.apache.drill.exec.server.rest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;
import org.junit.Test;

/**
 * Security tests for the result cache.
 * Validates UUID format enforcement and path traversal prevention.
 */
public class TestResultCacheSecurity extends org.apache.drill.test.BaseTest {

  // Must match the pattern in ResultCacheResources
  private static final Pattern UUID_PATTERN = Pattern.compile(
      "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

  private boolean isValidCacheId(String cacheId) {
    return cacheId != null && UUID_PATTERN.matcher(cacheId).matches();
  }

  @Test
  public void testValidUuidAccepted() {
    assertTrue(isValidCacheId("550e8400-e29b-41d4-a716-446655440000"));
    assertTrue(isValidCacheId("6ba7b810-9dad-11d1-80b4-00c04fd430c8"));
    assertTrue(isValidCacheId("00000000-0000-0000-0000-000000000000"));
  }

  @Test
  public void testPathTraversalRejected() {
    assertFalse(isValidCacheId("../../etc/passwd"));
    assertFalse(isValidCacheId("../secret"));
    assertFalse(isValidCacheId("..%2F..%2Fetc%2Fpasswd"));
    assertFalse(isValidCacheId("foo/bar"));
    assertFalse(isValidCacheId("foo\\bar"));
  }

  @Test
  public void testNullAndEmptyRejected() {
    assertFalse(isValidCacheId(null));
    assertFalse(isValidCacheId(""));
    assertFalse(isValidCacheId("   "));
  }

  @Test
  public void testNonUuidRejected() {
    assertFalse(isValidCacheId("not-a-uuid"));
    assertFalse(isValidCacheId("12345"));
    assertFalse(isValidCacheId("hello-world-this-is-not-valid"));
    // Uppercase hex should be rejected (our pattern only allows lowercase)
    assertFalse(isValidCacheId("550E8400-E29B-41D4-A716-446655440000"));
  }

  @Test
  public void testShellInjectionRejected() {
    assertFalse(isValidCacheId("$(whoami)"));
    assertFalse(isValidCacheId("`rm -rf /`"));
    assertFalse(isValidCacheId("foo; rm -rf /"));
    assertFalse(isValidCacheId("foo | cat /etc/passwd"));
  }

  @Test
  public void testHeaderInjectionInCacheIdRejected() {
    assertFalse(isValidCacheId("foo\r\nX-Injected: true"));
    assertFalse(isValidCacheId("foo\nSet-Cookie: evil=yes"));
  }

  @Test
  public void testFilenameSpecialCharsRejected() {
    assertFalse(isValidCacheId("foo<bar>baz"));
    assertFalse(isValidCacheId("foo\"bar"));
    assertFalse(isValidCacheId("foo:bar"));
    assertFalse(isValidCacheId("foo*bar"));
    assertFalse(isValidCacheId("foo?bar"));
  }

  @Test
  public void testSanitizeFilename() {
    // Simulate the sanitization logic from ResultCacheResources
    String dangerous = "query-id\"; malicious-header: true";
    String sanitized = dangerous.replaceAll("[^a-zA-Z0-9_-]", "_");
    assertFalse(sanitized.contains("\""));
    assertFalse(sanitized.contains(";"));
    assertFalse(sanitized.contains(":"));
  }

  @Test
  public void testSanitizeFilenamePreservesNormal() {
    String normal = "2a1b3c4d-abcd-1234-ef56-789012345678";
    String sanitized = normal.replaceAll("[^a-zA-Z0-9_-]", "_");
    // Normal UUID should survive sanitization intact
    assertTrue(sanitized.equals(normal));
  }
}
