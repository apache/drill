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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestSpaServletFilter extends BaseTest {

  /** The bug this guards: a bare "/profiles/{queryid}" must reach the SPA, not Jersey. */
  @Test
  public void bareProfilePageGoesToSpa() {
    assertFalse(SpaServletFilter.isJerseyPath("/profiles/15974fc0-9d0d-4430-21af-7ace8cfd76bf"));
  }

  @Test
  public void profileJsonEndpointsGoToJersey() {
    assertTrue(SpaServletFilter.isJerseyPath("/profiles/15974fc0-9d0d-4430-21af-7ace8cfd76bf.json"));
    assertTrue(SpaServletFilter.isJerseyPath("/profiles.json"));
    assertTrue(SpaServletFilter.isJerseyPath("/profiles/json"));
    assertTrue(SpaServletFilter.isJerseyPath("/profiles/running.json"));
    assertTrue(SpaServletFilter.isJerseyPath("/profiles/completed.json"));
  }

  @Test
  public void profileCancelGoesToJersey() {
    assertTrue(SpaServletFilter.isJerseyPath("/profiles/cancel/15974fc0-9d0d-4430-21af-7ace8cfd76bf"));
  }

  @Test
  public void apiAndStaticPrefixesGoToJersey() {
    assertTrue(SpaServletFilter.isJerseyPath("/api/v1/metadata/schemas"));
    assertTrue(SpaServletFilter.isJerseyPath("/storage/dfs.json"));
  }

  @Test
  public void spaRoutesDoNotGoToJersey() {
    assertFalse(SpaServletFilter.isJerseyPath("/profiles"));
    assertFalse(SpaServletFilter.isJerseyPath("/projects/abc/query"));
    assertFalse(SpaServletFilter.isJerseyPath("/"));
  }
}
