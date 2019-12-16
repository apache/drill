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
package org.apache.drill.exec.hive;

import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;

import static org.junit.Assume.assumeTrue;

/**
 * Base class for Hive cluster tests.
 */
public class HiveClusterTest extends ClusterTest {

  @BeforeClass
  public static void checkJavaVersion() {
    assumeTrue("Skipping tests since Hive supports only JDK 8.", HiveTestUtilities.supportedJavaVersion());
  }
}
