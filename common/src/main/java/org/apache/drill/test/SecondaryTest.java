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
package org.apache.drill.test;

/**
 * Label for Drill secondary tests. A secondary test is one that is omitted from
 * the normal Drill build because:
 * <ul>
 * <li>It is slow</li>
 * <li>It tests particular functionality which need not be tested on every
 * build.</li>
 * <li>It is old, but still worth running once in a while.</li>
 * <li>It requires specialized setup and/or runs on specific platforms.</li>
 * </ul>
 *
 * To mark a test as secondary, do either:<pre><code>
 * {@literal @}Category(SecondaryTest.class)
 * class MyTest {
 *    ...
 * }
 * </pre></code>Or:<pre><code>
 * class MyTest {
 *   {@literal @}Category(SecondaryTest.class)
 *   public void slowTest() { ... }
 * }
 * </code></pre>
 * Maven is configured as follows:<pre><code>
 *    &lt;plugin>
 *      &lt;artifactId>maven-surefire-plugin&lt;/artifactId>
 *      ...
 *      &lt;configuration>
 *        ...
 *        &lt;excludedGroups>org.apache.drill.test.SecondaryTest&lt;/excludedGroups>
 *      &lt;/configuration>
 *      ...
 *    &lt;/plugin></code></pre>
 *  To run all tests (including the secondary tests) (preliminary):<pre><code>
 *  > mvn surefire:test -Dgroups=org.apache.drill.test.SecondaryTest -DexcludedGroups=</code></pre>
 *  The above says to run the secondary test and exclude nothing. The exclusion
 *  is required to override the default exclusions: skip that parameter and Maven will
 *  blindly try to run all tests annotated with the SecondaryTest category except
 *  for those annotated with the SecondaryTest category, which is not very helpful...
 *  <p>
 *  Note that <tt>java-exec</tt> (only) provides a named execution to run large tests:
 *  <p>
 *  <tt>mvn surefire:test@include-large-tests</tt>
 *  <p>
 *  However, the above does not work. Nor did it work to include the category in
 *  a profile as described earlier. At present, there is no known way to run just
 *  the secondary tests from Maven. Sorry...
 */

public interface SecondaryTest {
  // Junit category marker
}
