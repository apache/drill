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
package org.apache.drill.common.util;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.net.URL;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Beginnings of unit tests for PathScanner.
 *
 * (Created for DRILL-2696 (bad URL/pathname mixing).)
 */
public class PathScannerTest {

  @Test
  public void testForResourceGetsResourcePath() {
    final Set<URL> urls =
        PathScanner.forResource(
            "org/apache/drill/common/util/PathScannerTest_plain_name.dat",
            false, getClass().getClassLoader() );
    assertThat( "Wrong number of returned URLs", urls.size(), equalTo( 1 ) );
    final URL url = urls.toArray( new URL[0] )[0];
    assertThat( "Wrong returned resource URL",
                url.toString(),
                endsWith( "org/apache/drill/common/util/PathScannerTest_plain_name.dat" ) );
    assertThat( "Unexpected returned resource URL",
                url.toString(),
                endsWith(
                    "common/target/test-classes/"
                    + "org/apache/drill/common/util/PathScannerTest_plain_name.dat" ) );
  }

  /**
   * Tests that forResource(...) handles URL-special characters in getting path.
   */
  @Test
  public void testForResourceHandlesUrlSpecialCharsGettingPath() {
    final Set<URL> urls =
        PathScanner.forResource(
            "org/apache/drill/common/util/PathScannerTest_3%_special.dat",
            false, getClass().getClassLoader() );
    assertThat( "Wrong number of returned URLs", urls.size(), equalTo( 1 ) );
    final URL url = urls.toArray( new URL[0] )[0];
    assertThat( "Wrong returned resource URL",
                url.toString(),
                endsWith( "PathScannerTest_3%25_special.dat" ) );
    assertThat( "Wrong returned resource URL",
                url.toString(),
                endsWith( "org/apache/drill/common/util/PathScannerTest_3%25_special.dat" ) );
    assertThat( "Unexpected returned resource URL",
                url.toString(),
                endsWith(
                    "common/target/test-classes/"
                    + "org/apache/drill/common/util/PathScannerTest_3%25_special.dat" ) );
  }

  @Test
  public void testForResourceGetsResourceClasspathRoot() {
    final Set<URL> urls =
        PathScanner.forResource(
            "org/apache/drill/common/util/PathScannerTest_plain_name.dat",
            true, getClass().getClassLoader() );
    assertThat( "Wrong number of returned URLs", urls.size(), equalTo( 1 ) );
    final URL url = urls.toArray( new URL[0] )[0];
    assertThat( "Wrong returned resource URL", url.toString(), endsWith( "/" ) );
    assertThat( "Unexpected returned resource URL",
                url.toString(),
                endsWith( "common/target/test-classes/" ) );
  }

  @Ignore( "until DRILL-2696 is fixed" )
  /**
   * Tests that forResource(...) handles URL-special characters in getting
   * classpath root.
   */
  @Test
  public void testForResourceHandlesUrlSpecialCharsGettingClasspathRoot() {
    final Set<URL> urls =
        PathScanner.forResource(
            "org/apache/drill/common/util/PathScannerTest_3%+special.dat",
            true, getClass().getClassLoader() );
    assertThat( "Wrong number of returned URLs", urls.size(), equalTo( 1 ) );
    final URL url = urls.toArray( new URL[0] )[0];
    assertThat( "Wrong returned resource URL", url.toString(), endsWith( "/" ) );
    assertThat( "Unexpected returned resource URL",
                url.toString(),
                endsWith( "common/target/test-classes/" ) );
  }

}
