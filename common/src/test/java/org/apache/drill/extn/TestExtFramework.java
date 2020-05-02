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
package org.apache.drill.extn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.drill.extn.ExtensionsRegistry.ServiceProviderRegistry;
import org.apache.drill.test.BaseTest;
import org.apache.drill.test.DirTestWatcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class TestExtFramework extends BaseTest {

  @ClassRule
  public static final DirTestWatcher dirTestWatcher = new DirTestWatcher();
  public static File extDir;

  /**
   * The Maven packager insists on prepending the coordinate name:<br>
   * {@code drill-common-x.y.z-mockx.jar}, but we want only the base
   * name. So, we have to search for the files and ignore the
   * variable prefix.
   * @throws IOException
   */
  @BeforeClass
  public static void setup() throws IOException {
    extDir = new File(dirTestWatcher.getDir(), "ext");
    File testJarsDir = new File("target/test-jars");
    assertTrue("target/test-jars/ is missing. Check build.", testJarsDir.isDirectory());
    File mock1Jar = null;
    File mock2Jar = null;
    File mock3Jar = null;
    File mock4Jar = null;
    for (File file : testJarsDir.listFiles()) {
      if (file.getName().endsWith("-mock1.jar")) {
        mock1Jar = file;
      }
      if (file.getName().endsWith("-mock2.jar")) {
        mock2Jar = file;
      }
      if (file.getName().endsWith("-mock3.jar")) {
        mock3Jar = file;
      }
      if (file.getName().endsWith("-mock4.jar")) {
        mock4Jar = file;
      }
    }
    assertNotNull("target/test-jars/drill-common-<version>-mock1.jar is missing. Check build.", mock1Jar);
    assertNotNull("target/test-jars/drill-common-<version>-mock2.jar is missing. Check build.", mock2Jar);
    assertNotNull("target/test-jars/drill-common-<version>-mock3.jar is missing. Check build.", mock3Jar);
    assertNotNull("target/test-jars/drill-common-<version>-mock4.jar is missing. Check build.", mock4Jar);

    // Mock1.jar: valid extension as a simple jar
    FileUtils.copyFile(mock1Jar, new File(extDir, "mock1.jar"));

    // Mock2.jar: valid extension within a directory
    File mock2Dir = new File(extDir, "mock2");
    mock2Dir.mkdir();
    FileUtils.copyFile(mock2Jar, new File(mock2Dir, "mock2.jar"));

    // Mock3.jar: missing the provider config file
    FileUtils.copyFile(mock3Jar, new File(extDir, "mock3.jar"));

    // Mock4.jar: missing the provider class (config file is bogus)
    FileUtils.copyFile(mock4Jar, new File(extDir, "mock4.jar"));
  }

  @Test
  public void testFramework() {
    ExtensionsRegistry reg = new ExtensionsRegistryImpl(extDir,
        Collections.singletonList(MockProvider.class));
    ServiceProviderRegistry<MockProvider> providerReg = reg.registryFor(MockProvider.class);
    assertNotNull(providerReg);
    List<MockProvider> providers = providerReg.providers();
    assertNotNull(providers);
    assertEquals(2, providers.size());

    // Lame, but the extension loading order is random
    List<String> results = new ArrayList<>();

    MockProvider provider1 = providers.get(0);
    assertNotNull(provider1);
    MockService service1 = provider1.newMock();
    assertNotNull(service1);
    results.add(service1.get());

    MockProvider provider2 = providers.get(1);
    assertNotNull(provider2);
    MockService service2 = provider2.newMock();
    assertNotNull(service2);
    results.add(service2.get());

    assertNotSame(provider1, provider2);
    assertNotSame(provider1.getClass(), provider2.getClass());

    Collections.sort(results);
    assertEquals("Hello, Drill", results.get(0));
    assertEquals("Hello, world", results.get(1));
  }
}
