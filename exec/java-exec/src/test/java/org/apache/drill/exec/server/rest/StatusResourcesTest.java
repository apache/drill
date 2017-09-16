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

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.options.OptionDefinition;
import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.FixtureBuilder;
import org.apache.drill.test.RestClientFixture;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.drill.test.TestConfigLinkage.MOCK_PROPERTY;
import static org.apache.drill.test.TestConfigLinkage.createMockPropOptionDefinition;

public class StatusResourcesTest {
  @Test
  public void testRetrieveInternalOption() throws Exception {
    OptionDefinition optionDefinition = createMockPropOptionDefinition();

    FixtureBuilder builder = ClusterFixture.builder().
      configProperty(ExecConstants.HTTP_ENABLE, true).
      configProperty(OptionValidator.OPTION_DEFAULTS_ROOT + MOCK_PROPERTY, "a").
      configProperty(ExecConstants.HTTP_PORT_HUNT, true).
      putDefinition(optionDefinition);

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture();
         RestClientFixture restClientFixture = cluster.restClientFixture()) {
      Assert.assertNull(restClientFixture.getStatusOption(MOCK_PROPERTY));
      StatusResources.OptionWrapper option = restClientFixture.getStatusInternalOption(MOCK_PROPERTY);
      Assert.assertEquals("a", option.getValueAsString());

      client.alterSystem(MOCK_PROPERTY, "c");

      Assert.assertNull(restClientFixture.getStatusOption(MOCK_PROPERTY));
      option = restClientFixture.getStatusInternalOption(MOCK_PROPERTY);
      Assert.assertEquals("c", option.getValueAsString());
    }
  }

  @Test
  public void testRetrievePublicOption() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder().
      configProperty(ExecConstants.HTTP_ENABLE, true).
      configProperty(ExecConstants.HTTP_PORT_HUNT, true).
      systemOption(ExecConstants.SLICE_TARGET, 20);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture();
         RestClientFixture restClientFixture = cluster.restClientFixture()) {
      Assert.assertNull(restClientFixture.getStatusInternalOption(ExecConstants.SLICE_TARGET));
      StatusResources.OptionWrapper option = restClientFixture.getStatusOption(ExecConstants.SLICE_TARGET);
      Assert.assertEquals(20, option.getValue());

      client.alterSystem(ExecConstants.SLICE_TARGET, 30);

      Assert.assertNull(restClientFixture.getStatusInternalOption(ExecConstants.SLICE_TARGET));
      option = restClientFixture.getStatusOption(ExecConstants.SLICE_TARGET);
      Assert.assertEquals(30, option.getValue());
    }
  }
}
