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
 ******************************************************************************/
package org.apache.drill.test;

import java.util.Collection;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.drill.common.config.DrillConfig;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

/**
 * Builds a {@link DrillConfig} for use in tests. Use this when a config
 * is needed by itself, separate from an embedded Drillbit.
 */
public class ConfigBuilder {

  protected String configResource;
  protected Properties configProps;

  /**
   * Use the given configuration properties as overrides.
   * @param configProps a collection of config properties
   * @return this builder
   * @see {@link #configProperty(String, Object)}
   */

  public ConfigBuilder configProps(Properties configProps) {
    if (hasResource()) {
      // Drill provides no constructor for this use case.
      throw new IllegalArgumentException( "Cannot provide both a config resource and config properties.");
    }
    if (this.configProps == null) {
      this.configProps = configProps;
    } else {
      this.configProps.putAll(configProps);
    }
    return this;
  }

  /**
   * Use the given configuration file, stored as a resource, to initialize
   * the Drill config. Note that the resource file should have the two
   * following settings to work as a config for an embedded Drillbit:
   * <pre><code>
   * drill.exec.sys.store.provider.local.write : false,
   * drill.exec.http.enabled : false
   * </code></pre>
   * It may be more convenient to add your settings to the default
   * config settings with {@link #configProperty(String, Object)}.
   * @param configResource path to the file that contains the
   * config file to be read
   * @return this builder
   * @see {@link #configProperty(String, Object)}
   */

  public ConfigBuilder resource(String configResource) {

    if (configProps != null) {
      // Drill provides no constructor for this use case.
      throw new IllegalArgumentException( "Cannot provide both a config resource and config properties.");
    }

    // TypeSafe gets unhappy about a leading slash, but other functions
    // require it. Silently discard the leading slash if given to
    // preserve the test writer's sanity.

    this.configResource = ClusterFixture.trimSlash(configResource);
    return this;
  }

  /**
   * Add an additional boot-time property for the embedded Drillbit.
   * @param key config property name
   * @param value property value
   * @return this builder
   */

  public ConfigBuilder put(String key, Object value) {
    if (hasResource()) {
      // Drill provides no constructor for this use case.
      throw new IllegalArgumentException( "Cannot provide both a config resource and config properties.");
    }
    if (configProps == null) {
      configProps = new Properties();
    }
    configProps.put(key, value.toString());
    return this;
  }

  public DrillConfig build() {

    // Create a config
    // Because of the way DrillConfig works, we can set the ZK
    // connection string only if a property set is provided.

    if (hasResource()) {
      return DrillConfig.create(configResource);
    } else if (configProps != null) {
      return constructConfig();
    } else {
      return DrillConfig.create();
    }
  }

  private DrillConfig constructConfig() {
    Properties stringProps = new Properties();
    Properties collectionProps = new Properties();

    // Filter out the collection type configs and other configs which can be converted to string.
    for(Entry<Object, Object> entry : configProps.entrySet()) {
      if(entry.getValue() instanceof Collection<?>) {
        collectionProps.put(entry.getKey(), entry.getValue());
      } else {
        stringProps.setProperty(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    // First create a DrillConfig based on string properties.
    Config drillConfig = DrillConfig.create(stringProps);

    // Then add the collection properties inside the DrillConfig. Below call to withValue returns
    // a new reference. Considering mostly properties will be of string type, doing this
    // later will be less expensive as compared to doing it for all the properties.
    for(Entry<Object, Object> entry : collectionProps.entrySet()) {
      drillConfig = drillConfig.withValue(entry.getKey().toString(),
        ConfigValueFactory.fromAnyRef(entry.getValue()));
    }

    return new DrillConfig(drillConfig, true);
  }

  public boolean hasResource() {
    return configResource != null;
  }
}
