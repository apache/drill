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
package org.apache.drill.exec.record.metadata;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base class for an object with properties. Defers property map creation
 * until needed, since most instances may not need properties.
 */
public class AbstractPropertied implements Propertied {

  private Map<String, String> properties;

  protected AbstractPropertied() { }

  protected AbstractPropertied(AbstractPropertied from) {
    setProperties(from.properties);
  }

  protected boolean hasProperties() {
    return properties != null && ! properties.isEmpty();
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    if (properties != null && ! properties.isEmpty()) {
      properties().putAll(properties);
    }
  }

  @Override
  public Map<String, String> properties() {
    if (properties == null) {
      properties = new LinkedHashMap<>();
    }
    return properties;
  }

  @Override
  public String property(String key) {
    return property(key, null);
  }

  @Override
  public String property(String key, String defValue) {
    if (properties == null) {
      return defValue;
    }
    String value = properties.get(key);
    return value == null ? defValue : value;
  }

  @Override
  public void setProperty(String key, String value) {
    if (value != null) {
      properties().put(key, value);
    } else if (properties != null) {
      properties.remove(key);
    }
  }

  @Override
  public boolean booleanProperty(String key) {
    return booleanProperty(key, false);
  }

  @Override
  public boolean booleanProperty(String key, boolean defaultValue) {
    String value = property(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  @Override
  public void setBooleanProperty(String key, boolean value) {
    if (value) {
      setProperty(key, Boolean.toString(value));
    } else {
      setProperty(key, null);
    }
  }
}
