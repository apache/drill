/*******************************************************************************
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
package org.apache.drill.common.config;

import com.typesafe.config.*;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

abstract class NestedConfig implements Config {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NestedConfig.class);

  private final Config c;

  NestedConfig(Config c) {
    this.c = c;
  }

  public ConfigObject root() {
    return c.root();
  }

  public ConfigOrigin origin() {
    return c.origin();
  }

  public Config withFallback(ConfigMergeable other) {
    return c.withFallback(other);
  }

  public Config resolve() {
    return c.resolve();
  }

  public Config resolve(ConfigResolveOptions options) {
    return c.resolve(options);
  }

  public void checkValid(Config reference, String... restrictToPaths) {
    c.checkValid(reference, restrictToPaths);
  }

  public boolean hasPath(String path) {
    return c.hasPath(path);
  }

  public boolean isEmpty() {
    return c.isEmpty();
  }

  public Set<Entry<String, ConfigValue>> entrySet() {
    return c.entrySet();
  }

  public boolean getBoolean(String path) {
    return c.getBoolean(path);
  }

  public Number getNumber(String path) {
    return c.getNumber(path);
  }

  public int getInt(String path) {
    return c.getInt(path);
  }

  public long getLong(String path) {
    return c.getLong(path);
  }

  public double getDouble(String path) {
    return c.getDouble(path);
  }

  public String getString(String path) {
    return c.getString(path);
  }

  public ConfigObject getObject(String path) {
    return c.getObject(path);
  }

  public Config getConfig(String path) {
    return c.getConfig(path);
  }

  public Object getAnyRef(String path) {
    return c.getAnyRef(path);
  }

  public ConfigValue getValue(String path) {
    return c.getValue(path);
  }

  public Long getBytes(String path) {
    return c.getBytes(path);
  }

  public Long getMilliseconds(String path) {
    return c.getMilliseconds(path);
  }

  public Long getNanoseconds(String path) {
    return c.getNanoseconds(path);
  }

  public ConfigList getList(String path) {
    return c.getList(path);
  }

  public List<Boolean> getBooleanList(String path) {
    return c.getBooleanList(path);
  }

  public List<Number> getNumberList(String path) {
    return c.getNumberList(path);
  }

  public List<Integer> getIntList(String path) {
    return c.getIntList(path);
  }

  public List<Long> getLongList(String path) {
    return c.getLongList(path);
  }

  public List<Double> getDoubleList(String path) {
    return c.getDoubleList(path);
  }

  public List<String> getStringList(String path) {
    return c.getStringList(path);
  }

  public List<? extends ConfigObject> getObjectList(String path) {
    return c.getObjectList(path);
  }

  public List<? extends Config> getConfigList(String path) {
    return c.getConfigList(path);
  }

  public List<? extends Object> getAnyRefList(String path) {
    return c.getAnyRefList(path);
  }

  public List<Long> getBytesList(String path) {
    return c.getBytesList(path);
  }

  public List<Long> getMillisecondsList(String path) {
    return c.getMillisecondsList(path);
  }

  public List<Long> getNanosecondsList(String path) {
    return c.getNanosecondsList(path);
  }

  public Config withOnlyPath(String path) {
    return c.withOnlyPath(path);
  }

  public Config withoutPath(String path) {
    return c.withoutPath(path);
  }

  public Config atPath(String path) {
    return c.atPath(path);
  }

  public Config atKey(String key) {
    return c.atKey(key);
  }

  public Config withValue(String path, ConfigValue value) {
    return c.withValue(path, value);
  }

}
