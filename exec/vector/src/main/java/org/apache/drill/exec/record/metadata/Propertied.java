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

import java.util.Map;

/**
 * Interface for an object that defines properties. Used in conjunction with
 * {@link PropertyAccessor}.
 */
public interface Propertied {

  /**
   * Base name for properties which Drill itself defines. Provides a
   * separate "name space" from user-defined properties which should
   * have some other perfix.
   */
  public static final String DRILL_PROP_PREFIX = "drill.";

  /**
   * Sets schema properties if not null.
   *
   * @param properties schema properties
   */
  void setProperties(Map<String, String> properties);

  Map<String, String> properties();

  String property(String key);
  String property(String key, String defValue);
  void setProperty(String key, String value);
  boolean getBooleanProperty(String key);
}
