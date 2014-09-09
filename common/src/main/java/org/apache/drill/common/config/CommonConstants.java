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
package org.apache.drill.common.config;

public interface CommonConstants {
  public static final String CONFIG_DEFAULT = "drill-default.conf";
  public static final String CONFIG_OVERRIDE = "drill-override.conf";

  public static final String LOGICAL_OPERATOR_SCAN_PACKAGES = "drill.logical.operator.packages";
  public static final String PHYSICAL_OPERATOR_SCAN_PACKAGES = "drill.physical.operator.packages";
  public static final String STORAGE_PLUGIN_CONFIG_SCAN_PACKAGES = "drill.logical.storage.packages";
  public static final String DRILL_JAR_MARKER_FILE = "drill-module.conf";
  public static final String LOGICAL_FUNCTION_SCAN_PACKAGES = "drill.logical.function.packages";
}
