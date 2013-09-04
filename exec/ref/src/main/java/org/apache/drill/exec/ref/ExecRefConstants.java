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
package org.apache.drill.exec.ref;

public class ExecRefConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExecRefConstants.class);
  
  public static final String CONFIG_DEFAULT = "drill-exec-ref-default.json";
  public static final String CONFIG_OVERRIDE = "drill-exec-ref-site.json";
  public static final String STORAGE_ENGINE_SCAN_PACKAGES = "drill.storage.packages";

}
