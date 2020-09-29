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
package org.apache.logging.log4j.util;

import org.apache.commons.lang3.StringUtils;

/**
 * Hive uses class with the same full name from log4j-1.2-api.
 * Added this class to avoid ClassNotFound errors from Hive.
 *
 * See <a href="https://issues.apache.org/jira/browse/HIVE-23088">HIVE-23088</a> for the problem description.
 */
public class Strings {

  public static boolean isBlank(final String s) {
    return StringUtils.isBlank(s);
  }
}
