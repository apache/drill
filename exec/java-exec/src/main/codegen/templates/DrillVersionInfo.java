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

<@pp.dropOutputFile />

<@pp.changeOutputFile name="/org/apache/drill/common/util/DrillVersionInfo.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.common.util;

import java.text.MessageFormat;
import java.text.ParseException;

/*
 * This file is generated with Freemarker using the template src/main/codegen/templates/DrillVersionInfo.java
 */
/**
 * Give access to Drill version as captured during the build
 *
 * <strong>Caution</strong> don't rely on major, minor and patch versions only to compare two 
 * Drill versions. Instead you should use the whole string, and apply the same semver algorithm 
 * as Maven (see {@code org.apache.maven.artifact.versioning.ComparableVersion}).
 *
 */
public class DrillVersionInfo {
  private static final String VERSION = "${maven.project.version}";

  private static final int MAJOR_VERSION = ${maven.project.artifact.selectedVersion.majorVersion};
  private static final int MINOR_VERSION = ${maven.project.artifact.selectedVersion.minorVersion};
  private static final int PATCH_VERSION = ${maven.project.artifact.selectedVersion.incrementalVersion};

  /**
   * Get the Drill version from pom
   * @return the version number as x.y.z
   */
  public static String getVersion() {
    return VERSION;
  }

  /**
   *  Get the Drill major version from pom
   *  @return x if assuming the version number is x.y.z
   */
  public static int getMajorVersion() {
    return MAJOR_VERSION;
  }

  /**
   *  Get the Drill minor version from pom
   *  @return y if assuming the version number is x.y.z
   */
  public static int getMinorVersion() {
    return MINOR_VERSION;
  }

  /**
   *  Get the Drill patch version from pom
   *  @return z if assuming the version number is x.y.z(-suffix)
   */
  public static int getPatchVersion() {
    return PATCH_VERSION;
  }
}
