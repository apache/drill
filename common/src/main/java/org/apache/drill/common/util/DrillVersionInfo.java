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
package org.apache.drill.common.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Manifest;

/**
 * Get access to the Drill Version
 */
public class DrillVersionInfo {

  public static final String UNKNOWN_VERSION = "Unknown";
  private static final BigDecimal version_increment = new BigDecimal(0.1);

  /**
   * Get the Drill version from the Manifest file
   * @return the version number as x.y.z
   */
  public static String getVersion() {
    String appName = "";
    String appVersion = UNKNOWN_VERSION;
    try {
      Enumeration<URL> resources = DrillVersionInfo.class.getClassLoader()
              .getResources("META-INF/MANIFEST.MF");
      while (resources.hasMoreElements()) {
        Manifest manifest = new Manifest(resources.nextElement().openStream());
        // check that this is your manifest and do what you need or
        // get the next one
        appName = manifest.getMainAttributes()
                .getValue("Implementation-Title");
        if (appName != null && appName.toLowerCase().contains("drill")) {
          appVersion = manifest.getMainAttributes()
                  .getValue("Implementation-Version");
        }
      }
    } catch (IOException except) {
      appVersion = UNKNOWN_VERSION;
    }
    return appVersion;
  }

  /**
   * Compare two Drill versions disregarding build number and comparing only major and minor versions.
   * Versions are considered to be compatible:
   * 1. if current version is the same as version to compare.
   * 2. if current version minor version + 1 is the same as version to compare.
   */
  public static boolean isVersionsCompatible(String currentVersion, String versionToCompare) {
    if (currentVersion != null && currentVersion.equals(versionToCompare)) {
      return true;
    }

    BigDecimal currentVersionDecimal = getVersionAsDecimal(currentVersion);
    BigDecimal versionToCompareDecimal = getVersionAsDecimal(versionToCompare);

    if (currentVersionDecimal != null && versionToCompareDecimal != null) {
      BigDecimal currentVersionIncr = currentVersionDecimal.add(version_increment)
          .setScale(1, BigDecimal.ROUND_HALF_UP);
      return currentVersionDecimal.equals(versionToCompareDecimal)
          || currentVersionIncr.equals(versionToCompareDecimal);
    }

    return false;
  }

  /**
   * Get the Drill version as big decimal, if version is invalid return null
   * @return the version number from x.y.z as x.y
   */
  private static BigDecimal getVersionAsDecimal(String version) {
    BigDecimal decimalValue = null;
    if (version != null) {
      String[] arr = version.split("\\.");
      if (arr.length >= 2) {
        try {
          decimalValue = new BigDecimal(arr[0] + "." + arr[1]);
        } catch (NumberFormatException e) {
          // do nothing
        }
      }
    }
    return decimalValue;
  }

}
