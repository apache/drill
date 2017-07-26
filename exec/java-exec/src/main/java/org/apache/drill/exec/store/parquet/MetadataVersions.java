/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import com.google.common.collect.Lists;
import org.apache.drill.common.Version;
import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Supported metadata versions.
 * <p>
 * Note: keep them synchronized with {@link Metadata.ParquetTableMetadataBase} versions
 */
public class MetadataVersions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metadata.class);

  /**
   * Version 1: Introduces parquet file metadata caching.<br>
   * See DRILL-2743
   */
  public static final String V1 = "v1";
  /**
   * Version 2: Metadata cache file size is reduced.<br>
   * See DRILL-4053
   */
  public static final String V2 = "v2";
  /**
   * Version 3: Difference between v3 and v2 : min/max, type_length, precision, scale, repetitionLevel, definitionLevel.<br>
   * Filter pushdown for Parquet is implemented. <br>
   * See DRILL-1950
   */
  public static final String V3 = "v3";
  /**
   * Version 3.1: Absolute paths of files and directories are replaced with relative ones.<br>
   * See DRILL-3867
   */
  public static final String V3_1 = "v3.1";

  /**
   * All historical versions of the Drill metadata cache files
   */
  public static final List<String> SUPPORTED_VERSIONS = Lists.newArrayList(V1, V2, V3, V3_1);

  /**
   * @param metadataVersion parquet metadata version
   * @return true if metadata version is supported, false otherwise
   */
  public static boolean isVersionSupported(String metadataVersion) {
    return SUPPORTED_VERSIONS.contains(metadataVersion);
  }

  /**
   * Helper compare method similar to {@link java.util.Comparator#compare}
   *
   * @param metadataVersion1 the first metadata version to be compared
   * @param metadataVersion2 the second metadata version to be compared
   * @return a negative integer, zero, or a positive integer as the
   *         first argument is less than, equal to, or greater than the
   *         second.
   */
  public static int compare(String metadataVersion1, String metadataVersion2) {
    if (isVersionSupported(metadataVersion1) && isVersionSupported(metadataVersion2)) {
      return VersionParser.parse(metadataVersion1).compareTo(VersionParser.parse(metadataVersion2));
    } else {
      // this is never reached
      throw new DrillRuntimeException(String.format("Unsupported metadata version. '%s' version can't be compared with '%s'",
          metadataVersion1, metadataVersion2));
    }
  }

  /**
   * Parses a parquet metadata version string
   */
  public static class VersionParser {
    // example: v3.1 or v2
    private static final String FORMAT = "v(\\d)\\.?(\\d)?";
    private static final Pattern PATTERN = Pattern.compile(FORMAT);

    /**
     * @param metadataVersion text metadata version
     * @return comparable metadata version object
     */
    public static Version parse(String metadataVersion) {
      Matcher matcher = PATTERN.matcher(metadataVersion);
      if (!matcher.matches()) {
        DrillRuntimeException.format("Could not parse metadata version '%s' using format '%s'", metadataVersion, FORMAT);
      }
      int majorVersion = Integer.parseInt(matcher.group(1));
      int minorVersion = 0;
      if (matcher.group(2) != null) {
        minorVersion = Integer.parseInt(matcher.group(2));
      }
      return new Version(metadataVersion, majorVersion, minorVersion, 0, 0, "");
    }
  }
}
