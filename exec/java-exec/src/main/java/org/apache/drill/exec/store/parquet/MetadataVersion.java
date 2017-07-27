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

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


class MetadataVersion implements Comparable<MetadataVersion> {
  /**
   * String version starts from 'v' letter<p>
   * First group is major metadata version (any number of digits, except a single zero digit)<p>
   * Next character is optional '.' (if minor version is specified)<p>
   * Next group is optional, minor metadata version (any number of digits, except a single zero digit)<p>
   * Examples of correct metadata versions: v1, v10, v4.13
   */
  private static final String FORMAT = "v((?!0)\\d+)\\.?((?!0)\\d+)?";
  private static final Pattern PATTERN = Pattern.compile(FORMAT);

  private final int major;
  private final int minor;

  public MetadataVersion(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  public MetadataVersion(String metadataVersion) {
    Matcher matcher = PATTERN.matcher(metadataVersion);
    if (!matcher.matches()) {
      DrillRuntimeException.format("Could not parse metadata version '%s' using format '%s'", metadataVersion, FORMAT);
    }
    this.major = Integer.parseInt(matcher.group(1));
    this.minor = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MetadataVersion)) {
      return false;
    }
    MetadataVersion that = (MetadataVersion) o;
    return this.major == that.major
        && this.minor == that.minor;
  }

  @Override
  public int hashCode() {
    int result = major;
    result = 31 * result + minor;
    return result;
  }

  @Override
  public String toString() {
    return minor == 0 ? String.format("v%s1", major) : String.format("v%s1.%s2", major, minor);
  }

  @Override
  public int compareTo(MetadataVersion o) {
    Preconditions.checkNotNull(o);
    return ComparisonChain.start()
        .compare(this.major, o.major)
        .compare(this.minor, o.minor)
        .result();
  }

/**
 * Supported metadata versions.
 * <p>
 * Note: keep them synchronized with {@link Metadata.ParquetTableMetadataBase} versions
 */
  public static class Constants {
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
     * All historical versions of the Drill metadata cache files. In case of introducing a new parquet metadata version
     * please follow the {@link MetadataVersion#FORMAT} and add a new version into the end of this list.
     */
    public static final List<String> SUPPORTED_VERSIONS = Lists.newArrayList(V1, V2, V3, V3_1);
  }
}
