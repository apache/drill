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
import com.google.common.collect.ImmutableSortedSet;
import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.util.SortedSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MetadataVersion implements Comparable<MetadataVersion> {

  private static final String FORMAT = "v?((?!0)\\d+)(\\.(\\d+))?";
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
    this.minor = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
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

  /**
   * @return string representation of the metadata file version, for example: "1", "10", "4.13"
   * <p>
   * String metadata version consists of the following characters:<p>
   * major metadata version (any number of digits, except a single zero digit),<p>
   * optional "." delimiter (used if minor metadata version is specified),<p>
   * minor metadata version (not specified for "0" minor version)
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(major);
    if (minor != 0) {
      builder.append(".").append(minor);
    }
    return builder.toString();
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
     * Version 3.1: Absolute paths of files and directories are replaced with relative ones. Metadata version value
     * doesn't contain `v` letter<br>
     * See DRILL-3867, DRILL-5660
     */
    public static final String V3_1 = "3.1";

    /**
     * All historical versions of the Drill metadata cache files. In case of introducing a new parquet metadata version
     * please follow the {@link MetadataVersion#FORMAT}.
     */
    public static final SortedSet<MetadataVersion> SUPPORTED_VERSIONS = ImmutableSortedSet.of(
        new MetadataVersion(V1),
        new MetadataVersion(V2),
        new MetadataVersion(V3),
        new MetadataVersion(V3_1)
    );

    /**
     * @param metadataVersion string representation of the parquet metadata version
     * @return true if metadata version is supported, false otherwise
     */
    public static boolean isVersionSupported(String metadataVersion) {
      return SUPPORTED_VERSIONS.contains(new MetadataVersion(metadataVersion));
    }
  }
}
