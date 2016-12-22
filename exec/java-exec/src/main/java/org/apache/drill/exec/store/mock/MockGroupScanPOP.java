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
package org.apache.drill.exec.store.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("mock-scan")
public class MockGroupScanPOP extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockGroupScanPOP.class);

  private final String url;
  protected final List<MockScanEntry> readEntries;
  private LinkedList<MockScanEntry>[] mappings;
  private boolean extended;

  @JsonCreator
  public MockGroupScanPOP(@JsonProperty("url") String url,
                          @JsonProperty("extended") Boolean extended,
                          @JsonProperty("entries") List<MockScanEntry> readEntries) {
    super((String)null);
    this.readEntries = readEntries;
    this.url = url;
    this.extended = extended == null ? false : extended;
  }

  @Override
  public ScanStats getScanStats() {
    return ScanStats.TRIVIAL_TABLE;
  }

  public String getUrl() {
    return url;
  }

  @JsonProperty("entries")
  public List<MockScanEntry> getReadEntries() {
    return readEntries;
  }

  public static class MockScanEntry {

    private final int records;
    private final MockColumn[] types;

    @JsonCreator
    public MockScanEntry(@JsonProperty("records") int records, @JsonProperty("types") MockColumn[] types) {
      this.records = records;
      this.types = types;
    }

    public int getRecords() {
      return records;
    }

    public MockColumn[] getTypes() {
      return types;
    }

    @Override
    public String toString() {
      return "MockScanEntry [records=" + records + ", columns=" + Arrays.toString(types) + "]";
    }
  }

  @JsonInclude(Include.NON_NULL)
  public static class MockColumn {
    @JsonProperty("type") public MinorType minorType;
    public String name;
    public DataMode mode;
    public Integer width;
    public Integer precision;
    public Integer scale;
    public String generator;
    public Integer repeat;

    @JsonCreator
    public MockColumn(@JsonProperty("name") String name,
                      @JsonProperty("type") MinorType minorType,
                      @JsonProperty("mode") DataMode mode,
                      @JsonProperty("width") Integer width,
                      @JsonProperty("precision") Integer precision,
                      @JsonProperty("scale") Integer scale,
                      @JsonProperty("generator") String generator,
                      @JsonProperty("repeat") Integer repeat) {
      this.name = name;
      this.minorType = minorType;
      this.mode = mode;
      this.width = width;
      this.precision = precision;
      this.scale = scale;
      this.generator = generator;
      this.repeat = repeat;
    }

    @JsonProperty("type")
    public MinorType getMinorType() { return minorType; }
    public String getName() { return name; }
    public DataMode getMode() { return mode; }
    public Integer getWidth() { return width; }
    public Integer getPrecision() { return precision; }
    public Integer getScale() { return scale; }
    public String getGenerator( ) { return generator; }
    public Integer getRepeat() { return repeat; }
    @JsonIgnore
    public int getRepeatCount() { return repeat == null ? 1 : repeat; }

    @JsonIgnore
    public MajorType getMajorType() {
      MajorType.Builder b = MajorType.newBuilder();
      b.setMode(mode);
      b.setMinorType(minorType);
      if (precision != null) {
        b.setPrecision(precision);
      }
      if (width != null) {
        b.setWidth(width);
      }
      if (scale != null) {
        b.setScale(scale);
      }
      return b.build();
    }

    @Override
    public String toString() {
      return "MockColumn [minorType=" + minorType + ", name=" + name + ", mode=" + mode + "]";
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    Preconditions.checkArgument(endpoints.size() <= getReadEntries().size());

    mappings = new LinkedList[endpoints.size()];

    int i =0;
    for (MockScanEntry e : this.getReadEntries()) {
      if (i == endpoints.size()) {
        i -= endpoints.size();
      }
      LinkedList<MockScanEntry> entries = mappings[i];
      if (entries == null) {
        entries = new LinkedList<MockScanEntry>();
        mappings[i] = entries;
      }
      entries.add(e);
      i++;
    }
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.length : String.format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.length, minorFragmentId);
    return new MockSubScanPOP(url, extended, mappings[minorFragmentId]);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return readEntries.size();
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MockGroupScanPOP(url, extended, readEntries);

  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("No columns for mock scan");
    }
    List<MockColumn> mockCols = new ArrayList<>( );
    Pattern p = Pattern.compile( "(\\w+)_([isd])(\\d*)" );
    for (SchemaPath path : columns) {
      String col = path.getLastSegment().getNameSegment().getPath();
      if (col.equals("*")) {
        return this;
      }
      Matcher m = p.matcher(col);
      if (! m.matches()) {
        throw new IllegalArgumentException( "Badly formatted mock column name: " + col );
      }
      @SuppressWarnings("unused")
      String name = m.group(1);
      String type = m.group(2);
      String length = m.group(3);
      int width = 10;
      if ( ! length.isEmpty() ) {
        width = Integer.parseInt(length);
      }
      MinorType minorType;
      switch(type) {
      case "i":
        minorType = MinorType.INT;
        break;
      case "s":
        minorType = MinorType.VARCHAR;
        break;
      case "d":
        minorType = MinorType.FLOAT8;
        break;
      default:
        throw new IllegalArgumentException( "Unsupported field type " + type + " for mock column " + col );
      }
      MockColumn mockCol = new MockColumn(col, minorType, DataMode.REQUIRED, width, 0, 0, null, 1);
      mockCols.add(mockCol);
    }
    MockScanEntry entry = readEntries.get(0);
    MockColumn types[] = new MockColumn[mockCols.size()];
    mockCols.toArray(types);
    MockScanEntry newEntry = new MockScanEntry( entry.records, types );
    List<MockScanEntry> newEntries = new ArrayList<>( );
    newEntries.add(newEntry);
    return new MockGroupScanPOP( url, true, newEntries );
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "MockGroupScanPOP [url=" + url
        + ", readEntries=" + readEntries + "]";
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }
}
