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
package org.apache.drill.exec.physical.rowSet.project;

import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ProjectionType;

/**
 * Represents one name element. Like a {@link NameSegment}, except that this
 * version is an aggregate. If the projection list contains `a.b` and `a.c`,
 * then one name segment exists for a, and contains segments for both b and c.
 */

public class RequestedColumnImpl implements RequestedColumn {

  /**
   * Special marker to indicate that that a) the item is an
   * array, and b) that all indexes are to be projected.
   * Used when seeing both a and a[x].
   */

  private static final Set<Integer> ALL_INDEXES = new HashSet<>();

  private final RequestedTuple parent;
  private final String name;
  private RequestedTuple members;
  private Set<Integer> indexes;
  private ProjectionType type;

  public RequestedColumnImpl(RequestedTuple parent, String name) {
    this.parent = parent;
    this.name = name;
    setType();
  }

  @Override
  public String name() { return name; }
  @Override
  public ProjectionType type() { return type; }
  @Override
  public boolean isWildcard() { return type == ProjectionType.WILDCARD; }
  @Override
  public boolean isSimple() { return type == ProjectionType.UNSPECIFIED; }

  @Override
  public boolean isArray() {
    return type == ProjectionType.ARRAY || type == ProjectionType.TUPLE_ARRAY;
  }

  @Override
  public boolean isTuple() {
    return type == ProjectionType.TUPLE || type == ProjectionType.TUPLE_ARRAY;
  }

  public RequestedTuple asTuple() {
    if (members == null) {
      members = new RequestedTupleImpl(this);
      setType();
    }
    return members;
  }

  public RequestedTuple projectAllMembers(boolean projectAll) {
    members = projectAll ? ImpliedTupleRequest.ALL_MEMBERS : ImpliedTupleRequest.NO_MEMBERS;
    setType();
    return members;
  }

  public void addIndex(int index) {
    if (indexes == null) {
      indexes = new HashSet<>();
    }
    if (indexes != ALL_INDEXES) {
      indexes.add(index);
    }
    setType();
  }

  public void projectAllElements() {
    indexes = ALL_INDEXES;
    setType();
  }

  @Override
  public boolean hasIndexes() {
    return indexes != null && indexes != ALL_INDEXES;
  }

  @Override
  public boolean hasIndex(int index) {
    return hasIndexes() ? indexes.contains(index) : false;
  }

  @Override
  public int maxIndex() {
    if (! hasIndexes()) {
      return 0;
    }
    int max = 0;
    for (final Integer index : indexes) {
      max = Math.max(max, index);
    }
    return max;
  }

  @Override
  public boolean[] indexes() {
    if (! hasIndexes()) {
      return null;
    }
    final int max = maxIndex();
    final boolean map[] = new boolean[max+1];
    for (final Integer index : indexes) {
      map[index] = true;
    }
    return map;
  }

  @Override
  public String fullName() {
    final StringBuilder buf = new StringBuilder();
    buildName(buf);
    return buf.toString();
  }

  public boolean isRoot() { return parent == null; }

  private void setType() {
    if (name.equals(SchemaPath.DYNAMIC_STAR)) {
      type = ProjectionType.WILDCARD;
    } else if (indexes != null && members != null) {
      type = ProjectionType.TUPLE_ARRAY;
    }
    else if (indexes != null) {
      type = ProjectionType.ARRAY;
    } else if (members != null) {
      type = ProjectionType.TUPLE;
    } else {
      type = ProjectionType.UNSPECIFIED;
    }
  }

  protected void buildName(StringBuilder buf) {
    parent.buildName(buf);
    buf.append('`')
       .append(name)
       .append('`');
  }

  @Override
  public String summary() {
    switch (type) {
    case ARRAY:
      return "array column";
    case TUPLE:
      return "map column";
    case TUPLE_ARRAY:
      return "repeated map";
    case WILDCARD:
      return "wildcard";
    default:
      return "column";
    }
  }

  @Override
  public boolean nameEquals(String target) {
    return name.equalsIgnoreCase(target);
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf
      .append("[")
      .append(getClass().getSimpleName())
      .append(" name=")
      .append(name())
      .append(", type=")
      .append(summary());
    if (isArray()) {
      buf
        .append(", array=")
        .append(indexes);
    }
    if (isTuple()) {
      buf
        .append(", tuple=")
        .append(members);
    }
    buf.append("]");
    return buf.toString();
  }

  @Override
  public RequestedTuple mapProjection() { return members; }
}