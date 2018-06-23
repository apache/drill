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
package org.apache.drill.exec.physical.rowSet.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;

/**
 * Represents an explicit projection at some tuple level.
 * <p>
 * A column is projected if it is explicitly listed in the selection list.
 * <p>
 * If a column is a map, then the projection for the map's columns is based on
 * two rules:
 * <ol>
 * <li>If the projection list includes at least one explicit mention of a map
 * member, then include only those columns explicitly listed.</li>
 * <li>If the projection at the parent level lists only the map column itself
 * (which the projection can't know is a map), then assume this implies all
 * columns, as if the entry where "map.*".</li>
 * </ol>
 * <p>
 * Examples:<br>
 * <code>m</code><br>
 * If m turns out to be a map, project all members of m.<br>
 * <code>m.a</code><br>
 * Column m must be a map. Project only column a.<br>
 * <code>m, m.a</code><br>
 * Tricky case. We interpret this as projecting only the "a" element of map m.
 * <p>
 * The projection set is build from a list of columns, represented as
 * {@link SchemaPath} objects, provided by the physical plan. The structure of
 * <tt>SchemaPath</tt> is a bit awkward:
 * <p>
 * <ul>
 * <li><tt>SchemaPath> is a wrapper for a column which directly holds the
 * <tt>NameSegment</tt> for the top-level column.</li>
 * <li><tt>NameSegment</tt> holds a name. This can be a top name such as
 * `a`, or parts of a compound name such as `a`.`b`. Each <tt>NameSegment</tt>
 * has a "child" that points to the option following parts of the name.</li>
 * <li><PathSegment</tt> is the base class for the parts of a name.</tt>
 * <li><tt>ArraySegment</tt> is the other kind of name part and represents
 * an array index such as the "[1]" in `columns`[1].</li>
 * <ul>
 * The parser here consumes only names, this mechanism does not consider
 * array indexes. As a result, there may be multiple projected columns that
 * map to the same projection here: `columns`[1] and `columns`[2] both map to
 * the name `columns`, for example.
 */

public class ProjectionSetImpl implements ProjectionSet {

  Set<String> projection = new HashSet<>();
  Map<String, ProjectionSetImpl> mapProjections = CaseInsensitiveMap
      .newHashMap();

  @Override
  public boolean isProjected(String colName) {
    return projection.contains(colName.toLowerCase());
  }

  @Override
  public ProjectionSet mapProjection(String colName) {
    ProjectionSet mapProj = mapProjections.get(colName.toLowerCase());
    if (mapProj != null) {
      return mapProj;
    }

    // No explicit information for the map. Members inherit the
    // same projection as the map itself.

    return new NullProjectionSet(isProjected(colName));
  }

  /**
   * Parse a projection list. The list should consist of a list of column
   * names; any wildcards should have been processed by the caller. An
   * empty or null list means everything is projected (that is, an
   * empty list here is equivalent to a wildcard in the SELECT
   * statement.)
   *
   * @param projList
   * @return
   */
  public static ProjectionSet parse(Collection<SchemaPath> projList) {
    if (projList == null || projList.isEmpty()) {
      return new NullProjectionSet(true);
    }
    ProjectionSetImpl projSet = new ProjectionSetImpl();
    for (SchemaPath col : projList) {
      projSet.addSegment(col.getRootSegment());
    }
    return projSet;
  }

  private void addSegment(NameSegment rootSegment) {
    String rootKey = rootSegment.getPath().toLowerCase();
    projection.add(rootKey);
    PathSegment child = rootSegment.getChild();
    if (child == null) {
      return;
    }
    if (child.isArray()) {
      // Ignore the [x] array suffix.
      return;
    }
    ProjectionSetImpl map = mapProjections.get(rootKey);
    if (map == null) {
      map = new ProjectionSetImpl();
      mapProjections.put(rootKey, map);
    }
    map.addSegment((NameSegment) child);
  }
}
