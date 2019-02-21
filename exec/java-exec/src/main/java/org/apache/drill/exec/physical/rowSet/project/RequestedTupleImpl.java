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

import java.util.Collection;
import java.util.List;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.ArraySegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.metadata.ProjectionType;
import org.apache.drill.exec.record.metadata.TupleNameSpace;

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
 * If <code>m</code> turns out to be a map, project all members of
 * <code>m</code>.<br>
 * <code>m.a</code><br>
 * Column <code>m</code> must be a map. Project only column <code>a</code>.<br>
 * <code>m, m.a</code><br>
 * Tricky case. We interpret this as projecting only the "a" element of map m.
 * <p>
 * The projection set is built from a list of columns, represented as
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

public class RequestedTupleImpl implements RequestedTuple {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RequestedTupleImpl.class);

  private final RequestedColumnImpl parent;
  private final TupleNameSpace<RequestedColumn> projection = new TupleNameSpace<>();

  public RequestedTupleImpl() {
    parent = null;
  }

  public RequestedTupleImpl(RequestedColumnImpl parent) {
    this.parent = parent;
  }

  @Override
  public RequestedColumn get(String colName) {
    return projection.get(colName.toLowerCase());
  }

  private RequestedColumnImpl getImpl(String colName) {
    return (RequestedColumnImpl) get(colName);
  }

  @Override
  public ProjectionType projectionType(String colName) {
    RequestedColumn col = get(colName);
    return col == null ? ProjectionType.UNPROJECTED : col.type();
  }

  @Override
  public RequestedTuple mapProjection(String colName) {
    RequestedColumnImpl col = getImpl(colName);
    RequestedTuple mapProj = (col == null) ? null : col.mapProjection();
    if (mapProj != null) {
      return mapProj;
    }

    // No explicit information for the map. Members inherit the
    // same projection as the map itself.

    if (col != null) {
      return col.projectAllMembers(true);
    }
    return ImpliedTupleRequest.NO_MEMBERS;
  }

  /**
   * Parse a projection list. The list should consist of a list of column names;
   * any wildcards should have been processed by the caller. An empty list means
   * nothing is projected. A null list means everything is projected (that is, a
   * null list here is equivalent to a wildcard in the SELECT statement.)
   *
   * @param projList
   *          the list of projected columns, or null if no projection is to be
   *          done
   * @return a projection set that implements the specified projection
   */

  public static RequestedTuple parse(Collection<SchemaPath> projList) {
    if (projList == null) {
      return new ImpliedTupleRequest(true);
    }
    if (projList.isEmpty()) {
      return new ImpliedTupleRequest(false);
    }
    RequestedTupleImpl projSet = new RequestedTupleImpl();
    for (SchemaPath col : projList) {
      projSet.parseSegment(col.getRootSegment());
    }
    return projSet;
  }

  @Override
  public void parseSegment(PathSegment pathSeg) {
    if (pathSeg.isLastPath()) {
      parseLeaf((NameSegment) pathSeg);
    } else if (pathSeg.getChild().isArray()) {
      parseArray((NameSegment) pathSeg);
    } else {
      parseInternal((NameSegment) pathSeg);
    }
  }

  private void parseLeaf(NameSegment nameSeg) {
    String name = nameSeg.getPath();
    RequestedColumnImpl member = getImpl(name);
    if (member == null) {
      projection.add(name, new RequestedColumnImpl(this, name));
      return;
    }
    if (member.isSimple() || member.isWildcard()) {
      throw UserException
        .validationError()
        .message("Duplicate column in project list: %s",
            member.fullName())
        .build(logger);
    }
    if (member.isArray()) {

      // Saw both a and a[x]. Occurs in project list.
      // Project all elements.

      member.projectAllElements();
      return;
    }

    // Else the column is a known map.

    assert member.isTuple();

    // Allow both a.b (existing) and a (this column)
    // Since we we know a is a map, and we've projected the
    // whole map, modify the projection of the column to
    // project the entire map.

    member.projectAllMembers(true);
  }

  private void parseInternal(NameSegment nameSeg) {
    String name = nameSeg.getPath();
    RequestedColumnImpl member = getImpl(name);
    RequestedTuple map;
    if (member == null) {
      // New member. Since this is internal, this new member
      // must be a map.

      member = new RequestedColumnImpl(this, name);
      projection.add(name, member);
      map = member.asTuple();
    } else if (member.isTuple()) {

      // Known map. Add to it.

      map = member.asTuple();
    } else {

      // Member was previously projected by itself. We now
      // know it is a map. So, project entire map. (Earlier
      // we saw `a`. Now we see `a`.`b`.)

      map = member.projectAllMembers(true);
    }
    map.parseSegment(nameSeg.getChild());
  }

  private void parseArray(NameSegment arraySeg) {
    String name = arraySeg.getPath();
    int index = ((ArraySegment) arraySeg.getChild()).getIndex();
    RequestedColumnImpl member = getImpl(name);
    if (member == null) {
      member = new RequestedColumnImpl(this, name);
      projection.add(name, member);
    } else if (member.isSimple()) {

      // Saw both a and a[x]. Occurs in project list.
      // Project all elements.

      member.projectAllElements();
      return;
    } else if (member.hasIndex(index)) {
      throw UserException
        .validationError()
        .message("Duplicate array index in project list: %s[%d]",
            member.fullName(), index)
        .build(logger);
    }
    member.addIndex(index);
  }

  @Override
  public List<RequestedColumn> projections() {
    return projection.entries();
  }

  @Override
  public void buildName(StringBuilder buf) {
    if (parent != null) {
      parent.buildName(buf);
    }
  }
}
