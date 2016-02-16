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
package org.apache.drill.exec.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.expression.PathSegment;
import com.google.common.collect.Maps;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.skiprecord.RecordContextVisitor;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.drill.exec.vector.VarCharVector;

public abstract class AbstractRecordReader implements RecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractRecordReader.class);

  private static final String COL_NULL_ERROR = "Columns cannot be null. Use star column to select all fields.";
  public static final SchemaPath STAR_COLUMN = SchemaPath.getSimplePath("*");

  // For text reader, the default columns to read is "columns[0]".
  protected static final List<SchemaPath> DEFAULT_TEXT_COLS_TO_READ = ImmutableList.of(new SchemaPath(new PathSegment.NameSegment("columns", new PathSegment.ArraySegment(0))));

  private Collection<SchemaPath> columns = null;
  private boolean isStarQuery = false;
  private boolean isSkipQuery = false;

  @Override
  public String toString() {
    return super.toString()
        + "[columns = " + columns
        + ", isStarQuery = " + isStarQuery
        + ", isSkipQuery = " + isSkipQuery + "]";
  }

  protected final void setColumns(Collection<SchemaPath> projected) {
    Preconditions.checkNotNull(projected, COL_NULL_ERROR);
    isSkipQuery = projected.isEmpty();
    Collection<SchemaPath> columnsToRead = projected;

    // If no column is required (SkipQuery), by default it will use DEFAULT_COLS_TO_READ .
    // Handling SkipQuery is storage-plugin specif : JSON, text reader, parquet will override, in order to
    // improve query performance.
    if (projected.isEmpty()) {
      columnsToRead = getDefaultColumnsToRead();
    }

    isStarQuery = isStarQuery(columnsToRead);
    columns = transformColumns(columnsToRead);

    logger.debug("columns to read : {}", columns);
  }

  @Override
  public Collection<SchemaPath> getVirtualColumns() {
    if(columns == null || columns.isEmpty()) {
      return ImmutableList.of();
    }

    final List<SchemaPath> schemaPaths = Lists.newArrayList();
    for(SchemaPath schemaPath : columns) {
      if(schemaPath.getRootSegment().getPath().startsWith(RecordContextVisitor.VIRTUAL_COLUMN_PREFIX)) {
        schemaPaths.add(schemaPath);
      }
    }
    return schemaPaths;
  }

  protected Collection<SchemaPath> getColumns() {
    List<SchemaPath> schemaPaths = Lists.newArrayList();
    for(SchemaPath schemaPath : columns) {
      if(!schemaPath.getRootSegment().getPath().startsWith(RecordContextVisitor.VIRTUAL_COLUMN_PREFIX)) {
        schemaPaths.add(schemaPath);
      }
    }
    return schemaPaths;
  }

  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projected) {
    return projected;
  }

  protected boolean isStarQuery() {
    return isStarQuery;
  }

  /**
   * Returns true if reader should skip all of the columns, reporting number of records only. Handling of a skip query
   * is storage plugin-specific.
   */
  protected boolean isSkipQuery() {
    return isSkipQuery;
  }

  public static boolean isStarQuery(Collection<SchemaPath> projected) {
    return Iterables.tryFind(Preconditions.checkNotNull(projected, COL_NULL_ERROR), new Predicate<SchemaPath>() {
      @Override
      public boolean apply(SchemaPath path) {
        return Preconditions.checkNotNull(path).equals(STAR_COLUMN);
      }
    }).isPresent();
  }

  @Override
  public void allocate(Map<Key, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  protected List<SchemaPath> getDefaultColumnsToRead() {
    return GroupScan.ALL_COLUMNS;
  }

  @Override
  public List<Pair<String, ? extends RecordContextVisitor.RecordReaderContextPopulator>> addReaderContextField() {
    return ImmutableList.of();
  }

  @Override
  public void populateRecordContextVectors(final List<VarCharVector> virtualColumnVector, final int recordCount) {
    for(Pair<String, ? extends RecordContextVisitor.RecordReaderContextPopulator> pair : addReaderContextField()) {
      String key = pair.getKey();

      boolean isMatch = false;
      Pattern p = Pattern.compile("\\$" + key + "[0-9]+");
      for(VarCharVector varCharVector : virtualColumnVector) {
        final SchemaPath schemaPath = varCharVector.getField().getPath();
        final String col = schemaPath.getRootSegment().getPath();
        Matcher m = p.matcher(col);
        if(m.matches()) {
          AllocationHelper.allocateNew(varCharVector, recordCount);
          for(int i = 0; i < recordCount; ++i) {
            pair.getValue().populate(this, varCharVector, i);
          }
          isMatch = true;
          break;
        }
      }
      assert isMatch;
    }
  }
}
