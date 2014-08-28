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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.drill.common.expression.SchemaPath;

public abstract class AbstractRecordReader implements RecordReader {
  private static final String COL_NULL_ERROR = "Columns cannot be null. Use star column to select all fields.";
  private static final String COL_EMPTY_ERROR = "Readers needs at least a column to read.";
  public static final SchemaPath STAR_COLUMN = SchemaPath.getSimplePath("*");

  private Collection<SchemaPath> columns = null;
  private boolean isStarQuery = false;

  protected final void setColumns(Collection<SchemaPath> projected) {
    assert Preconditions.checkNotNull(projected, COL_NULL_ERROR).size() > 0 : COL_EMPTY_ERROR;
    isStarQuery = isStarQuery(projected);
    columns = transformColumns(projected);
  }

  protected Collection<SchemaPath> getColumns() {
    return columns;
  }

  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projected) {
    return projected;
  }

  protected boolean isStarQuery() {
    return isStarQuery;
  }

  public static boolean isStarQuery(Collection<SchemaPath> projected) {
    return Iterables.tryFind(Preconditions.checkNotNull(projected, COL_NULL_ERROR), new Predicate<SchemaPath>() {
      @Override
      public boolean apply(SchemaPath path) {
        return Preconditions.checkNotNull(path).equals(STAR_COLUMN);
      }
    }).isPresent();
  }

}
