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
package org.apache.drill.exec.planner.logical;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;

/**
 * A list decorator that wraps a list of columns along with a {@link org.apache.drill.exec.planner.logical.ColumnList.Mode mode}
 * that informs {@link org.apache.drill.exec.store.AbstractRecordReader readers} to scan or skip the underlying list of columns.
 */
public class ColumnList implements List<SchemaPath> {

  public static enum Mode {
    SKIP_ALL,
    SCAN_ALL,
    SCAN_SOME
  }

  private final List<SchemaPath> backend;
  private final Mode mode;

  protected ColumnList(List<SchemaPath> backend, Mode mode) {
    this.backend = Preconditions.checkNotNull(backend);
    this.mode = Preconditions.checkNotNull(mode);
  }

  public Mode getMode() {
    return mode;
  }

  public static ColumnList none() {
    return new ColumnList(GroupScan.ALL_COLUMNS, Mode.SKIP_ALL);
  }

  public static ColumnList all() {
    return new ColumnList(GroupScan.ALL_COLUMNS, Mode.SCAN_ALL);
  }

  public static ColumnList some(List<SchemaPath> columns) {
    return new ColumnList(columns, Mode.SCAN_SOME);
  }


  @Override
  public int size() {
    return backend.size();
  }

  @Override
  public boolean isEmpty() {
    return backend.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return backend.contains(o);
  }

  @Override
  public Iterator<SchemaPath> iterator() {
    return backend.iterator();
  }

  @Override
  public Object[] toArray() {
    return backend.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return backend.toArray(a);
  }

  @Override
  public boolean add(SchemaPath path) {
    return backend.add(path);
  }

  @Override
  public boolean remove(Object o) {
    return backend.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return backend.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends SchemaPath> c) {
    return backend.addAll(c);
  }

  @Override
  public boolean addAll(int index, Collection<? extends SchemaPath> c) {
    return backend.addAll(index, c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return backend.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return backend.retainAll(c);
  }

  @Override
  public void clear() {
    backend.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ColumnList) {
      final ColumnList other = ColumnList.class.cast(o);
      return backend.equals(other) && Objects.equal(mode, other.mode);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(backend, mode);
  }

  @Override
  public SchemaPath get(int index) {
    return backend.get(index);
  }

  @Override
  public SchemaPath set(int index, SchemaPath element) {
    return backend.set(index, element);
  }

  @Override
  public void add(int index, SchemaPath element) {
    backend.add(index, element);
  }

  @Override
  public SchemaPath remove(int index) {
    return backend.remove(index);
  }

  @Override
  public int indexOf(Object o) {
    return backend.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return backend.lastIndexOf(o);
  }

  @Override
  public ListIterator<SchemaPath> listIterator() {
    return backend.listIterator();
  }

  @Override
  public ListIterator<SchemaPath> listIterator(int index) {
    return backend.listIterator(index);
  }

  @Override
  public List<SchemaPath> subList(int fromIndex, int toIndex) {
    return backend.subList(fromIndex, toIndex);
  }

  @Override
  public String toString() {
    return backend.toString();
  }
}
