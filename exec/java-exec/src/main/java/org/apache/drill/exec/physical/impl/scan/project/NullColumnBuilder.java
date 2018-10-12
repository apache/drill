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
package org.apache.drill.exec.physical.impl.scan.project;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.project.NullColumnLoader.NullColumnSpec;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;

import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;

/**
 * Manages null columns by creating a null column loader for each
 * set of non-empty null columns. This class acts as a scan-wide
 * facade around the per-schema null column loader.
 */

public class NullColumnBuilder implements VectorSource {

  /**
   * Creates null columns if needed.
   */

  protected final List<NullColumnSpec> nullCols = new ArrayList<>();
  private NullColumnLoader nullColumnLoader;
  private VectorContainer outputContainer;

  /**
   * The reader-specified null type if other than the default.
   */

  private final MajorType nullType;
  private final boolean allowRequiredNullColumns;

  public NullColumnBuilder(
      MajorType nullType, boolean allowRequiredNullColumns) {
    this.nullType = nullType;
    this.allowRequiredNullColumns = allowRequiredNullColumns;
  }

  public NullColumnBuilder newChild() {
    return new NullColumnBuilder(nullType, allowRequiredNullColumns);
  }

  public ResolvedNullColumn add(String name) {
    return add(name, null);
  }

  public ResolvedNullColumn add(String name, MajorType type) {
    final ResolvedNullColumn col = new ResolvedNullColumn(name, type, this, nullCols.size());
    nullCols.add(col);
    return col;
  }

  public void build(ResultVectorCache vectorCache) {
    close();

    // If no null columns for this schema, no need to create
    // the loader.

    if (hasColumns()) {
      nullColumnLoader = new NullColumnLoader(vectorCache, nullCols, nullType, allowRequiredNullColumns);
      outputContainer = nullColumnLoader.output();
    }
  }

  public boolean hasColumns() {
    return nullCols != null && ! nullCols.isEmpty();
  }

  public void load(int rowCount) {
    if (nullColumnLoader != null) {
      final VectorContainer output = nullColumnLoader.load(rowCount);
      assert output == outputContainer;
    }
  }

  @Override
  public ValueVector vector(int index) {
    return outputContainer.getValueVector(index).getValueVector();
  }

  @VisibleForTesting
  public VectorContainer output() { return outputContainer; }

  public void close() {
    if (nullColumnLoader != null) {
      nullColumnLoader.close();
      nullColumnLoader = null;
    }
    if (outputContainer != null) {
      outputContainer.clear();
      outputContainer = null;
    }
  }
}
