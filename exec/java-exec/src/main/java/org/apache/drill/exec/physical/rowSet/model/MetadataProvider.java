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
package org.apache.drill.exec.physical.rowSet.model;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;

/**
 * Interface for retrieving and/or creating metadata given
 * a vector.
 */

public interface MetadataProvider {
  ColumnMetadata metadata(int index, MaterializedField field);
  MetadataProvider childProvider(ColumnMetadata colMetadata);
  TupleMetadata tuple();

  public static class VectorDescrip {
    public final MetadataProvider parent;
    public final ColumnMetadata metadata;

    public VectorDescrip(MetadataProvider provider, int index,
        MaterializedField field) {
      parent = provider;
      metadata = provider.metadata(index, field);
    }
  }

  public static class MetadataCreator implements MetadataProvider {

    private final TupleSchema tuple;

    public MetadataCreator() {
      tuple = new TupleSchema();
    }

    public MetadataCreator(TupleSchema tuple) {
      this.tuple = tuple;
    }

    @Override
    public ColumnMetadata metadata(int index, MaterializedField field) {
      return tuple.addView(field);
    }

    @Override
    public MetadataProvider childProvider(ColumnMetadata colMetadata) {
      return new MetadataCreator((TupleSchema) colMetadata.mapSchema());
    }

    @Override
    public TupleMetadata tuple() { return tuple; }
  }

  public static class MetadataRetrieval implements MetadataProvider {

    private final TupleMetadata tuple;

    public MetadataRetrieval(TupleMetadata schema) {
      tuple = schema;
    }

    @Override
    public ColumnMetadata metadata(int index, MaterializedField field) {
      return tuple.metadata(index);
    }

    @Override
    public MetadataProvider childProvider(ColumnMetadata colMetadata) {
      return new MetadataRetrieval((TupleSchema) colMetadata.mapSchema());
    }

    @Override
    public TupleMetadata tuple() { return tuple; }
  }
}
