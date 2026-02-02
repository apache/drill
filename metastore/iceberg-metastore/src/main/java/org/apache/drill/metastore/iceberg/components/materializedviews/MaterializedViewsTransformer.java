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
package org.apache.drill.metastore.iceberg.components.materializedviews;

import org.apache.drill.metastore.components.materializedviews.MaterializedViewMetadataUnit;
import org.apache.drill.metastore.iceberg.IcebergMetastoreContext;
import org.apache.drill.metastore.iceberg.transform.InputDataTransformer;
import org.apache.drill.metastore.iceberg.transform.OperationTransformer;
import org.apache.drill.metastore.iceberg.transform.OutputDataTransformer;
import org.apache.drill.metastore.iceberg.transform.Transformer;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

/**
 * Metastore MaterializedViews component filter, data and operations transformer.
 * Provides needed transformations when reading / writing {@link MaterializedViewMetadataUnit}
 * from / into Iceberg table.
 */
public class MaterializedViewsTransformer implements Transformer<MaterializedViewMetadataUnit> {

  private final IcebergMetastoreContext<MaterializedViewMetadataUnit> context;

  public MaterializedViewsTransformer(IcebergMetastoreContext<MaterializedViewMetadataUnit> context) {
    this.context = context;
  }

  @Override
  public InputDataTransformer<MaterializedViewMetadataUnit> inputData() {
    Table table = context.table();
    return new InputDataTransformer<>(table.schema(), new Schema(table.spec().partitionType().fields()),
        MaterializedViewMetadataUnit.SCHEMA.unitGetters());
  }

  @Override
  public OutputDataTransformer<MaterializedViewMetadataUnit> outputData() {
    return new MaterializedViewsOutputDataTransformer(MaterializedViewMetadataUnit.SCHEMA.unitBuilderSetters());
  }

  @Override
  public OperationTransformer<MaterializedViewMetadataUnit> operation() {
    return new MaterializedViewsOperationTransformer(context);
  }
}
