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

import org.apache.drill.metastore.MetastoreColumn;
import org.apache.drill.metastore.components.materializedviews.MaterializedViewMetadataUnit;
import org.apache.drill.metastore.iceberg.IcebergMetastoreContext;
import org.apache.drill.metastore.iceberg.operate.Overwrite;
import org.apache.drill.metastore.iceberg.transform.OperationTransformer;
import org.apache.iceberg.expressions.Expression;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Metastore MaterializedViews component operations transformer that provides mechanism
 * to convert {@link MaterializedViewMetadataUnit} data to Metastore overwrite / delete operations.
 */
public class MaterializedViewsOperationTransformer extends OperationTransformer<MaterializedViewMetadataUnit> {

  public MaterializedViewsOperationTransformer(IcebergMetastoreContext<MaterializedViewMetadataUnit> context) {
    super(context);
  }

  /**
   * Groups given list of {@link MaterializedViewMetadataUnit} based on MV key
   * (storage plugin, workspace, and MV name).
   * Each group is converted into overwrite operation.
   *
   * @param units Metastore component units
   * @return list of overwrite operations
   */
  @Override
  public List<Overwrite> toOverwrite(List<MaterializedViewMetadataUnit> units) {
    Map<MaterializedViewKey, List<MaterializedViewMetadataUnit>> data = units.stream()
        .collect(Collectors.groupingBy(MaterializedViewKey::of));

    return data.entrySet().parallelStream()
        .map(entry -> {
          MaterializedViewKey mvKey = entry.getKey();

          String location = mvKey.toLocation(context.table().location());

          Map<MetastoreColumn, Object> filterConditions = mvKey.toFilterConditions();
          Expression expression = context.transformer().filter().transform(filterConditions);

          return toOverwrite(location, expression, entry.getValue());
        })
        .collect(Collectors.toList());
  }
}
