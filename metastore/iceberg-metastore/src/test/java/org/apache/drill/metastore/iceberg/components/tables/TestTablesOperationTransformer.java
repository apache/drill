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
package org.apache.drill.metastore.iceberg.components.tables;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.metastore.components.tables.TableMetadataUnit;
import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.iceberg.IcebergBaseTest;
import org.apache.drill.metastore.iceberg.IcebergMetastore;
import org.apache.drill.metastore.iceberg.operate.Delete;
import org.apache.drill.metastore.iceberg.operate.Overwrite;
import org.apache.drill.metastore.iceberg.transform.FilterTransformer;
import org.apache.drill.metastore.iceberg.transform.OperationTransformer;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTablesOperationTransformer extends IcebergBaseTest {

  private static String location;
  private static OperationTransformer<TableMetadataUnit> transformer;

  @BeforeClass
  public static void init() throws Exception {
    location = new Path(defaultFolder.newFolder(TestTablesOperationTransformer.class.getSimpleName()).toURI().getPath()).toUri().getPath();
    IcebergMetastore metastore = new IcebergMetastore(new DrillConfig(baseIcebergConfig(new File(location))));
    transformer = new TablesOperationTransformer((IcebergTables) metastore.tables());
  }

  @Test
  public void testToOverwriteOperation() {
    TableMetadataUnit unit = TableMetadataUnit.builder()
      .storagePlugin("dfs").workspace("tmp").tableName("nation").metadataKey("dir0").build();

    TableKey tableKey = new TableKey(unit.storagePlugin(), unit.workspace(), unit.tableName());

    Map<String, Object> filterConditions = new HashMap<>(tableKey.toFilterConditions());
    filterConditions.put(IcebergTables.METADATA_KEY, unit.metadataKey());

    String location = tableKey.toLocation(TestTablesOperationTransformer.location);
    Expression expression = new FilterTransformer().transform(filterConditions);
    Overwrite operation = transformer.toOverwrite(location, expression, Collections.singletonList(unit));

    assertEquals(expression.toString(), operation.filter().toString());

    Path path = new Path(String.valueOf(operation.dataFile().path()));
    File file = new File(path.toUri().getPath());
    assertTrue(file.exists());
    assertEquals(location, path.getParent().toUri().getPath());
  }

  @Test
  public void testToOverwriteOperations() {
    List<TableMetadataUnit> units = Arrays.asList(
      TableMetadataUnit.builder().storagePlugin("dfs").workspace("tmp").tableName("nation").metadataKey("dir0").build(),
      TableMetadataUnit.builder().storagePlugin("dfs").workspace("tmp").tableName("nation").metadataKey("dir0").build(),
      TableMetadataUnit.builder().storagePlugin("dfs").workspace("tmp").tableName("nation").metadataKey("dir2").build(),
      TableMetadataUnit.builder().storagePlugin("dfs").workspace("tmp").tableName("nation").metadataKey("dir2").build(),
      TableMetadataUnit.builder().storagePlugin("dfs").workspace("tmp").tableName("region").metadataKey("dir0").build(),
      TableMetadataUnit.builder().storagePlugin("s3").workspace("tmp").tableName("region").metadataKey("dir0").build());

    List<Overwrite> operations = transformer.toOverwrite(units);
    assertEquals(4, operations.size());
  }

  @Test
  public void testToDeleteOperation() {
    FilterExpression filter = FilterExpression.and(
      FilterExpression.equal("storagePlugin", "dfs"),
      FilterExpression.equal("workspace", "tmp"));

    Expression expected = Expressions.and(
      Expressions.equal(IcebergTables.STORAGE_PLUGIN, "dfs"),
      Expressions.equal(IcebergTables.WORKSPACE, "tmp"));

    Delete operation = transformer.toDelete(filter);

    assertEquals(expected.toString(), operation.filter().toString());
  }

  @Test
  public void testToDeleteOperations() {
    FilterExpression dfs = FilterExpression.equal("storagePlugin", "dfs");
    FilterExpression s3 = FilterExpression.equal("storagePlugin", "s3");

    List<Delete> operations = transformer.toDelete(Arrays.asList(dfs, s3));

    assertEquals(2, operations.size());
  }
}
