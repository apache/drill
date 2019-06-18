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

import com.typesafe.config.ConfigValueFactory;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.metastore.components.tables.Tables;
import org.apache.drill.metastore.operate.Metadata;
import org.apache.drill.metastore.Metastore;
import org.apache.drill.metastore.components.tables.TableMetadataUnit;
import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.iceberg.IcebergBaseTest;
import org.apache.drill.metastore.iceberg.IcebergMetastore;
import org.apache.drill.metastore.iceberg.config.IcebergConfigConstants;
import org.apache.drill.metastore.metadata.TableInfo;
import org.apache.iceberg.TableProperties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class TestIcebergTablesMetastore extends IcebergBaseTest {

  private static final String COMPONENTS_COMMON_PROPERTIES_PATTERN = IcebergConfigConstants.COMPONENTS_COMMON_PROPERTIES + ".%s";
  private static final String COMPONENTS_TABLES_PROPERTIES_PATTERN = IcebergConfigConstants.COMPONENTS_TABLES_PROPERTIES + ".%s";

  @Rule
  public TemporaryFolder baseLocation = new TemporaryFolder();

  @Test
  public void testCreationWithoutProperties() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));

    Metastore metastore = new IcebergMetastore(config);
    assertTrue(metastore.tables().metadata().properties().isEmpty());
  }

  @Test
  public void testCreationWithCommonProperties() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot())
      .withValue(String.format(COMPONENTS_COMMON_PROPERTIES_PATTERN, TableProperties.SPLIT_SIZE),
        ConfigValueFactory.fromAnyRef(10))
      .withValue(String.format(COMPONENTS_COMMON_PROPERTIES_PATTERN, TableProperties.MANIFEST_MIN_MERGE_COUNT),
        ConfigValueFactory.fromAnyRef(2)));

    Metastore metastore = new IcebergMetastore(config);
    Map<String, String> expected = new HashMap<>();
    expected.put(TableProperties.SPLIT_SIZE, "10");
    expected.put(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2");
    assertEquals(expected, metastore.tables().metadata().properties());
  }

  @Test
  public void testCreationWithCommonAndComponentProperties() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot())
      .withValue(String.format(COMPONENTS_COMMON_PROPERTIES_PATTERN, TableProperties.SPLIT_SIZE),
        ConfigValueFactory.fromAnyRef(10))
      .withValue(String.format(COMPONENTS_TABLES_PROPERTIES_PATTERN, TableProperties.MANIFEST_MIN_MERGE_COUNT),
        ConfigValueFactory.fromAnyRef(2)));

    Metastore metastore = new IcebergMetastore(config);
    Map<String, String> expected = new HashMap<>();
    expected.put(TableProperties.SPLIT_SIZE, "10");
    expected.put(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2");
    assertEquals(expected, metastore.tables().metadata().properties());
  }

  @Test
  public void testCreationWithComponentPropertiesPrecedence() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot())
      .withValue(String.format(COMPONENTS_COMMON_PROPERTIES_PATTERN, TableProperties.SPLIT_SIZE),
        ConfigValueFactory.fromAnyRef(10))
      .withValue(String.format(COMPONENTS_TABLES_PROPERTIES_PATTERN, TableProperties.SPLIT_SIZE),
        ConfigValueFactory.fromAnyRef(100)));

    Metastore metastore = new IcebergMetastore(config);
    assertEquals(Collections.singletonMap(TableProperties.SPLIT_SIZE, "100"),
      metastore.tables().metadata().properties());
  }

  @Test
  public void testLoadWithoutProperties() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));

    Metastore initialMetastore = new IcebergMetastore(config);
    assertTrue(initialMetastore.tables().metadata().properties().isEmpty());

    Metastore newMetastore = new IcebergMetastore(config);
    assertTrue(newMetastore.tables().metadata().properties().isEmpty());
  }

  @Test
  public void testLoadWithSameProperties() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot())
      .withValue(String.format(COMPONENTS_COMMON_PROPERTIES_PATTERN, TableProperties.SPLIT_SIZE),
        ConfigValueFactory.fromAnyRef(10)));

    Map<String, String> initialProperties = Collections.singletonMap(TableProperties.SPLIT_SIZE, "10");

    Metastore initialMetastore = new IcebergMetastore(config);
    assertEquals(initialProperties, initialMetastore.tables().metadata().properties());

    Metastore newMetastore = new IcebergMetastore(config);
    assertEquals(initialProperties, newMetastore.tables().metadata().properties());
  }

  @Test
  public void testLoadWithUpdatedProperties() {
    DrillConfig initialConfig = new DrillConfig(baseIcebergConfig(baseLocation.getRoot())
      .withValue(String.format(COMPONENTS_COMMON_PROPERTIES_PATTERN, TableProperties.SPLIT_SIZE),
        ConfigValueFactory.fromAnyRef(10))
      .withValue(String.format(COMPONENTS_TABLES_PROPERTIES_PATTERN, TableProperties.MANIFEST_MIN_MERGE_COUNT),
        ConfigValueFactory.fromAnyRef(2)));

    Map<String, String> initialProperties = new HashMap<>();
    initialProperties.put(TableProperties.SPLIT_SIZE, "10");
    initialProperties.put(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2");

    Metastore initialMetastore = new IcebergMetastore(initialConfig);
    assertEquals(initialProperties, initialMetastore.tables().metadata().properties());

    DrillConfig newConfig = new DrillConfig(baseIcebergConfig(baseLocation.getRoot())
      .withValue(String.format(COMPONENTS_COMMON_PROPERTIES_PATTERN, TableProperties.SPLIT_SIZE),
        ConfigValueFactory.fromAnyRef(100))
      .withValue(String.format(COMPONENTS_TABLES_PROPERTIES_PATTERN, TableProperties.COMMIT_NUM_RETRIES),
        ConfigValueFactory.fromAnyRef(5)));

    Map<String, String> newProperties = new HashMap<>();
    newProperties.put(TableProperties.SPLIT_SIZE, "100");
    newProperties.put(TableProperties.COMMIT_NUM_RETRIES, "5");

    Metastore newMetastore = new IcebergMetastore(newConfig);
    assertEquals(newProperties, newMetastore.tables().metadata().properties());
  }

  @Test
  public void testNewInstance() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));
    Metastore metastore = new IcebergMetastore(config);

    assertNotSame(metastore.tables(), metastore.tables());
  }

  @Test
  public void testVersionInitial() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));
    Metastore metastore = new IcebergMetastore(config);
    Metadata metadata = metastore.tables().metadata();
    assertTrue(metadata.supportsVersioning());
    assertEquals(0, metadata.version());
  }

  @Test
  public void testVersionUpdate() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));

    Tables tables = new IcebergMetastore(config).tables();
    Metadata metadata = tables.metadata();

    assertTrue(metadata.supportsVersioning());
    assertEquals(0, metadata.version());

    tables.modify()
      .overwrite(TableMetadataUnit.builder()
        .storagePlugin("dfs")
        .workspace("tmp")
        .tableName("nation")
        .metadataKey("dir0")
        .build())
      .execute();

    assertNotEquals(0, metadata.version());
  }

  @Test
  public void testWriteReadAllFieldTypes() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));
    Tables tables = new IcebergMetastore(config).tables();

    Map<String, String> columnStatistics = new HashMap<>();
    columnStatistics.put("stat1", "val1");
    columnStatistics.put("stat2", "val2");

    TableInfo tableInfo = TableInfo.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .name("nation")
      .build();

    TableMetadataUnit unit = TableMetadataUnit.builder()
      .storagePlugin(tableInfo.storagePlugin())
      .workspace(tableInfo.workspace())
      .tableName(tableInfo.name())
      .metadataKey("dir0")
      .rowGroupIndex(1)
      .lastModifiedTime(System.currentTimeMillis())
      .partitionValues(Collections.emptyList())
      .locations(Arrays.asList("a", "b", "c"))
      .hostAffinity(Collections.emptyMap())
      .columnsStatistics(columnStatistics)
      .build();

    tables.modify()
      .overwrite(unit)
      .execute();

    List<TableMetadataUnit> units = tables.read()
      .filter(tableInfo.toFilter())
      .execute();

    assertEquals(1, units.size());
    assertEquals(unit, units.get(0));
  }

  @Test
  public void testReadSelectedColumns() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));
    Tables tables = new IcebergMetastore(config).tables();

    TableInfo tableInfo = TableInfo.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .name("nation")
      .build();

    TableMetadataUnit unit = TableMetadataUnit.builder()
      .storagePlugin(tableInfo.storagePlugin())
      .workspace(tableInfo.workspace())
      .tableName(tableInfo.name())
      .metadataKey("dir0")
      .build();

    tables.modify()
      .overwrite(unit)
      .execute();

    List<TableMetadataUnit> units = tables.read()
      .filter(tableInfo.toFilter())
      .columns("tableName", "metadataKey")
      .execute();

    assertEquals(1, units.size());
    assertEquals(TableMetadataUnit.builder().tableName("nation").metadataKey("dir0").build(), units.get(0));
  }

  @Test
  public void testReadNoResult() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));
    Tables tables = new IcebergMetastore(config).tables();

    List<TableMetadataUnit> units = tables.read()
      .filter(FilterExpression.equal("storagePlugin", "dfs"))
      .columns("tableName", "metadataKey")
      .execute();

    assertTrue(units.isEmpty());
  }

  @Test
  public void testOverwrite() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));
    Tables tables = new IcebergMetastore(config).tables();

    TableInfo tableInfo = TableInfo.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .name("nation")
      .build();

    TableMetadataUnit initialUnit = TableMetadataUnit.builder()
      .storagePlugin(tableInfo.storagePlugin())
      .workspace(tableInfo.workspace())
      .tableName(tableInfo.name())
      .metadataKey("dir0")
      .tableType("parquet")
      .build();

    tables.modify()
      .overwrite(initialUnit)
      .execute();

    List<TableMetadataUnit> units = tables.read()
      .filter(tableInfo.toFilter())
      .execute();

    assertEquals(1, units.size());
    assertEquals(initialUnit, units.get(0));

    TableMetadataUnit updatedUnit = TableMetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey("dir0")
      .tableType("text")
      .build();

    tables.modify()
      .overwrite(updatedUnit)
      .execute();

    List<TableMetadataUnit> updatedUnits = tables.read()
      .filter(tableInfo.toFilter())
      .execute();

    assertEquals(1, updatedUnits.size());
    assertEquals(updatedUnit, updatedUnits.get(0));
  }

  @Test
  public void testDelete() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));
    Tables tables = new IcebergMetastore(config).tables();

    TableInfo tableInfo = TableInfo.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .name("nation")
      .build();

    TableMetadataUnit firstUnit = TableMetadataUnit.builder()
      .storagePlugin(tableInfo.storagePlugin())
      .workspace(tableInfo.workspace())
      .tableName(tableInfo.name())
      .metadataKey("dir0")
      .build();

    TableMetadataUnit secondUnit = TableMetadataUnit.builder()
      .storagePlugin(tableInfo.storagePlugin())
      .workspace(tableInfo.workspace())
      .tableName(tableInfo.name())
      .metadataKey("dir1")
      .build();

    tables.modify()
      .overwrite(firstUnit, secondUnit)
      .execute();

    List<TableMetadataUnit> units = tables.read()
      .filter(tableInfo.toFilter())
      .execute();

    assertEquals(2, units.size());

    FilterExpression deleteFilter = FilterExpression.and(
      tableInfo.toFilter(),
      FilterExpression.equal("metadataKey", "dir0"));

    tables.modify()
      .delete(deleteFilter)
      .execute();

    List<TableMetadataUnit> updatedUnits = tables.read()
      .filter(tableInfo.toFilter())
      .execute();

    assertEquals(1, updatedUnits.size());
    assertEquals(secondUnit, updatedUnits.get(0));
  }

  @Test
  public void testOverwriteAndDeleteInOneTransaction() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));
    Tables tables = new IcebergMetastore(config).tables();

    TableInfo tableInfo = TableInfo.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .name("nation")
      .build();

    TableMetadataUnit firstUnit = TableMetadataUnit.builder()
      .storagePlugin(tableInfo.storagePlugin())
      .workspace(tableInfo.workspace())
      .tableName(tableInfo.name())
      .metadataKey("dir0")
      .tableType("parquet")
      .build();

    TableMetadataUnit secondUnit = TableMetadataUnit.builder()
      .storagePlugin(tableInfo.storagePlugin())
      .workspace(tableInfo.workspace())
      .tableName(tableInfo.name())
      .metadataKey("dir1")
      .tableType("parquet")
      .build();

    tables.modify()
      .overwrite(firstUnit, secondUnit)
      .execute();

    List<TableMetadataUnit> units = tables.read()
      .filter(tableInfo.toFilter())
      .execute();

    assertEquals(2, units.size());

    FilterExpression deleteFilter = FilterExpression.and(
      tableInfo.toFilter(),
      FilterExpression.equal("metadataKey", "dir0"));

    TableMetadataUnit updatedUnit = TableMetadataUnit.builder()
      .storagePlugin(tableInfo.storagePlugin())
      .workspace(tableInfo.workspace())
      .tableName(tableInfo.name())
      .metadataKey("dir1")
      .tableType("text")
      .build();

    tables.modify()
      .delete(deleteFilter)
      .overwrite(updatedUnit)
      .execute();

    List<TableMetadataUnit> updatedUnits = tables.read()
      .filter(tableInfo.toFilter())
      .execute();

    assertEquals(1, updatedUnits.size());
    assertEquals(updatedUnit, updatedUnits.get(0));
  }

  @Test
  public void testPurge() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(baseLocation.getRoot()));
    Tables tables = new IcebergMetastore(config).tables();

    TableMetadataUnit firstUnit = TableMetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey("dir0")
      .tableType("parquet")
      .build();

    TableMetadataUnit secondUnit = TableMetadataUnit.builder()
      .storagePlugin("s3")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey("dir0")
      .tableType("parquet")
      .build();

    tables.modify()
      .overwrite(firstUnit, secondUnit)
      .execute();

    List<TableMetadataUnit> initialUnits = tables.read()
      .execute();

    assertEquals(2, initialUnits.size());

    tables.modify()
      .purge()
      .execute();

    List<TableMetadataUnit> resultingUnits = tables.read()
      .execute();

    assertTrue(resultingUnits.isEmpty());
  }
}
