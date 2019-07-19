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
import org.apache.drill.metastore.components.tables.BasicTablesTransformer;
import org.apache.drill.metastore.components.tables.BasicTablesRequests;
import org.apache.drill.metastore.components.tables.TableMetadataUnit;
import org.apache.drill.metastore.components.tables.MetastoreTableInfo;
import org.apache.drill.metastore.components.tables.Tables;
import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.iceberg.IcebergBaseTest;
import org.apache.drill.metastore.iceberg.IcebergMetastore;
import org.apache.drill.metastore.metadata.BaseTableMetadata;
import org.apache.drill.metastore.metadata.FileMetadata;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.PartitionMetadata;
import org.apache.drill.metastore.metadata.RowGroupMetadata;
import org.apache.drill.metastore.metadata.SegmentMetadata;
import org.apache.drill.metastore.metadata.TableInfo;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestBasicRequests extends IcebergBaseTest {

  private static Tables tables;
  private static BasicTablesRequests basicRequests;
  private static TableMetadataUnit nationTable;
  private static TableInfo nationTableInfo;

  @BeforeClass
  public static void init() {
    DrillConfig config = new DrillConfig(baseIcebergConfig(defaultFolder.getRoot()));
    tables = new IcebergMetastore(config).tables();
    prepareData(tables);
    basicRequests = tables.basicRequests();
  }

  @Test
  public void testMetastoreTableInfoAbsentTable() {
    TableInfo tableInfo = TableInfo.builder().storagePlugin("dfs").workspace("tmp").name("absent").build();
    MetastoreTableInfo metastoreTableInfo = basicRequests.metastoreTableInfo(tableInfo);
    assertFalse(metastoreTableInfo.isExists());
    assertEquals(tableInfo, metastoreTableInfo.tableInfo());
    assertNull(metastoreTableInfo.lastModifiedTime());
  }

  @Test
  public void testMetastoreTableInfoExistingTable() {
    MetastoreTableInfo metastoreTableInfo = basicRequests.metastoreTableInfo(nationTableInfo);
    assertTrue(metastoreTableInfo.isExists());
    assertEquals(nationTableInfo, metastoreTableInfo.tableInfo());
    assertEquals(nationTable.lastModifiedTime(), metastoreTableInfo.lastModifiedTime());
    assertTrue(metastoreTableInfo.metastoreVersion() > 0);
  }

  @Test
  public void testHasMetastoreTableInfoChangedFalse() {
    MetastoreTableInfo metastoreTableInfo = basicRequests.metastoreTableInfo(nationTableInfo);
    assertFalse(basicRequests.hasMetastoreTableInfoChanged(metastoreTableInfo));
  }

  @Test
  public void testHasMetastoreTableInfoChangedTrue() {
    TableMetadataUnit unit = nationTable.toBuilder()
      .tableName("changingTable")
      .lastModifiedTime(1L)
      .build();
    tables.modify()
      .overwrite(unit)
      .execute();
    TableInfo tableInfo = TableInfo.builder().metadataUnit(unit).build();
    MetastoreTableInfo metastoreTableInfo = basicRequests.metastoreTableInfo(tableInfo);

    TableMetadataUnit updatedUnit = unit.toBuilder()
      .lastModifiedTime(2L)
      .build();
    tables.modify()
      .overwrite(updatedUnit)
      .execute();

    assertTrue(basicRequests.hasMetastoreTableInfoChanged(metastoreTableInfo));
  }

  @Test
  public void testTablesMetadataAbsent() {
    List<BaseTableMetadata> tablesMetadata = basicRequests.tablesMetadata(
      FilterExpression.and(
        FilterExpression.equal(IcebergTables.STORAGE_PLUGIN, "dfs"),
        FilterExpression.equal(IcebergTables.WORKSPACE, "absent")));
    assertTrue(tablesMetadata.isEmpty());
  }

  @Test
  public void testTablesMetadataExisting() {
    List<BaseTableMetadata> baseTableMetadata = basicRequests.tablesMetadata(
      FilterExpression.and(
        FilterExpression.equal(IcebergTables.STORAGE_PLUGIN, "dfs"),
        FilterExpression.equal(IcebergTables.WORKSPACE, "tmp")));
    assertTrue(baseTableMetadata.size() > 1);
  }

  @Test
  public void testTableMetadataAbsent() {
    TableInfo tableInfo = TableInfo.builder().storagePlugin("dfs").workspace("tmp").name("absent").build();
    BaseTableMetadata tableMetadata = basicRequests.tableMetadata(tableInfo);
    assertNull(tableMetadata);
  }

  @Test
  public void testTableMetadataExisting() {
    BaseTableMetadata tableMetadata = basicRequests.tableMetadata(nationTableInfo);
    assertNotNull(tableMetadata);
  }

  @Test
  public void testSegmentsMetadataByMetadataKeyAbsent() {
    List<SegmentMetadata> segmentMetadata = basicRequests.segmentsMetadataByMetadataKey(
      nationTableInfo,
      Collections.singletonList("/tmp/nation/part_int=3/d6"),
      "part_int=3");
    assertTrue(segmentMetadata.isEmpty());
  }

  @Test
  public void testSegmentsMetadataByMetadataKeyExisting() {
    List<SegmentMetadata> segmentMetadata = basicRequests.segmentsMetadataByMetadataKey(
      nationTableInfo,
      Arrays.asList("/tmp/nation/part_int=3/d3", "/tmp/nation/part_int=3/d4"),
      "part_int=3");
    assertEquals(2, segmentMetadata.size());
  }

  @Test
  public void testSegmentsMetadataByColumnAbsent() {
    List<SegmentMetadata> segmentMetadata = basicRequests.segmentsMetadataByColumn(
      nationTableInfo,
      Arrays.asList("/tmp/nation/part_int=3/d4", "/tmp/nation/part_int=3/d5"),
      "n_region");
    assertTrue(segmentMetadata.isEmpty());
  }

  @Test
  public void testSegmentsMetadataByColumnExisting() {
    List<SegmentMetadata> segmentMetadata = basicRequests.segmentsMetadataByColumn(
      nationTableInfo,
      Arrays.asList("/tmp/nation/part_int=3/d3", "/tmp/nation/part_int=3/d4"),
      "n_nation");
    assertEquals(2, segmentMetadata.size());
  }

  @Test
  public void testPartitionsMetadataAbsent() {
    List<PartitionMetadata> partitionMetadata = basicRequests.partitionsMetadata(
      nationTableInfo,
      Arrays.asList("part_int=3", "part_int=4"),
      "id");
    assertTrue(partitionMetadata.isEmpty());
  }

  @Test
  public void testPartitionsMetadataExisting() {
    List<PartitionMetadata> partitionMetadata = basicRequests.partitionsMetadata(
      nationTableInfo,
      Arrays.asList("part_int=3", "part_int=4"),
      "n_nation");
    assertEquals(2, partitionMetadata.size());
  }

  @Test
  public void testFilesMetadataAbsent() {
    List<FileMetadata> fileMetadata = basicRequests.filesMetadata(
      nationTableInfo,
      "part_int=3",
      Collections.singletonList("/tmp/nation/part_int=3/part_varchar=g/0_0_2.parquet"));
    assertTrue(fileMetadata.isEmpty());
  }

  @Test
  public void testFilesMetadataExisting() {
    List<FileMetadata> fileMetadata = basicRequests.filesMetadata(
      nationTableInfo,
      "part_int=3",
      Arrays.asList("/tmp/nation/part_int=3/part_varchar=g/0_0_0.parquet",
        "/tmp/nation/part_int=3/part_varchar=g/0_0_1.parquet"));
    assertEquals(2, fileMetadata.size());
  }

  @Test
  public void testFileMetadataAbsent() {
    FileMetadata fileMetadata = basicRequests.fileMetadata(
      nationTableInfo,
      "part_int=3",
      "/tmp/nation/part_int=3/part_varchar=g/0_0_2.parquet");
    assertNull(fileMetadata);
  }

  @Test
  public void testFileMetadataExisting() {
    FileMetadata fileMetadata = basicRequests.fileMetadata(
      nationTableInfo,
      "part_int=3",
      "/tmp/nation/part_int=3/part_varchar=g/0_0_0.parquet");
    assertNotNull(fileMetadata);
  }

  @Test
  public void testRowGroupsMetadataAbsent() {
    List<RowGroupMetadata> rowGroupMetadata = basicRequests.rowGroupsMetadata(
      nationTableInfo,
      "part_int=3",
      "/tmp/nation/part_int=3/part_varchar=g/0_0_2.parquet");
    assertTrue(rowGroupMetadata.isEmpty());
  }

  @Test
  public void testRowGroupsMetadataExisting() {
    List<RowGroupMetadata> rowGroupMetadata = basicRequests.rowGroupsMetadata(
      nationTableInfo,
      "part_int=3",
      "/tmp/nation/part_int=3/part_varchar=g/0_0_0.parquet");
    assertEquals(2, rowGroupMetadata.size());
  }

  @Test
  public void testFullSegmentsMetadataWithoutPartitions() {
    BasicTablesTransformer.MetadataHolder metadataHolder = basicRequests.fullSegmentsMetadataWithoutPartitions(
      nationTableInfo,
      Arrays.asList("part_int=4", "part_int=5"),
      Arrays.asList("/tmp/nation/part_int=4/d5", "/tmp/nation/part_int=4/part_varchar=g"));
    assertTrue(metadataHolder.tables().isEmpty());
    assertTrue(metadataHolder.partitions().isEmpty());
    assertEquals(1, metadataHolder.segments().size());
    assertEquals(1, metadataHolder.files().size());
    assertEquals(2, metadataHolder.rowGroups().size());
  }

  @Test
  public void testFilesLastModifiedTime() {
    Map<String, Long> result = basicRequests.filesLastModifiedTime(
      nationTableInfo,
      "part_int=3",
      Collections.singletonList("/tmp/nation/part_int=3/part_varchar=g"));

    Map<String, Long> expected = new HashMap<>();
    expected.put("/tmp/nation/part_int=3/part_varchar=g/0_0_0.parquet", 1L);
    expected.put("/tmp/nation/part_int=3/part_varchar=g/0_0_1.parquet", 2L);

    assertEquals(expected, result);
  }

  @Test
  public void testSegmentsLastModifiedTime() {
    Map<String, Long> result = basicRequests.segmentsLastModifiedTime(
      nationTableInfo,
      Arrays.asList("/tmp/nation/part_int=3/d3", "/tmp/nation/part_int=4/d5"));

    Map<String, Long> expected = new HashMap<>();
    expected.put("part_int=3", 1L);
    expected.put("part_int=4", 3L);

    assertEquals(expected, result);
  }

  @Test
  public void testInterestingColumnsAndPartitionKeys() {
    TableMetadataUnit result = basicRequests.interestingColumnsAndPartitionKeys(nationTableInfo);
    assertEquals(nationTable.interestingColumns(), result.interestingColumns());
    assertEquals(nationTable.partitionKeys(), result.partitionKeys());
    assertNull(result.tableName());
    assertNull(result.lastModifiedTime());
  }

  @Test
  public void testCustomRequest() {
    BasicTablesRequests.RequestMetadata requestMetadata = BasicTablesRequests.RequestMetadata.builder()
      .column("n_nation")
      .metadataType(MetadataType.PARTITION.name())
      .build();

    List<TableMetadataUnit> units = basicRequests.request(requestMetadata);
    assertEquals(2, units.size());
  }

  /**
   * Prepares data which will be used in the unit tests.
   * Note: data is filled to check basic request results and might not be exactly true to reality.
   *
   * @param tables Drill Metastore Tables instance
   */
  private static void prepareData(Tables tables) {
    nationTable = basicUnit().toBuilder()
      .tableName("nation")
      .metadataType(MetadataType.TABLE.name())
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .build();

    nationTableInfo = TableInfo.builder().metadataUnit(nationTable).build();

    TableMetadataUnit nationSegment1 = nationTable.toBuilder()
      .metadataType(MetadataType.SEGMENT.name())
      .metadataKey("part_int=3")
      .location("/tmp/nation/part_int=3/d3")
      .column("n_nation")
      .lastModifiedTime(1L)
      .build();

    TableMetadataUnit nationSegment2 = nationTable.toBuilder()
      .metadataType(MetadataType.SEGMENT.name())
      .metadataKey("part_int=3")
      .location("/tmp/nation/part_int=3/d4")
      .column("n_nation")
      .lastModifiedTime(2L)
      .build();

    TableMetadataUnit nationSegment3 = nationTable.toBuilder()
      .metadataType(MetadataType.SEGMENT.name())
      .metadataKey("part_int=4")
      .location("/tmp/nation/part_int=4/d5")
      .column("n_nation")
      .lastModifiedTime(3L)
      .build();

    TableMetadataUnit nationPartition1 = nationTable.toBuilder()
      .metadataType(MetadataType.PARTITION.name())
      .metadataKey("part_int=3")
      .location("/tmp/nation/part_int=3/d5")
      .column("n_nation")
      .build();

    TableMetadataUnit nationPartition2 = nationTable.toBuilder()
      .metadataType(MetadataType.PARTITION.name())
      .metadataKey("part_int=4")
      .location("/tmp/nation/part_int=4/d5")
      .column("n_nation")
      .build();

    TableMetadataUnit nationPartition3 = nationTable.toBuilder()
      .metadataType(MetadataType.PARTITION.name())
      .metadataKey("part_int=4")
      .column("n_region")
      .location("/tmp/nation/part_int=4/d6")
      .build();

    TableMetadataUnit nationFile1 = nationTable.toBuilder()
      .metadataType(MetadataType.FILE.name())
      .metadataKey("part_int=3")
      .location("/tmp/nation/part_int=3/part_varchar=g")
      .path("/tmp/nation/part_int=3/part_varchar=g/0_0_0.parquet")
      .lastModifiedTime(1L)
      .build();

    TableMetadataUnit nationFile2 = nationTable.toBuilder()
      .metadataType(MetadataType.FILE.name())
      .metadataKey("part_int=3")
      .location("/tmp/nation/part_int=3/part_varchar=g")
      .path("/tmp/nation/part_int=3/part_varchar=g/0_0_1.parquet")
      .lastModifiedTime(System.currentTimeMillis())
      .lastModifiedTime(2L)
      .build();

    TableMetadataUnit nationFile3 = nationTable.toBuilder()
      .metadataType(MetadataType.FILE.name())
      .metadataKey("part_int=4")
      .location("/tmp/nation/part_int=4/part_varchar=g")
      .path("/tmp/nation/part_int=4/part_varchar=g/0_0_0.parquet")
      .lastModifiedTime(3L)
      .build();

    TableMetadataUnit nationRowGroup1 = nationTable.toBuilder()
      .metadataType(MetadataType.ROW_GROUP.name())
      .metadataKey("part_int=3")
      .location("/tmp/nation/part_int=3/part_varchar=g")
      .path("/tmp/nation/part_int=3/part_varchar=g/0_0_0.parquet")
      .rowGroupIndex(1)
      .build();

    TableMetadataUnit nationRowGroup2 = nationTable.toBuilder()
      .metadataType(MetadataType.ROW_GROUP.name())
      .metadataKey("part_int=3")
      .location("/tmp/nation/part_int=3/part_varchar=g")
      .path("/tmp/nation/part_int=3/part_varchar=g/0_0_0.parquet")
      .rowGroupIndex(2)
      .build();

    TableMetadataUnit nationRowGroup3 = nationTable.toBuilder()
      .metadataType(MetadataType.ROW_GROUP.name())
      .metadataKey("part_int=4")
      .location("/tmp/nation/part_int=4/part_varchar=g")
      .path("/tmp/nation/part_int=4/part_varchar=g/0_0_0.parquet")
      .rowGroupIndex(1)
      .build();

    TableMetadataUnit nationRowGroup4 = nationTable.toBuilder()
      .metadataType(MetadataType.ROW_GROUP.name())
      .metadataKey("part_int=4")
      .location("/tmp/nation/part_int=4/part_varchar=g")
      .path("/tmp/nation/part_int=4/part_varchar=g/0_0_0.parquet")
      .rowGroupIndex(2)
      .build();

    TableMetadataUnit regionTable = basicUnit().toBuilder()
      .tableName("region")
      .metadataType(MetadataType.TABLE.name())
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .build();

    tables.modify()
      .overwrite(nationTable,
        nationSegment1, nationSegment2, nationSegment3,
        nationPartition1, nationPartition2, nationPartition3,
        nationFile1, nationFile2, nationFile3,
        nationRowGroup1, nationRowGroup2, nationRowGroup3, nationRowGroup4,
        regionTable)
      .execute();
  }

  /**
   * Returns metadata unit where all fields are filled in.
   * Note: data in the fields may be not exactly true to reality.
   *
   * @return basic metadata unit
   */
  private static TableMetadataUnit basicUnit() {
    return TableMetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("test")
      .owner("user")
      .tableType("parquet")
      .metadataType(MetadataType.NONE.name())
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .location("/tmp/nation")
      .interestingColumns(Arrays.asList("`id`", "`name`"))
      .schema("{\"type\":\"tuple_schema\"," +
        "\"columns\":[{\"name\":\"id\",\"type\":\"INT\",\"mode\":\"REQUIRED\"}," +
        "{\"name\":\"name\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}]," +
        "\"properties\":{\"drill.strict\":\"true\"}}\n")
      .columnsStatistics(Collections.singletonMap("`name`", "{\"statistics\":[{\"statisticsValue\":\"aaa\"," +
        "\"statisticsKind\":{\"exact\":true,\"name\":\"minValue\"}},{\"statisticsValue\":\"zzz\"," +
        "\"statisticsKind\":{\"exact\":true,\"name\":\"maxValue\"}}],\"type\":\"VARCHAR\"}"))
      .metadataStatistics(Collections.singletonList("{\"statisticsValue\":2.1," +
        "\"statisticsKind\":{\"name\":\"approx_count_distinct\"}}"))
      .lastModifiedTime(System.currentTimeMillis())
      .partitionKeys(Collections.singletonMap("dir0", "2018"))
      .additionalMetadata("test table metadata")
      .metadataIdentifier("part_int=3/part_varchar=g/0_0_0.parquet")
      .column("`id`")
      .locations(Arrays.asList("/tmp/nation/1", "/tmp/nation/2"))
      .partitionValues(Arrays.asList("1", "2"))
      .path("/tmp/nation/1/0_0_0.parquet")
      .rowGroupIndex(0)
      .hostAffinity(Collections.singletonMap("host1", 0.1F))
      .build();
  }
}
