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
package org.apache.drill.exec.sql;

import org.apache.commons.io.FileUtils;
import org.apache.drill.categories.MetastoreTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.metastore.analyze.AnalyzeParquetInfoProvider;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.metastore.components.tables.BasicTablesRequests;
import org.apache.drill.metastore.components.tables.MetastoreTableInfo;
import org.apache.drill.metastore.components.tables.TableMetadataUnit;
import org.apache.drill.metastore.metadata.BaseTableMetadata;
import org.apache.drill.metastore.metadata.FileMetadata;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.RowGroupMetadata;
import org.apache.drill.metastore.metadata.SegmentMetadata;
import org.apache.drill.metastore.metadata.TableInfo;
import org.apache.drill.metastore.statistics.BaseStatisticsKind;
import org.apache.drill.metastore.statistics.ColumnStatistics;
import org.apache.drill.metastore.statistics.ColumnStatisticsKind;
import org.apache.drill.metastore.statistics.ExactStatisticsConstants;
import org.apache.drill.metastore.statistics.StatisticsHolder;
import org.apache.drill.metastore.statistics.TableStatisticsKind;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category({SlowTest.class, MetastoreTest.class})
public class TestMetastoreCommands extends ClusterTest {

  private static final TupleMetadata SCHEMA = new SchemaBuilder()
      .addNullable("dir0", TypeProtos.MinorType.VARCHAR)
      .addNullable("dir1", TypeProtos.MinorType.VARCHAR)
      .add("o_orderkey", TypeProtos.MinorType.INT)
      .add("o_custkey", TypeProtos.MinorType.INT)
      .add("o_orderstatus", TypeProtos.MinorType.VARCHAR)
      .add("o_totalprice", TypeProtos.MinorType.FLOAT8)
      .add("o_orderdate", TypeProtos.MinorType.DATE)
      .add("o_orderpriority", TypeProtos.MinorType.VARCHAR)
      .add("o_clerk", TypeProtos.MinorType.VARCHAR)
      .add("o_shippriority", TypeProtos.MinorType.INT)
      .add("o_comment", TypeProtos.MinorType.VARCHAR)
      .build();

  private static final Map<SchemaPath, ColumnStatistics> TABLE_COLUMN_STATISTICS = ImmutableMap.<SchemaPath, ColumnStatistics>builder()
      .put(SchemaPath.getSimplePath("o_shippriority"),
          getColumnStatistics(0, 0, 120L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_orderstatus"),
          getColumnStatistics("F", "P", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderpriority"),
          getColumnStatistics("1-URGENT", "5-LOW", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderkey"),
          getColumnStatistics(1, 1319, 120L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_clerk"),
          getColumnStatistics("Clerk#000000004", "Clerk#000000995", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_totalprice"),
          getColumnStatistics(3266.69, 350110.21, 120L, TypeProtos.MinorType.FLOAT8))
      .put(SchemaPath.getSimplePath("o_comment"),
          getColumnStatistics(" about the final platelets. dependen",
              "zzle. carefully enticing deposits nag furio", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_custkey"),
          getColumnStatistics(25, 1498, 120L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("dir0"),
          getColumnStatistics("1994", "1996", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("dir1"),
          getColumnStatistics("Q1", "Q4", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderdate"),
          getColumnStatistics(757382400000L, 850953600000L, 120L, TypeProtos.MinorType.DATE))
      .build();

  private static final Map<SchemaPath, ColumnStatistics> DIR0_1994_SEGMENT_COLUMN_STATISTICS = ImmutableMap.<SchemaPath, ColumnStatistics>builder()
      .put(SchemaPath.getSimplePath("o_shippriority"),
          getColumnStatistics(0, 0, 40L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_orderstatus"),
          getColumnStatistics("F", "F", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderpriority"),
          getColumnStatistics("1-URGENT", "5-LOW", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderkey"),
          getColumnStatistics(5, 1031, 40L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_clerk"),
          getColumnStatistics("Clerk#000000004", "Clerk#000000973", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_totalprice"),
          getColumnStatistics(3266.69, 350110.21, 40L, TypeProtos.MinorType.FLOAT8))
      .put(SchemaPath.getSimplePath("o_comment"),
          getColumnStatistics(" accounts nag slyly. ironic, ironic accounts wake blithel",
              "yly final requests over the furiously regula", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_custkey"),
          getColumnStatistics(25, 1469, 40L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("dir0"),
          getColumnStatistics("1994", "1994", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("dir1"),
          getColumnStatistics("Q1", "Q4", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderdate"),
          getColumnStatistics(757382400000L, 788140800000L, 40L, TypeProtos.MinorType.DATE))
      .build();

  private static final Map<SchemaPath, ColumnStatistics> DIR0_1994_Q1_SEGMENT_COLUMN_STATISTICS = ImmutableMap.<SchemaPath, ColumnStatistics>builder()
      .put(SchemaPath.getSimplePath("o_shippriority"),
          getColumnStatistics(0, 0, 10L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_orderstatus"),
          getColumnStatistics("F", "F", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderpriority"),
          getColumnStatistics("1-URGENT", "5-LOW", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderkey"),
          getColumnStatistics(66, 833, 10L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_clerk"),
          getColumnStatistics("Clerk#000000062", "Clerk#000000973", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_totalprice"),
          getColumnStatistics(3266.69, 132531.73, 10L, TypeProtos.MinorType.FLOAT8))
      .put(SchemaPath.getSimplePath("o_comment"),
          getColumnStatistics(" special pinto beans use quickly furiously even depende",
              "y pending requests integrate", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_custkey"),
          getColumnStatistics(392, 1411, 10L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("dir0"),
          getColumnStatistics("1994", "1994", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("dir1"),
          getColumnStatistics("Q1", "Q1", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderdate"),
          getColumnStatistics(757382400000L, 764640000000L, 10L, TypeProtos.MinorType.DATE))
      .build();

  private static final MetadataInfo TABLE_META_INFO = MetadataInfo.builder()
      .type(MetadataType.TABLE)
      .key(MetadataInfo.GENERAL_INFO_KEY)
      .build();

  @ClassRule
  public static TemporaryFolder defaultFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUp() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    builder.configProperty(ExecConstants.ZK_ROOT, defaultFolder.getRoot().getPath());
    builder.sessionOption(ExecConstants.METASTORE_ENABLED, true);
    builder.sessionOption(ExecConstants.SLICE_TARGET, 1);
    startCluster(builder);

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"));
    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet2"));
  }

  @Before
  public void prepare() {
    client.alterSession(ExecConstants.METASTORE_ENABLED, true);
    client.alterSession(ExecConstants.METASTORE_USE_SCHEMA_METADATA, true);
    client.alterSession(ExecConstants.METASTORE_USE_STATISTICS_METADATA, true);
    client.alterSession(ExecConstants.SLICE_TARGET, 1);
  }

  @Test
  public void testAnalyzeWithDisabledMetastore() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"));
    client.alterSession(ExecConstants.METASTORE_ENABLED, false);

    try {
      thrown.expect(UserRemoteException.class);
      run("ANALYZE TABLE dfs.`multilevel/parquet` REFRESH METADATA");
    } finally {
      client.resetSession(ExecConstants.METASTORE_ENABLED);
    }
  }

  @Test
  public void testSelectWithDisabledMetastore() throws Exception {
    String tableName = "region_parquet";
    TableInfo tableInfo = getTableInfo(tableName, "tmp");
    try {
      run("create table dfs.tmp.`%s` as\n" +
          "select * from cp.`tpch/region.parquet`", tableName);

      run("analyze table dfs.tmp.`%s` columns none REFRESH METADATA", tableName);

      String query = "select mykey from dfs.tmp.`%s` where mykey is null";

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals("Row count does not match the expected value", 5, actualRowCount);
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(usedMetaPattern)
          .match();

      client.alterSession(ExecConstants.METASTORE_ENABLED, false);

      queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals("Row count does not match the expected value", 5, actualRowCount);
      usedMetaPattern = "usedMetastore=false";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(usedMetaPattern)
          .match();
    } finally {
      cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .modify()
          .delete(tableInfo.toFilter())
          .execute();
      run("drop table if exists dfs.tmp.`%s`", tableName);
      client.resetSession(ExecConstants.METASTORE_ENABLED);
    }
  }

  @Test
  public void testSimpleAnalyze() throws Exception {
    String tableName = "multilevel/parquetSimpleAnalyze";

    TableInfo tableInfo = getTableInfo(tableName, "default");

    File table = dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"), Paths.get(tableName));

    Path tablePath = new Path(table.toURI().getPath());

    BaseTableMetadata expectedTableMetadata = getBaseTableMetadata(tableInfo, table);

    TableInfo baseTableInfo = TableInfo.builder()
        .name(tableName)
        .storagePlugin("dfs")
        .workspace("default")
        .build();
    SegmentMetadata dir0 = SegmentMetadata.builder()
        .tableInfo(baseTableInfo)
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.SEGMENT)
            .identifier("1994")
            .key("1994")
            .build())
        .path(new Path(tablePath, "1994"))
        .schema(SCHEMA)
        .lastModifiedTime(new File(table, "1994").lastModified())
        .column(SchemaPath.getSimplePath("dir0"))
        .columnsStatistics(DIR0_1994_SEGMENT_COLUMN_STATISTICS)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT)))
        .locations(ImmutableSet.of(
            new Path(tablePath, "1994/Q1/orders_94_q1.parquet"),
            new Path(tablePath, "1994/Q2/orders_94_q2.parquet"),
            new Path(tablePath, "1994/Q3/orders_94_q3.parquet"),
            new Path(tablePath, "1994/Q4/orders_94_q4.parquet")))
        .partitionValues(Collections.singletonList("1994"))
        .build();

    Set<Path> expectedTopLevelSegmentLocations = ImmutableSet.of(
        new Path(tablePath, "1994"),
        new Path(tablePath, "1995"),
        new Path(tablePath, "1996"));

    Set<Set<Path>> expectedSegmentFilesLocations = new HashSet<>();

    Set<Path> segmentFiles = ImmutableSet.of(
        new Path(tablePath, "1994/Q2/orders_94_q2.parquet"),
        new Path(tablePath, "1994/Q4/orders_94_q4.parquet"),
        new Path(tablePath, "1994/Q1/orders_94_q1.parquet"),
        new Path(tablePath, "1994/Q3/orders_94_q3.parquet"));
    expectedSegmentFilesLocations.add(segmentFiles);

    segmentFiles = ImmutableSet.of(
        new Path(tablePath, "1995/Q2/orders_95_q2.parquet"),
        new Path(tablePath, "1995/Q4/orders_95_q4.parquet"),
        new Path(tablePath, "1995/Q1/orders_95_q1.parquet"),
        new Path(tablePath, "1995/Q3/orders_95_q3.parquet"));
    expectedSegmentFilesLocations.add(segmentFiles);

    segmentFiles = ImmutableSet.of(
        new Path(tablePath, "1996/Q3/orders_96_q3.parquet"),
        new Path(tablePath, "1996/Q2/orders_96_q2.parquet"),
        new Path(tablePath, "1996/Q4/orders_96_q4.parquet"),
        new Path(tablePath, "1996/Q1/orders_96_q1.parquet"));
    expectedSegmentFilesLocations.add(segmentFiles);

    long dir0q1lastModified = new File(new File(new File(table, "1994"), "Q1"), "orders_94_q1.parquet").lastModified();
    FileMetadata dir01994q1File = FileMetadata.builder()
        .tableInfo(baseTableInfo)
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.FILE)
            .identifier("1994/Q1/orders_94_q1.parquet")
            .key("1994")
            .build())
        .schema(SCHEMA)
        .lastModifiedTime(dir0q1lastModified)
        .columnsStatistics(DIR0_1994_Q1_SEGMENT_COLUMN_STATISTICS)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(10L, TableStatisticsKind.ROW_COUNT)))
        .path(new Path(tablePath, "1994/Q1/orders_94_q1.parquet"))
        .build();

    RowGroupMetadata dir01994q1rowGroup = RowGroupMetadata.builder()
        .tableInfo(baseTableInfo)
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.ROW_GROUP)
            .identifier("1994/Q1/orders_94_q1.parquet/0")
            .key("1994")
            .build())
        .schema(SCHEMA)
        .rowGroupIndex(0)
        .hostAffinity(Collections.emptyMap())
        .lastModifiedTime(dir0q1lastModified)
        .columnsStatistics(DIR0_1994_Q1_SEGMENT_COLUMN_STATISTICS)
        .metadataStatistics(Arrays.asList(
            new StatisticsHolder<>(10L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(1196L, new BaseStatisticsKind(ExactStatisticsConstants.LENGTH, true)),
            new StatisticsHolder<>(4L, new BaseStatisticsKind(ExactStatisticsConstants.START, true))))
        .path(new Path(tablePath, "1994/Q1/orders_94_q1.parquet"))
        .build();

    try {
      run("ANALYZE TABLE dfs.`%s` REFRESH METADATA", tableName);

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      List<SegmentMetadata> topSegmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByColumn(tableInfo, null, "`dir0`");

      SegmentMetadata actualDir0Metadata =
          topSegmentMetadata.stream()
              .filter(unit -> unit.getMetadataInfo().identifier().equals("1994"))
              .findAny().orElseThrow(() -> new AssertionError("Segment is absent"));
      Set<Path> locations = actualDir0Metadata.getLocations();
      actualDir0Metadata.toBuilder().locations(locations);
      assertEquals(dir0, actualDir0Metadata);

      Set<Path> topLevelSegmentLocations = topSegmentMetadata.stream()
          .map(SegmentMetadata::getLocation)
          .collect(Collectors.toSet());

      // verify top segments locations
      assertEquals(
          expectedTopLevelSegmentLocations,
          topLevelSegmentLocations);

      Set<Set<Path>> segmentFilesLocations = topSegmentMetadata.stream()
          .map(SegmentMetadata::getLocations)
          .collect(Collectors.toSet());

      assertEquals(
          expectedSegmentFilesLocations,
          segmentFilesLocations);

      // verify nested segments
      List<SegmentMetadata> nestedSegmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByColumn(tableInfo, null, "`dir1`");

      assertEquals(12, nestedSegmentMetadata.size());

      SegmentMetadata dir01994q1Segment = SegmentMetadata.builder()
          .tableInfo(baseTableInfo)
          .metadataInfo(MetadataInfo.builder()
              .type(MetadataType.SEGMENT)
              .identifier("1994/Q1")
              .key("1994")
              .build())
          .path(new Path(new Path(tablePath, "1994"), "Q1"))
          .schema(SCHEMA)
          .lastModifiedTime(new File(new File(table, "1994"), "Q1").lastModified())
          .column(SchemaPath.getSimplePath("dir1"))
          .columnsStatistics(DIR0_1994_Q1_SEGMENT_COLUMN_STATISTICS)
          .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(10L, TableStatisticsKind.ROW_COUNT)))
          .locations(ImmutableSet.of(new Path(tablePath, "1994/Q1/orders_94_q1.parquet")))
          .partitionValues(Arrays.asList("1994", "Q1"))
          .build();

      // verify segment for 1994
      assertEquals(dir01994q1Segment,
          nestedSegmentMetadata.stream()
              .filter(unit -> unit.getMetadataInfo().identifier().equals("1994/Q1"))
              .findAny()
              .orElse(null));

      // verify files metadata
      List<FileMetadata> filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      // verify first file metadata
      assertEquals(dir01994q1File,
          filesMetadata.stream()
              .filter(unit -> unit.getMetadataInfo().identifier().equals("1994/Q1/orders_94_q1.parquet"))
              .findAny()
              .orElse(null));

      // verify row groups metadata
      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());

      // verify first row group dir01994q1rowGroup
      assertEquals(dir01994q1rowGroup,
          rowGroupsMetadata.stream()
              .filter(unit -> unit.getMetadataInfo().identifier().equals("1994/Q1/orders_94_q1.parquet/0"))
              .findAny()
              .orElse(null));
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testTableMetadataWithLevels() throws Exception {
    List<MetadataType> analyzeLevels =
        Arrays.asList(MetadataType.ROW_GROUP, MetadataType.FILE, MetadataType.SEGMENT, MetadataType.TABLE);

    String tableName = "multilevel/parquetLevels";
    File tablePath = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    for (MetadataType analyzeLevel : analyzeLevels) {
      BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
          .tableInfo(tableInfo)
          .metadataInfo(TABLE_META_INFO)
          .schema(SCHEMA)
          .location(new Path(tablePath.toURI().getPath()))
          .columnsStatistics(TABLE_COLUMN_STATISTICS)
          .metadataStatistics(Arrays.asList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
              new StatisticsHolder<>(analyzeLevel, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
          .partitionKeys(Collections.emptyMap())
          .lastModifiedTime(tablePath.lastModified())
          .build();

      try {
        run("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA '%s' level", tableName, analyzeLevel.name());

        BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
            .getMetastoreRegistry()
            .get()
            .tables()
            .basicRequests()
            .tableMetadata(tableInfo);

        assertEquals(expectedTableMetadata, actualTableMetadata);
      } finally {
        run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
      }
    }
  }

  @Test
  public void testAnalyzeLowerLevelMetadata() throws Exception {
    // checks that metadata for levels below specified in analyze statement is absent
    String tableName = "multilevel/parquetLowerLevel";

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    List<MetadataType> analyzeLevels =
        Arrays.asList(MetadataType.FILE, MetadataType.SEGMENT, MetadataType.TABLE);

    for (MetadataType analyzeLevel : analyzeLevels) {
      try {
        run("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA '%s' level", tableName, analyzeLevel.name());

        List<String> emptyMetadataLevels = Arrays.stream(MetadataType.values())
            .filter(metadataType -> metadataType.compareTo(analyzeLevel) > 0
                // for the case when there are no segment metadata, default segment is present
                && metadataType.compareTo(MetadataType.SEGMENT) > 0
                && metadataType.compareTo(MetadataType.ALL) < 0)
            .map(Enum::name)
            .collect(Collectors.toList());

        BasicTablesRequests.RequestMetadata requestMetadata = BasicTablesRequests.RequestMetadata.builder()
            .tableInfo(tableInfo)
            .metadataTypes(emptyMetadataLevels)
            .build();

        List<TableMetadataUnit> metadataUnitList = cluster.drillbit().getContext()
            .getMetastoreRegistry()
            .get()
            .tables()
            .read()
            .filter(requestMetadata.filter())
            .execute();

        assertTrue(
            String.format("Some metadata [%s] for [%s] analyze query level is present" + metadataUnitList, emptyMetadataLevels, analyzeLevel),
            metadataUnitList.isEmpty());
      } finally {
        run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
      }
    }
  }

  @Test
  public void testAnalyzeWithColumns() throws Exception {
    String tableName = "multilevel/parquetColumns";
    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));
    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    Map<SchemaPath, ColumnStatistics> updatedTableColumnStatistics = new HashMap<>();

    SchemaPath orderStatusPath = SchemaPath.getSimplePath("o_orderstatus");
    SchemaPath dir0Path = SchemaPath.getSimplePath("dir0");
    SchemaPath dir1Path = SchemaPath.getSimplePath("dir1");

    updatedTableColumnStatistics.put(orderStatusPath, TABLE_COLUMN_STATISTICS.get(orderStatusPath));
    updatedTableColumnStatistics.put(dir0Path, TABLE_COLUMN_STATISTICS.get(dir0Path));
    updatedTableColumnStatistics.put(dir1Path, TABLE_COLUMN_STATISTICS.get(dir1Path));

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedTableColumnStatistics)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ROW_GROUP, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .interestingColumns(Collections.singletonList(orderStatusPath))
        .build();

    try {
      run("ANALYZE TABLE dfs.tmp.`%s` columns(o_orderstatus) REFRESH METADATA 'row_group' LEVEL", tableName);

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testAnalyzeWithNoColumns() throws Exception {
    String tableName = "multilevel/parquetNoColumns";
    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));
    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    Map<SchemaPath, ColumnStatistics> updatedTableColumnStatistics = new HashMap<>();

    SchemaPath dir0Path = SchemaPath.getSimplePath("dir0");
    SchemaPath dir1Path = SchemaPath.getSimplePath("dir1");

    updatedTableColumnStatistics.put(dir0Path, TABLE_COLUMN_STATISTICS.get(dir0Path));
    updatedTableColumnStatistics.put(dir1Path, TABLE_COLUMN_STATISTICS.get(dir1Path));

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedTableColumnStatistics)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ROW_GROUP, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .interestingColumns(Collections.emptyList())
        .build();

    try {
      run("ANALYZE TABLE dfs.tmp.`%s` columns NONE REFRESH METADATA 'row_group' LEVEL", tableName);

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testIncrementalAnalyzeWithFewerColumns() throws Exception {
    String tableName = "multilevel/parquetFewerColumns";
    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));
    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    Map<SchemaPath, ColumnStatistics> updatedTableColumnStatistics = new HashMap<>();

    SchemaPath orderStatusPath = SchemaPath.getSimplePath("o_orderstatus");
    SchemaPath orderDatePath = SchemaPath.getSimplePath("o_orderdate");
    SchemaPath dir0Path = SchemaPath.getSimplePath("dir0");
    SchemaPath dir1Path = SchemaPath.getSimplePath("dir1");

    updatedTableColumnStatistics.put(orderStatusPath, TABLE_COLUMN_STATISTICS.get(orderStatusPath));
    updatedTableColumnStatistics.put(orderDatePath, TABLE_COLUMN_STATISTICS.get(orderDatePath));
    updatedTableColumnStatistics.put(dir0Path, TABLE_COLUMN_STATISTICS.get(dir0Path));
    updatedTableColumnStatistics.put(dir1Path, TABLE_COLUMN_STATISTICS.get(dir1Path));

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedTableColumnStatistics)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ROW_GROUP, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .interestingColumns(Arrays.asList(orderStatusPath, orderDatePath))
        .build();

    try {
      run("ANALYZE TABLE dfs.tmp.`%s` columns(o_orderstatus, o_orderdate) REFRESH METADATA 'row_group' LEVEL", tableName);

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      // checks that analyze wasn't produced though interesting columns list differs, but it is a sublist of previously analyzed table
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` columns(o_orderstatus) REFRESH METADATA 'row_group' LEVEL", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(false, "Table metadata is up to date, analyze wasn't performed.")
          .go();

      actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testIncrementalAnalyzeWithMoreColumns() throws Exception {
    String tableName = "multilevel/parquetMoreColumns";
    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));
    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    Map<SchemaPath, ColumnStatistics> updatedTableColumnStatistics = new HashMap<>();

    SchemaPath orderStatusPath = SchemaPath.getSimplePath("o_orderstatus");
    SchemaPath orderDatePath = SchemaPath.getSimplePath("o_orderdate");
    SchemaPath dir0Path = SchemaPath.getSimplePath("dir0");
    SchemaPath dir1Path = SchemaPath.getSimplePath("dir1");

    updatedTableColumnStatistics.put(orderStatusPath, TABLE_COLUMN_STATISTICS.get(orderStatusPath));
    updatedTableColumnStatistics.put(dir0Path, TABLE_COLUMN_STATISTICS.get(dir0Path));
    updatedTableColumnStatistics.put(dir1Path, TABLE_COLUMN_STATISTICS.get(dir1Path));

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedTableColumnStatistics)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ROW_GROUP, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .interestingColumns(Collections.singletonList(orderStatusPath))
        .build();

    try {
      run("ANALYZE TABLE dfs.tmp.`%s` columns(o_orderstatus) REFRESH METADATA 'row_group' LEVEL", tableName);

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      // checks that analyze was produced since interesting columns list differs, and second columns list isn't a sublist of previously analyzed table
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` columns(o_orderstatus, o_orderdate) REFRESH METADATA 'row_group' LEVEL", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      updatedTableColumnStatistics.put(orderDatePath, TABLE_COLUMN_STATISTICS.get(orderDatePath));

      assertEquals(
          expectedTableMetadata.toBuilder()
              .columnsStatistics(updatedTableColumnStatistics)
              .interestingColumns(Arrays.asList(orderStatusPath, orderDatePath))
              .build(),
          actualTableMetadata);
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testIncrementalAnalyzeWithEmptyColumns() throws Exception {
    String tableName = "multilevel/parquetEmptyColumns";
    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));
    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    Map<SchemaPath, ColumnStatistics> updatedTableColumnStatistics = new HashMap<>();

    SchemaPath orderStatusPath = SchemaPath.getSimplePath("o_orderstatus");
    SchemaPath orderDatePath = SchemaPath.getSimplePath("o_orderdate");
    SchemaPath dir0Path = SchemaPath.getSimplePath("dir0");
    SchemaPath dir1Path = SchemaPath.getSimplePath("dir1");

    updatedTableColumnStatistics.put(orderStatusPath, TABLE_COLUMN_STATISTICS.get(orderStatusPath));
    updatedTableColumnStatistics.put(orderDatePath, TABLE_COLUMN_STATISTICS.get(orderDatePath));
    updatedTableColumnStatistics.put(dir0Path, TABLE_COLUMN_STATISTICS.get(dir0Path));
    updatedTableColumnStatistics.put(dir1Path, TABLE_COLUMN_STATISTICS.get(dir1Path));

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedTableColumnStatistics)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ROW_GROUP, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .interestingColumns(Arrays.asList(orderStatusPath, orderDatePath))
        .build();

    try {
      run("ANALYZE TABLE dfs.tmp.`%s` columns(o_orderstatus, o_orderdate) REFRESH METADATA 'row_group' LEVEL", tableName);

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      // checks that analyze wasn't produced though interesting columns list differs, but it is a sublist of previously analyzed table
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` columns NONE REFRESH METADATA 'row_group' LEVEL", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(false, "Table metadata is up to date, analyze wasn't performed.")
          .go();

      actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testIncrementalAnalyzeUnchangedTable() throws Exception {
    String tableName = "multilevel/parquetUnchanged";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    long lastModifiedTime = table.lastModified();

    try {
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(false, "Table metadata is up to date, analyze wasn't performed.")
          .go();

      segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      long postAnalyzeLastModifiedTime = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .metastoreTableInfo(tableInfo)
          .lastModifiedTime();

      assertEquals(lastModifiedTime, postAnalyzeLastModifiedTime);
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalAnalyzeNewParentSegment() throws Exception {
    String tableName = "multilevel/parquetNewParentSegment";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    // updates statistics values due to new segment
    Map<SchemaPath, ColumnStatistics> updatedStatistics = new HashMap<>(TABLE_COLUMN_STATISTICS);
    updatedStatistics.replaceAll((logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics<>(
            Arrays.asList(
                new StatisticsHolder<>(160L, TableStatisticsKind.ROW_COUNT),
                new StatisticsHolder<>(160L, ColumnStatisticsKind.NON_NULL_COUNT)))));

    updatedStatistics.computeIfPresent(SchemaPath.getSimplePath("dir0"), (logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics<>(
            Collections.singletonList(new StatisticsHolder<>("1993", ColumnStatisticsKind.MIN_VALUE)))));

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedStatistics)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(160L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ALL, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .build();

    try {
      assertEquals(0, cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null).size());

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet", "1994"), Paths.get(tableName, "1993"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(20, segmentMetadata.size());
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalAnalyzeNewChildSegment() throws Exception {
    String tableName = "multilevel/parquetNewChildSegment";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    // updates statistics values due to new segment
    Map<SchemaPath, ColumnStatistics> updatedStatistics = new HashMap<>(TABLE_COLUMN_STATISTICS);
    updatedStatistics.replaceAll((logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics(
            Arrays.asList(
                new StatisticsHolder<>(130L, TableStatisticsKind.ROW_COUNT),
                new StatisticsHolder<>(130L, ColumnStatisticsKind.NON_NULL_COUNT)))));

    updatedStatistics.computeIfPresent(SchemaPath.getSimplePath("dir1"), (logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics(
            Collections.singletonList(new StatisticsHolder<>("Q5", ColumnStatisticsKind.MAX_VALUE)))));

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedStatistics)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(130L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ALL, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .build();

    try {
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel", "parquet", "1994", "Q4"), Paths.get(tableName, "1994", "Q5"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(16, segmentMetadata.size());
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalAnalyzeNewFile() throws Exception {
    String tableName = "multilevel/parquetNewFile";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    // updates statistics values due to new segment
    Map<SchemaPath, ColumnStatistics> updatedStatistics = new HashMap<>(TABLE_COLUMN_STATISTICS);
    updatedStatistics.replaceAll((logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics(
            Arrays.asList(
                new StatisticsHolder<>(130L, TableStatisticsKind.ROW_COUNT),
                new StatisticsHolder<>(130L, ColumnStatisticsKind.NON_NULL_COUNT)))));

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedStatistics)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(130L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ALL, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .build();

    try {
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      List<SegmentMetadata> segmentsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentsMetadata.size());

      List<FileMetadata> filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());

      dirTestWatcher.copyResourceToTestTmp(
          Paths.get("multilevel", "parquet", "1994", "Q4", "orders_94_q4.parquet"),
          Paths.get(tableName, "1994", "Q4", "orders_94_q4_1.parquet"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      segmentsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      // verifies that segments count left unchanged
      assertEquals(15, segmentsMetadata.size());

      filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(13, filesMetadata.size());

      rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(13, rowGroupsMetadata.size());
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  public void testIncrementalAnalyzeRemovedParentSegment() throws Exception {
    String tableName = "multilevel/parquetRemovedParent";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    BaseTableMetadata expectedTableMetadata = getBaseTableMetadata(tableInfo, table);

    try {
      dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet", "1994"), Paths.get(tableName, "1993"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(20, segmentMetadata.size());

      List<FileMetadata> filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(16, filesMetadata.size());

      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(16, rowGroupsMetadata.size());

      FileUtils.deleteQuietly(new File(table, "1993"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  public void testIncrementalAnalyzeRemovedNestedSegment() throws Exception {
    String tableName = "multilevel/parquetRemovedNestedSegment";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    BaseTableMetadata expectedTableMetadata = getBaseTableMetadata(tableInfo, table);

    try {
      dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet", "1994", "Q4"), Paths.get(tableName, "1994", "Q5"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(16, segmentMetadata.size());

      List<FileMetadata> filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(13, filesMetadata.size());

      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(13, rowGroupsMetadata.size());

      FileUtils.deleteQuietly(new File(new File(table, "1994"), "Q5"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  public void testIncrementalAnalyzeRemovedFile() throws Exception {
    String tableName = "multilevel/parquetRemovedFile";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    BaseTableMetadata expectedTableMetadata = getBaseTableMetadata(tableInfo, table);

    try {
      dirTestWatcher.copyResourceToTestTmp(
          Paths.get("multilevel", "parquet", "1994", "Q4", "orders_94_q4.parquet"),
          Paths.get(tableName, "1994", "Q4", "orders_94_q4_1.parquet"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      List<FileMetadata> filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(13, filesMetadata.size());

      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(13, rowGroupsMetadata.size());

      FileUtils.deleteQuietly(new File(new File(new File(table, "1994"), "Q4"), "orders_94_q4_1.parquet"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalAnalyzeUpdatedFile() throws Exception {
    String tableName = "multilevel/parquetUpdatedFile";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    try {
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      List<FileMetadata> filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());

      File fileToUpdate = new File(new File(new File(table, "1994"), "Q4"), "orders_94_q4.parquet");
      long lastModified = fileToUpdate.lastModified();
      FileUtils.deleteQuietly(fileToUpdate);

      // replaces original file
      dirTestWatcher.copyResourceToTestTmp(
          Paths.get("multilevel", "parquet", "1994", "Q1", "orders_94_q1.parquet"),
          Paths.get(tableName, "1994", "Q4", "orders_94_q4.parquet"));

      assertTrue(fileToUpdate.setLastModified(lastModified + 1000));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      Map<SchemaPath, ColumnStatistics> tableColumnStatistics = new HashMap<>(TABLE_COLUMN_STATISTICS);
      tableColumnStatistics.computeIfPresent(SchemaPath.getSimplePath("o_clerk"),
          (logicalExpressions, columnStatistics) ->
              columnStatistics.cloneWith(new ColumnStatistics(
                  Collections.singletonList(new StatisticsHolder<>("Clerk#000000006", ColumnStatisticsKind.MIN_VALUE)))));

      tableColumnStatistics.computeIfPresent(SchemaPath.getSimplePath("o_totalprice"),
          (logicalExpressions, columnStatistics) ->
              columnStatistics.cloneWith(new ColumnStatistics(
                  Collections.singletonList(new StatisticsHolder<>(328207.15, ColumnStatisticsKind.MAX_VALUE)))));

      BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
          .tableInfo(tableInfo)
          .metadataInfo(TABLE_META_INFO)
          .schema(SCHEMA)
          .location(new Path(table.toURI().getPath()))
          .columnsStatistics(tableColumnStatistics)
          .metadataStatistics(Arrays.asList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
              new StatisticsHolder<>(MetadataType.ALL, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
          .partitionKeys(Collections.emptyMap())
          .lastModifiedTime(lastModified + 1000)
          .build();

      assertEquals(expectedTableMetadata, actualTableMetadata);

      segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      filesMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      rowGroupsMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  public void testIncrementalAnalyzeWithDifferentMetadataLevel() throws Exception {
    String tableName = "multilevel/parquetDifferentMetadataLevel";
    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));
    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(TABLE_COLUMN_STATISTICS)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.FILE, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .build();

    try {
      run("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA 'file' LEVEL", tableName);

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      List<RowGroupMetadata> rowGroupMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, (String) null, null);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      assertTrue(rowGroupMetadata.isEmpty());

      // checks that analyze was produced since metadata level more specific
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA 'row_group' LEVEL", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      expectedTableMetadata = BaseTableMetadata.builder()
          .tableInfo(tableInfo)
          .metadataInfo(TABLE_META_INFO)
          .schema(SCHEMA)
          .location(tablePath)
          .columnsStatistics(TABLE_COLUMN_STATISTICS)
          .metadataStatistics(Arrays.asList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
              new StatisticsHolder<>(MetadataType.ROW_GROUP, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
          .partitionKeys(Collections.emptyMap())
          .lastModifiedTime(table.lastModified())
          .build();

      assertEquals(expectedTableMetadata, actualTableMetadata);

      rowGroupMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, (String) null, null);

      assertEquals(12, rowGroupMetadata.size());
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDefaultSegment() throws Exception {
    String tableName = "multilevel/parquet/1994/Q1";
    Path tablePath = new Path(dirTestWatcher.getRootDir().toURI().getPath(), tableName);

    TableInfo tableInfo = getTableInfo(tableName, "default");

    Map<SchemaPath, ColumnStatistics> tableColumnStatistics = new HashMap<>(TABLE_COLUMN_STATISTICS);
    tableColumnStatistics.remove(SchemaPath.getSimplePath("dir0"));
    tableColumnStatistics.remove(SchemaPath.getSimplePath("dir1"));

    tableColumnStatistics.put(SchemaPath.getSimplePath("o_orderstatus"),
            getColumnStatistics("F", "F", 120L, TypeProtos.MinorType.VARCHAR));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_orderkey"),
            getColumnStatistics(66, 833, 833L, TypeProtos.MinorType.INT));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_clerk"),
            getColumnStatistics("Clerk#000000062", "Clerk#000000973", 120L, TypeProtos.MinorType.VARCHAR));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_totalprice"),
            getColumnStatistics(3266.69, 132531.73, 120L, TypeProtos.MinorType.FLOAT8));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_comment"),
            getColumnStatistics(" special pinto beans use quickly furiously even depende",
                "y pending requests integrate", 120L, TypeProtos.MinorType.VARCHAR));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_custkey"),
            getColumnStatistics(392, 1411, 120L, TypeProtos.MinorType.INT));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_orderdate"),
            getColumnStatistics(757382400000L, 764640000000L, 120L, TypeProtos.MinorType.DATE));

    tableColumnStatistics.replaceAll((logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics(
            Arrays.asList(
                new StatisticsHolder<>(10L, TableStatisticsKind.ROW_COUNT),
                new StatisticsHolder<>(10L, ColumnStatisticsKind.NON_NULL_COUNT)))));

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(new SchemaBuilder()
            .add("o_orderkey", TypeProtos.MinorType.INT)
            .add("o_custkey", TypeProtos.MinorType.INT)
            .add("o_orderstatus", TypeProtos.MinorType.VARCHAR)
            .add("o_totalprice", TypeProtos.MinorType.FLOAT8)
            .add("o_orderdate", TypeProtos.MinorType.DATE)
            .add("o_orderpriority", TypeProtos.MinorType.VARCHAR)
            .add("o_clerk", TypeProtos.MinorType.VARCHAR)
            .add("o_shippriority", TypeProtos.MinorType.INT)
            .add("o_comment", TypeProtos.MinorType.VARCHAR)
            .build())
        .location(tablePath)
        .columnsStatistics(tableColumnStatistics)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(10L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ALL, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName).lastModified())
        .build();

    SegmentMetadata defaultSegment = SegmentMetadata.builder()
        .tableInfo(TableInfo.builder()
            .name(tableName)
            .storagePlugin("dfs")
            .workspace("default")
            .build())
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.SEGMENT)
            .key(MetadataInfo.DEFAULT_SEGMENT_KEY)
            .build())
        .lastModifiedTime(new File(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName), "orders_94_q1.parquet").lastModified())
        .columnsStatistics(Collections.emptyMap())
        .metadataStatistics(Collections.emptyList())
        .path(new Path(dirTestWatcher.getRootDir().toURI().getPath(), tableName))
        .locations(ImmutableSet.of(
            new Path(tablePath, "orders_94_q1.parquet")))
        .build();

    try {
      run("ANALYZE TABLE dfs.`%s` REFRESH METADATA", tableName);

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(1, segmentMetadata.size());

      assertEquals(defaultSegment, segmentMetadata.get(0));
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testAnalyzeWithMapColumns() throws Exception {
    String tableName = "complex";

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("store/parquet/complex/complex.parquet"), Paths.get(tableName));

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("trans_id", TypeProtos.MinorType.BIGINT)
        .addNullable("date", TypeProtos.MinorType.VARCHAR)
        .addNullable("time", TypeProtos.MinorType.VARCHAR)
        .addNullable("amount", TypeProtos.MinorType.FLOAT8)
        .addMap("user_info")
            .addNullable("cust_id", TypeProtos.MinorType.BIGINT)
            .addNullable("device", TypeProtos.MinorType.VARCHAR)
            .addNullable("state", TypeProtos.MinorType.VARCHAR)
            .resumeSchema()
        .addMap("marketing_info")
            .addNullable("camp_id", TypeProtos.MinorType.BIGINT)
            .addArray("keywords", TypeProtos.MinorType.VARCHAR)
            .resumeSchema()
        .addMap("trans_info")
            .addArray("prod_id", TypeProtos.MinorType.BIGINT)
            .addNullable("purch_flag", TypeProtos.MinorType.VARCHAR)
            .resumeSchema()
        .build();

    Map<SchemaPath, ColumnStatistics> columnStatistics = ImmutableMap.<SchemaPath, ColumnStatistics>builder()
        .put(SchemaPath.getCompoundPath("user_info", "state"),
            getColumnStatistics("ct", "nj", 5L, TypeProtos.MinorType.VARCHAR))
        .put(SchemaPath.getSimplePath("date"),
            getColumnStatistics("2013-05-16", "2013-07-26", 5L, TypeProtos.MinorType.VARCHAR))
        .put(SchemaPath.getSimplePath("time"),
            getColumnStatistics("04:56:59", "15:31:45", 5L, TypeProtos.MinorType.VARCHAR))
        .put(SchemaPath.getCompoundPath("user_info", "cust_id"),
            getColumnStatistics(11L, 86623L, 5L, TypeProtos.MinorType.BIGINT))
        .put(SchemaPath.getSimplePath("amount"),
            getColumnStatistics(20.25, 500.75, 5L, TypeProtos.MinorType.FLOAT8))
        .put(SchemaPath.getCompoundPath("user_info", "device"),
            getColumnStatistics("AOS4.2", "IOS7", 5L, TypeProtos.MinorType.VARCHAR))
        .put(SchemaPath.getCompoundPath("marketing_info", "camp_id"),
            getColumnStatistics(4L, 17L, 5L, TypeProtos.MinorType.BIGINT))
        .put(SchemaPath.getSimplePath("trans_id"),
            getColumnStatistics(0L, 4L, 5L, TypeProtos.MinorType.BIGINT))
        .put(SchemaPath.getCompoundPath("trans_info", "purch_flag"),
            getColumnStatistics("false", "true", 5L, TypeProtos.MinorType.VARCHAR))
        .build();

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(schema)
        .location(new Path(table.toURI().getPath()))
        .columnsStatistics(columnStatistics)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(5L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ALL, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .build();

    try {
      run("analyze table dfs.tmp.`%s` REFRESH METADATA", tableName);

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata);
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testDirPartitionPruning() throws Exception {
    String tableName = "multilevel/parquetDir";

    dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    try {
      run("analyze table dfs.tmp.`%s` REFRESH METADATA", tableName);

      String query =
          "select dir0, dir1, o_custkey, o_orderdate from dfs.tmp.`%s`\n" +
          "where dir0=1994 and dir1 in ('Q1', 'Q2')";
      long expectedRowCount = 20;
      int expectedNumFiles = 2;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();

      assertEquals(expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testPartitionPruningRootSegment() throws Exception {
    String tableName = "multilevel/parquetRootSegment";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"), Paths.get(tableName));

    try {
      run("analyze table dfs.`%s` REFRESH METADATA", tableName);

      String query =
          "select dir0, dir1, o_custkey, o_orderdate from dfs.`%s`\n" +
          "where dir0=1994";
      long expectedRowCount = 40;
      int expectedNumFiles = 4;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();

      assertEquals(expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testPartitionPruningVarCharPartition() throws Exception {
    String tableName = "orders_ctas_varchar";

    try {
      run("create table dfs.%s (o_orderdate, o_orderpriority) partition by (o_orderpriority)\n"
          + "as select o_orderdate, o_orderpriority from dfs.`multilevel/parquet/1994/Q1`", tableName);

      run("analyze table dfs.`%s` REFRESH METADATA", tableName);

      String query = "select * from dfs.%s where o_orderpriority = '1-URGENT'";
      long expectedRowCount = 3;
      int expectedNumFiles = 1;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals(expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
      run("drop table if exists dfs.`%s`", tableName);
    }
  }

  @Test
  public void testPartitionPruningBinaryPartition() throws Exception {
    String tableName = "orders_ctas_binary";

    try {
      run("create table dfs.%s (o_orderdate, o_orderpriority) partition by (o_orderpriority)\n"
          + "as select o_orderdate, convert_to(o_orderpriority, 'UTF8') as o_orderpriority\n"
          + "from dfs.`multilevel/parquet/1994/Q1`", tableName);

      run("analyze table dfs.`%s` REFRESH METADATA", tableName);
      String query = String.format("select * from dfs.%s where o_orderpriority = '1-URGENT'", tableName);
      long expectedRowCount = 3;
      int expectedNumFiles = 1;

      long actualRowCount = queryBuilder().sql(query).run().recordCount();
      assertEquals(expectedRowCount, actualRowCount);

      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
      run("drop table if exists dfs.`%s`", tableName);
    }
  }

  @Test
  public void testPartitionPruningSingleLeafPartition() throws Exception {
    String tableName = "multilevel/parquetSingleLeafPartition";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet2"), Paths.get(tableName));

    try {
      run("analyze table dfs.`%s` REFRESH METADATA", tableName);

      String query =
          "select dir0, dir1, o_custkey, o_orderdate from dfs.`%s`\n" +
          "where dir0=1995 and dir1='Q3'";
      long expectedRowCount = 20;
      int expectedNumFiles = 2;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals(expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testPartitionPruningSingleNonLeafPartition() throws Exception {
    String tableName = "multilevel/parquetSingleNonLeafPartition";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet2"), Paths.get(tableName));

    try {
      run("analyze table dfs.`%s` REFRESH METADATA", tableName);

      String query =
          "select dir0, dir1, o_custkey, o_orderdate from dfs.`%s`\n" +
          "where dir0=1995";
      long expectedRowCount = 80;
      int expectedNumFiles = 8;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals(expectedRowCount, actualRowCount);

      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testPartitionPruningDir1Filter() throws Exception {
    String tableName = "multilevel/parquetDir1";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet2"), Paths.get(tableName));

    try {
      run("analyze table dfs.`%s` REFRESH METADATA", tableName);

      String query =
          "select dir0, dir1, o_custkey, o_orderdate from dfs.`%s`\n" +
          "where dir1='Q3'";
      long expectedRowCount = 40;
      int expectedNumFiles = 4;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals(expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .match();
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testPartitionPruningNonExistentPartition() throws Exception {
    String tableName = "multilevel/parquetNonExistentPartition";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet2"), Paths.get(tableName));

    try {
      run("analyze table dfs.`%s` REFRESH METADATA", tableName);

      String query =
          "select dir0, dir1, o_custkey, o_orderdate from dfs.`%s`\n" +
          "where dir0=1995 and dir1='Q6'";
      long expectedRowCount = 0;
      int expectedNumFiles = 1;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals(expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .match();
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  @Ignore("Ignored due to schema change connected with absence of `dir0` partition field for one of files")
  public void testAnalyzeMultilevelTable() throws Exception {
    String tableName = "path with spaces";

    try {
      // table with directory and file at the same level
      run("create table dfs.`%s` as select * from cp.`tpch/nation.parquet`", tableName);
      run("create table dfs.`%1$s/%1$s` as select * from cp.`tpch/nation.parquet`", tableName);

      run("analyze table dfs.`%s` REFRESH METADATA", tableName);

      String query = "select * from  dfs.`%s`";
      long expectedRowCount = 50;
      int expectedNumFiles = 2;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals("An incorrect result was obtained while querying a table with metadata cache files",
          expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .match();
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
      run("drop table if exists dfs.`%s`", tableName);
    }
  }

  @Test
  public void testFieldWithDots() throws Exception {
    String tableName = "dfs.tmp.`complex_table`";
    try {
      run("create table %s as\n" +
          "select cast(1 as int) as `column.with.dots`, t.`column`.`with.dots`\n" +
          "from cp.`store/parquet/complex/complex.parquet` t limit 1", tableName);

      String query = "select * from %s";
      int expectedRowCount = 1;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include("usedMetastore=false")
          .match();

      run("analyze table %s REFRESH METADATA", tableName);

      actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();

      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include("usedMetastore=true")
          .match();
    } finally {
      run("analyze table %s drop metadata if exists", tableName);
      run("drop table if exists %s", tableName);
    }
  }

  @Test
  public void testBooleanPartitionPruning() throws Exception {
    String tableName = "dfs.tmp.`interval_bool_partition`";

    try {
      run("create table %s partition by (col_bln) as\n" +
          "select * from cp.`parquet/alltypes_required.parquet`", tableName);

      run("analyze table %s REFRESH METADATA", tableName);

      String query = "select * from %s where col_bln = true";
      int expectedRowCount = 2;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals("Row count does not match the expected value", expectedRowCount, actualRowCount);
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table %s drop metadata if exists", tableName);
      run("drop table if exists %s", tableName);
    }
  }

  @Test
  public void testIntWithNullsPartitionPruning() throws Exception {
    String tableName = "t5";

    try {
      run("create table dfs.tmp.`%s/a` as\n" +
          "select 100 as mykey from cp.`tpch/nation.parquet`\n" +
          "union all\n" +
          "select col_notexist from cp.`tpch/region.parquet`", tableName);

      run("create table dfs.tmp.`%s/b` as\n" +
          "select 200 as mykey from cp.`tpch/nation.parquet`\n" +
          "union all\n" +
          "select col_notexist from cp.`tpch/region.parquet`", tableName);

      run("analyze table dfs.tmp.`%s` REFRESH METADATA", tableName);

      String query = "select mykey from dfs.tmp.`t5` where mykey = 100";
      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals("Row count does not match the expected value", 25, actualRowCount);

      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(usedMetaPattern)
          .match();
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
      run("drop table if exists dfs.tmp.`%s`", tableName);
    }
  }

  @Test
  public void testPartitionPruningWithIsNull() throws Exception {
    String tableName = "t6";

    try {
      run("create table dfs.tmp.`%s/a` as\n" +
          "select col_notexist as mykey from cp.`tpch/region.parquet`", tableName);

      run("create table dfs.tmp.`%s/b` as\n" +
          "select case when true then 100 else null end as mykey from cp.`tpch/region.parquet`", tableName);

      run("analyze table dfs.tmp.`%s` REFRESH METADATA", tableName);

      String query = "select mykey from dfs.tmp.`%s` where mykey is null";

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals("Row count does not match the expected value", 5, actualRowCount);
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
      run("drop table if exists dfs.tmp.`%s`", tableName);
    }
  }

  @Test
  public void testPartitionPruningWithIsNotNull() throws Exception {
    String tableName = "t7";

    try {
      run("create table dfs.tmp.`%s/a` as\n" +
          "select col_notexist as mykey from cp.`tpch/region.parquet`", tableName);

      run("create table dfs.tmp.`%s/b` as\n" +
          "select  case when true then 100 else null end as mykey from cp.`tpch/region.parquet`", tableName);

      run("analyze table dfs.tmp.`%s` REFRESH METADATA", tableName);

      String query = "select mykey from dfs.tmp.`%s` where mykey is null";

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals("Row count does not match the expected value", 5, actualRowCount);
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
      run("drop table if exists dfs.tmp.`%s`", tableName);
    }
  }

  @Test
  public void testNonInterestingColumnInFilter() throws Exception {
    String tableName = "t8";

    try {
      run("create table dfs.tmp.`%s/a` as\n" +
          "select col_notexist as mykey from cp.`tpch/region.parquet`", tableName);

      run("create table dfs.tmp.`%s/b` as\n" +
          "select case when true then 100 else null end as mykey from cp.`tpch/region.parquet`", tableName);

      run("analyze table dfs.tmp.`%s` columns none REFRESH METADATA", tableName);

      String query = "select mykey from dfs.tmp.`%s` where mykey is null";

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();
      assertEquals("Row count does not match the expected value", 5, actualRowCount);
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(usedMetaPattern, "Filter") // checks that filter wasn't removed since statistics is absent for filtering column
          .exclude()
          .match();
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
      run("drop table if exists dfs.tmp.`%s`", tableName);
    }
  }

  @Test
  public void testSelectAfterAnalyzeWithNonRowGroupLevel() throws Exception {
    String tableName = "parquetAnalyzeWithNonRowGroupLevel";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"), Paths.get(tableName));

    try {
      run("analyze table dfs.`%s` REFRESH METADATA 'file' level", tableName);

      String query = "select * from dfs.`%s`";
      long expectedRowCount = 120;
      int expectedNumFiles = 12;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();

      assertEquals(expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=true";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
    }
  }

  @Test
  public void testAnalyzeWithFallbackError() throws Exception {
    String tableName = "parquetAnalyzeWithFallback";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"), Paths.get(tableName));

    try {
      run("analyze table dfs.`%s` REFRESH METADATA 'file' level", tableName);
      client.alterSession(ExecConstants.METASTORE_FALLBACK_TO_FILE_METADATA, false);

      queryBuilder()
          .sql("select * from dfs.`%s`", tableName)
          .planMatcher()
          .include("usedMetastore=false")
          .match();
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
      client.resetSession(ExecConstants.METASTORE_FALLBACK_TO_FILE_METADATA);
    }
  }

  @Test
  public void testAnalyzeWithSchemaError() throws Exception {
    String tableName = "parquetAnalyzeWithSchemaError";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"), Paths.get(tableName));

    try {
      run("analyze table dfs.`%s` REFRESH METADATA", tableName);
      client.alterSession(ExecConstants.METASTORE_USE_SCHEMA_METADATA, false);

      queryBuilder()
          .sql("select * from dfs.`%s`", tableName)
          .planMatcher()
          .include("usedMetastore=false")
          .match();
    } finally {
      run("analyze table dfs.`%s` drop metadata if exists", tableName);
      client.resetSession(ExecConstants.METASTORE_USE_SCHEMA_METADATA);
    }
  }

  @Test
  public void testAnalyzeWithSchema() throws Exception {
    String tableName = "parquetAnalyzeWithSchema";

    String table = String.format("dfs.tmp.%s", tableName);

    try {
      client.alterSession(ExecConstants.METASTORE_USE_SCHEMA_METADATA, false);
      client.alterSession(ExecConstants.STORE_TABLE_USE_SCHEMA_FILE, true);
      run("create table %s as select 'a' as c from (values(1))", table);
      run("analyze table %s REFRESH METADATA", table);

      run("create schema (o_orderstatus varchar) for table %s", table);

      run("select * from %s", table);
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
      client.resetSession(ExecConstants.METASTORE_USE_SCHEMA_METADATA);
      client.resetSession(ExecConstants.STORE_TABLE_USE_SCHEMA_FILE);
      run("drop table if exists %s", table);
    }
  }

  @Test
  public void testUseStatistics() throws Exception {
    String tableName = "dfs.tmp.employeeUseStat";

    try {
      run("CREATE TABLE %s AS SELECT * from cp.`employee.json`", tableName);

      client.alterSession(PlannerSettings.STATISTICS_USE.getOptionName(), true);

      run("analyze table %s REFRESH METADATA", tableName);

      String query = " select employee_id from %s where department_id = 2";

      String expectedPlan1 = "Filter\\(condition.*\\).*rowcount = 96.25,.*";
      String expectedPlan2 = "Scan.*columns=\\[`department_id`, `employee_id`].*rowcount = 1155.0.*";

      queryBuilder().sql(query, tableName)
          .detailedPlanMatcher()
          .include(expectedPlan1, expectedPlan2)
          .match();
    } finally {
      run("analyze table %s drop metadata if exists", tableName);
      client.resetSession(PlannerSettings.STATISTICS_USE.getOptionName());
      run("drop table if exists %s", tableName);
    }
  }

  @Test
  public void testAnalyzeWithDisabledStatistics() throws Exception {
    String tableName = "dfs.tmp.employeeWithoutStat";

    try {
      run("CREATE TABLE %s AS SELECT * from cp.`employee.json`", tableName);

      client.alterSession(PlannerSettings.STATISTICS_USE.getOptionName(), false);

      run("analyze table %s REFRESH METADATA", tableName);

      String query = "select employee_id from %s where department_id = 2";

      // filter row count is greater since statistics wasn't used
      String expectedPlan1 = "Filter\\(condition.*\\).*rowcount = 173.25,.*";
      String expectedPlan2 = "Scan.*columns=\\[`department_id`, `employee_id`].*rowcount = 1155.0.*";

      queryBuilder().sql(query, tableName)
          .detailedPlanMatcher()
          .include(expectedPlan1, expectedPlan2)
          .match();
    } finally {
      run("analyze table %s drop metadata if exists", tableName);
      client.resetSession(PlannerSettings.STATISTICS_USE.getOptionName());
      run("drop table if exists %s", tableName);
    }
  }

  @Test
  public void testAnalyzeWithoutStatisticsWithStatsFile() throws Exception {
    String tableName = "dfs.tmp.employeeWithStatsFile";

    try {
      run("CREATE TABLE %s AS SELECT * from cp.`employee.json`", tableName);

      client.alterSession(PlannerSettings.STATISTICS_USE.getOptionName(), false);

      run("analyze table %s REFRESH METADATA", tableName);

      client.alterSession(PlannerSettings.STATISTICS_USE.getOptionName(), true);

      run("ANALYZE TABLE %s COMPUTE STATISTICS", tableName);

      String query = "select employee_id from %s where department_id = 2";

      String expectedPlan1 = "Filter\\(condition.*\\).*rowcount = 96.25,.*";
      String expectedPlan2 = "Scan.*columns=\\[`department_id`, `employee_id`].*rowcount = 1155.0.*";

      queryBuilder().sql(query, tableName)
          .detailedPlanMatcher()
          .include(expectedPlan1, expectedPlan2)
          .match();
    } finally {
      run("analyze table %s drop metadata if exists", tableName);
      client.resetSession(PlannerSettings.STATISTICS_USE.getOptionName());
      run("drop table if exists %s", tableName);
    }
  }

  @Test
  public void testAnalyzeWithSampleStatistics() throws Exception {
    String tableName = "dfs.tmp.employeeWithStatsFile";

    try {
      run("CREATE TABLE %s AS SELECT * from cp.`employee.json`", tableName);

      client.alterSession(PlannerSettings.STATISTICS_USE.getOptionName(), true);

      run("ANALYZE TABLE %s COLUMNS(department_id) REFRESH METADATA COMPUTE STATISTICS SAMPLE 95 PERCENT", tableName);

      String query = "select employee_id from %s where department_id = 2";

      String expectedPlan1 = "Filter\\(condition.*\\).*rowcount = 96.25,";
      String expectedPlan2 = "Scan.*columns=\\[`department_id`, `employee_id`].*rowcount = 1155.0";

      queryBuilder().sql(query, tableName)
          .detailedPlanMatcher()
          .include(expectedPlan1, expectedPlan2)
          .match();
    } finally {
      run("analyze table %s drop metadata if exists", tableName);
      client.resetSession(PlannerSettings.STATISTICS_USE.getOptionName());
      run("drop table if exists %s", tableName);
    }
  }

  @Test
  public void testDropMetadata() throws Exception {
    String tableName = "tableDropMetadata";
    TableInfo tableInfo = getTableInfo(tableName, "tmp");
    try {
      run("create table dfs.tmp.`%s` as\n" +
          "select * from cp.`tpch/region.parquet`", tableName);

      run("analyze table dfs.tmp.`%s` REFRESH METADATA", tableName);

      MetastoreTableInfo metastoreTableInfo = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .metastoreTableInfo(tableInfo);

      assertTrue(metastoreTableInfo.isExists());

      BaseTableMetadata baseTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertNotNull(baseTableMetadata);

      List<RowGroupMetadata> rowGroupMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, (String) null, null);

      assertEquals(1, rowGroupMetadata.size());

      testBuilder()
          .sqlQuery("analyze table dfs.tmp.`%s` drop metadata", tableName)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Metadata for table [%s] dropped.", tableName))
          .go();

      metastoreTableInfo = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .metastoreTableInfo(tableInfo);

      assertFalse(metastoreTableInfo.isExists());

      baseTableMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .tableMetadata(tableInfo);

      assertNull(baseTableMetadata);

      rowGroupMetadata = cluster.drillbit().getContext()
          .getMetastoreRegistry()
          .get()
          .tables()
          .basicRequests()
          .rowGroupsMetadata(tableInfo, (String) null, null);

      assertEquals(0, rowGroupMetadata.size());
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);
      run("drop table if exists dfs.tmp.`%s`", tableName);
      client.resetSession(ExecConstants.METASTORE_ENABLED);
    }
  }

  @Test
  public void testDropNonExistingMetadata() throws Exception {
    String tableName = "parquetAnalyzeNonExistingMetadata";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"), Paths.get(tableName));

    testBuilder()
        .sqlQuery("analyze table dfs.`%s` drop metadata if exists", tableName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Metadata for table [%s] does not exist.", tableName))
        .go();

    thrown.expect(UserRemoteException.class);

    run("analyze table dfs.`%s` drop metadata", tableName);
  }

  @Test
  public void testIncorrectAnalyzeCommand() throws Exception {
    String tableName = "parquetAnalyzeNonExistingMetadata";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"), Paths.get(tableName));

    thrown.expect(UserRemoteException.class);
    thrown.expectMessage("PARSE ERROR:");

    run("analyze table dfs.tmp.`%1$s` REFRESH METADATA analyze table dfs.`%1$s` drop metadata", tableName);
  }

  @Test
  public void testIncompleteAnalyzeCommand() throws Exception {
    String tableName = "parquetAnalyzeNonExistingMetadata";

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"), Paths.get(tableName));

    thrown.expect(UserRemoteException.class);
    thrown.expectMessage("PARSE ERROR:");

    run("analyze table dfs.tmp.`%1$s`", tableName);
  }

  @Test
  public void testAnalyzeOnTextTable() throws Exception {
    String tableName = "textTable.csv";

    dirTestWatcher.copyResourceToTestTmp(Paths.get("store/text/data/regions.csv"), Paths.get(tableName));

    thrown.expect(UserRemoteException.class);

    run("analyze table dfs.tmp.`%s` REFRESH METADATA", tableName);
  }

  @Test
  public void testAnalyzeOnView() throws Exception {
    String viewName = "analyzeView";

    run("create view dfs.tmp.`%s` as select * from cp.`tpch/nation.parquet`", viewName);

    thrown.expect(UserRemoteException.class);

    run("analyze table dfs.tmp.`%s` REFRESH METADATA", viewName);
  }

  @Test
  public void testSelectWitOutdatedMetadataWithUpdatedFile() throws Exception {
    String tableName = "outdatedParquetUpdatedFile";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    try {
      run("analyze table dfs.tmp.`%s` REFRESH METADATA", tableName);

      File fileToUpdate = new File(new File(new File(table, "1994"), "Q4"), "orders_94_q4.parquet");
      long lastModified = fileToUpdate.lastModified();
      FileUtils.deleteQuietly(fileToUpdate);

      // replaces original file
      dirTestWatcher.copyResourceToTestTmp(
          Paths.get("multilevel", "parquet", "1994", "Q1", "orders_94_q1.parquet"),
          Paths.get(tableName, "1994", "Q4", "orders_94_q4.parquet"));

      assertTrue(fileToUpdate.setLastModified(lastModified + 1000));

      String query =
          "select dir0, dir1, o_custkey, o_orderdate from dfs.tmp.`%s`\n" +
              "where dir0=1994 and dir1 in ('Q4', 'Q2')";
      long expectedRowCount = 20;
      int expectedNumFiles = 2;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();

      assertEquals(expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=false";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  public void testSelectWitOutdatedMetadataWithNewFile() throws Exception {
    String tableName = "outdatedParquetNewFile";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    try {
      run("analyze table dfs.tmp.`%s` REFRESH METADATA", tableName);

      dirTestWatcher.copyResourceToTestTmp(
          Paths.get("multilevel", "parquet", "1994", "Q1", "orders_94_q1.parquet"),
          Paths.get(tableName, "1994", "Q4", "orders_94_q5.parquet"));

      String query =
          "select dir0, dir1, o_custkey, o_orderdate from dfs.tmp.`%s`\n" +
              "where dir0=1994 and dir1 in ('Q4', 'Q2')";
      long expectedRowCount = 30;
      int expectedNumFiles = 3;

      long actualRowCount = queryBuilder().sql(query, tableName).run().recordCount();

      assertEquals(expectedRowCount, actualRowCount);
      String numFilesPattern = "numFiles=" + expectedNumFiles;
      String usedMetaPattern = "usedMetastore=false";

      queryBuilder().sql(query, tableName)
          .planMatcher()
          .include(numFilesPattern, usedMetaPattern)
          .exclude("Filter")
          .match();
    } finally {
      run("analyze table dfs.tmp.`%s` drop metadata if exists", tableName);

      FileUtils.deleteQuietly(table);
    }
  }

  private static <T> ColumnStatistics<T> getColumnStatistics(T minValue, T maxValue,
      long rowCount, TypeProtos.MinorType minorType) {
    return new ColumnStatistics<>(
        Arrays.asList(
            new StatisticsHolder<>(minValue, ColumnStatisticsKind.MIN_VALUE),
            new StatisticsHolder<>(maxValue, ColumnStatisticsKind.MAX_VALUE),
            new StatisticsHolder<>(rowCount, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(rowCount, ColumnStatisticsKind.NON_NULL_COUNT),
            new StatisticsHolder<>(0L, ColumnStatisticsKind.NULLS_COUNT)),
        minorType);
  }

  private TableInfo getTableInfo(String tableName, String workspace) {
    return TableInfo.builder()
        .name(tableName)
        .owner(cluster.config().getString("user.name"))
        .storagePlugin("dfs")
        .workspace(workspace)
        .type(AnalyzeParquetInfoProvider.TABLE_TYPE_NAME)
        .build();
  }

  private BaseTableMetadata getBaseTableMetadata(TableInfo tableInfo, File table) {
    return BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(new Path(table.toURI().getPath()))
        .columnsStatistics(TABLE_COLUMN_STATISTICS)
        .metadataStatistics(Arrays.asList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(MetadataType.ALL, TableStatisticsKind.ANALYZE_METADATA_LEVEL)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .build();
  }
}
