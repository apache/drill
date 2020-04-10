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
package org.apache.drill.exec.physical.impl.scan.v3.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;

import org.apache.drill.categories.EvfTests;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.EmptyErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ImplicitColumnResolver;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ImplicitColumnResolver.ImplicitColumnOptions;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ImplicitColumnResolver.ParseResult;
import org.apache.drill.exec.physical.impl.scan.v3.schema.MutableTupleSchema;
import org.apache.drill.exec.physical.impl.scan.v3.schema.MutableTupleSchema.ColumnHandle;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ProjectionSchemaTracker;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanProjectionParser.ProjectionParseResult;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanSchemaConfigBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanSchemaTracker;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanSchemaTracker.ProjectionType;
import org.apache.drill.exec.physical.impl.scan.v3.schema.SchemaBasedTracker;
import org.apache.drill.exec.physical.impl.scan.v3.schema.SchemaUtils;
import org.apache.drill.exec.physical.rowSet.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(EvfTests.class)
public class TestImplicitColumnResolver extends SubOperatorTest {

  private static final CustomErrorContext ERROR_CONTEXT = EmptyErrorContext.INSTANCE;

  private static class ParserFixture {

    public final ImplicitColumnOptions options;
    public final ProjectionSchemaTracker tracker;

    public ParserFixture(Collection<SchemaPath> projList) {
      ProjectionParseResult result = ScanProjectionParser.parse(projList);
      tracker = new ProjectionSchemaTracker(result, true, EmptyErrorContext.INSTANCE);
      options = new ImplicitColumnOptions()
          .optionSet(fixture.getOptionManager());
   }

    public ParseResult parseImplicit() {
      ImplicitColumnResolver parser = new ImplicitColumnResolver(options, ERROR_CONTEXT);
      return parser.parse(tracker);
    }
  }

  private boolean isImplicit(List<ColumnHandle> cols, int index) {
    return SchemaUtils.isImplicit(cols.get(index).column());
  }

  @Test
  public void testNoImplicitCols() {
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectList("a", "b", "c"));
    ParseResult result = parseFixture.parseImplicit();
    assertTrue(result.columns().isEmpty());
    assertTrue(result.schema().isEmpty());
  }

  /**
   * Test including file implicit columns in the project list.
   */
  @Test
  public void testFileImplicitColumnSelection() {
    // Simulate SELECT a, fqn, filEPath, filename, suffix ...
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectList("a",
            ScanTestUtils.FULLY_QUALIFIED_NAME_COL,
            "filEPath", // Sic, to test case sensitivity
            ScanTestUtils.FILE_NAME_COL,
            ScanTestUtils.SUFFIX_COL));
    ParseResult result = parseFixture.parseImplicit();

    assertEquals(4, result.columns().size());

    TupleMetadata expected = new SchemaBuilder()
        .add(ScanTestUtils.FULLY_QUALIFIED_NAME_COL, MinorType.VARCHAR)
        .add("filEPath", MinorType.VARCHAR)
        .add(ScanTestUtils.FILE_NAME_COL, MinorType.VARCHAR)
        .add(ScanTestUtils.SUFFIX_COL, MinorType.VARCHAR)
        .build();
    assertEquals(expected, result.schema());

    List<ColumnHandle> cols = parseFixture.tracker.internalSchema().columns();
    assertFalse(isImplicit(cols, 0));
    assertTrue(isImplicit(cols, 1));
    assertTrue(isImplicit(cols, 2));
    assertTrue(isImplicit(cols, 3));
    assertTrue(isImplicit(cols, 4));
  }

  @Test
  public void testPartitionColumnSelection() {

    String dir0 = ScanTestUtils.partitionColName(0);
    // Sic: case insensitivity, but name in project list
    // is preferred over "natural" name.
    String dir1 = "DIR1";
    String dir2 = ScanTestUtils.partitionColName(2);
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectList(dir2, dir1, dir0, "a"));
    ParseResult result = parseFixture.parseImplicit();
    assertEquals(3, result.columns().size());

    TupleMetadata expected = new SchemaBuilder()
        .addNullable(dir2, MinorType.VARCHAR)
        .addNullable(dir1, MinorType.VARCHAR)
        .addNullable(dir0, MinorType.VARCHAR)
        .build();
    assertEquals(expected, result.schema());

    List<ColumnHandle> cols = parseFixture.tracker.internalSchema().columns();
    assertTrue(isImplicit(cols, 0));
    assertTrue(isImplicit(cols, 1));
    assertTrue(isImplicit(cols, 2));
    assertFalse(isImplicit(cols, 3));
  }

  @Test
  public void testLegacyWildcard() {
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectAll());
    parseFixture.options
        .maxPartitionDepth(3)
        .useLegacyWildcardExpansion(true);
    ParseResult result = parseFixture.parseImplicit();
    assertEquals(3, result.columns().size());

    TupleMetadata expected = new SchemaBuilder()
        .addNullable(ScanTestUtils.partitionColName(0), MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(1), MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(2), MinorType.VARCHAR)
        .build();
    assertEquals(expected, result.schema());

    List<ColumnHandle> cols = parseFixture.tracker.internalSchema().columns();
    assertTrue(isImplicit(cols, 0));
    assertTrue(isImplicit(cols, 1));
    assertTrue(isImplicit(cols, 2));
  }

  @Test
  public void testRevisedWildcard() {
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectAll());
    parseFixture.options
        .maxPartitionDepth(3)
        .useLegacyWildcardExpansion(false);
    ParseResult result = parseFixture.parseImplicit();

    assertTrue(result.columns().isEmpty());
    assertTrue(result.schema().isEmpty());
    assertTrue(parseFixture.tracker.internalSchema().columns().isEmpty());
  }

  /**
   * Combine wildcard and file metadata columns. The wildcard expands
   * table columns but not metadata columns.
   */
  @Test
  public void testLegacyWildcardAndImplictCols() {
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectList(
            SchemaPath.DYNAMIC_STAR,
            ScanTestUtils.FILE_NAME_COL,
            ScanTestUtils.SUFFIX_COL));
    parseFixture.options
        .maxPartitionDepth(2)
        .useLegacyWildcardExpansion(true);
    ParseResult result = parseFixture.parseImplicit();

    TupleMetadata expected = new SchemaBuilder()
        .addNullable(ScanTestUtils.partitionColName(0), MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(1), MinorType.VARCHAR)
        .add(ScanTestUtils.FILE_NAME_COL, MinorType.VARCHAR)
        .add(ScanTestUtils.SUFFIX_COL, MinorType.VARCHAR)
        .build();
    assertEquals(expected, result.schema());
  }

  /**
   * As above, but include implicit columns before and after the wildcard.
   * Include both a wildcard and a partition column. The wildcard, in legacy
   * mode, will create partition columns for any partitions not mentioned in the
   * project list.
   */
  @Test
  public void testLegacyWildcardAndImplicitColsMixed() {
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectList(
            ScanTestUtils.FILE_NAME_COL,
            SchemaPath.DYNAMIC_STAR,
            ScanTestUtils.SUFFIX_COL,
            ScanTestUtils.partitionColName(0)));
    parseFixture.options
        .maxPartitionDepth(3)
        .useLegacyWildcardExpansion(true);
    ParseResult result = parseFixture.parseImplicit();

    TupleMetadata expected = new SchemaBuilder()
        .add(ScanTestUtils.FILE_NAME_COL, MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(1), MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(2), MinorType.VARCHAR)
        .add(ScanTestUtils.SUFFIX_COL, MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(0), MinorType.VARCHAR)
        .build();
    assertEquals(expected, result.schema());
  }

  /**
   * Verify that names that look like metadata columns, but appear
   * to be maps or arrays, are not interpreted as metadata. That is,
   * the projected table map or array "shadows" the metadata column.
   */
  @Test
  public void testShadowed() {
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectList(
            ScanTestUtils.FILE_NAME_COL + ".a",
            ScanTestUtils.FILE_PATH_COL + "[0]",
            ScanTestUtils.partitionColName(0) + ".b",
            ScanTestUtils.partitionColName(1) + "[0]",
            ScanTestUtils.SUFFIX_COL));
    ParseResult result = parseFixture.parseImplicit();

    TupleMetadata expected = new SchemaBuilder()
        .add(ScanTestUtils.SUFFIX_COL, MinorType.VARCHAR)
        .build();
    assertEquals(expected, result.schema());

    List<ColumnHandle> cols = parseFixture.tracker.internalSchema().columns();
    assertFalse(isImplicit(cols, 0));
    assertFalse(isImplicit(cols, 1));
    assertFalse(isImplicit(cols, 2));
    assertFalse(isImplicit(cols, 3));
    assertTrue(isImplicit(cols, 4));
  }

  @Test
  public void testProvidedImplicitCols() {

    TupleMetadata providedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("myFqn", MinorType.VARCHAR)
        // Sic, to test case nullable
        .addNullable("myFilePath", MinorType.VARCHAR)
        .add("myFileName", MinorType.VARCHAR)
        .add("mySuffix", MinorType.VARCHAR)
        .addNullable("myDir", MinorType.VARCHAR)
        .build();
    SchemaUtils.markImplicit(providedSchema.metadata("myFqn"), ColumnMetadata.IMPLICIT_FQN);
    // Sic, to test case sensitivity
    SchemaUtils.markImplicit(providedSchema.metadata("myFilePath"), ColumnMetadata.IMPLICIT_FILEPATH.toUpperCase());
    SchemaUtils.markImplicit(providedSchema.metadata("myFileName"), ColumnMetadata.IMPLICIT_FILENAME);
    SchemaUtils.markImplicit(providedSchema.metadata("mySuffix"), ColumnMetadata.IMPLICIT_SUFFIX);
    SchemaUtils.markAsPartition(providedSchema.metadata("myDir"), 0);

    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectAll());
    parseFixture.tracker.applyProvidedSchema(providedSchema);
    parseFixture.options
        .maxPartitionDepth(1);
    ParseResult result = parseFixture.parseImplicit();

    assertEquals(5, result.columns().size());

    TupleMetadata expected = new SchemaBuilder()
        .add("myFqn", MinorType.VARCHAR)
        .addNullable("myFilePath", MinorType.VARCHAR)
        .add("myFileName", MinorType.VARCHAR)
        .add("mySuffix", MinorType.VARCHAR)
        .addNullable("myDir", MinorType.VARCHAR)
        .build();
    assertEquals(expected, result.schema());

    List<ColumnHandle> cols = parseFixture.tracker.internalSchema().columns();
    assertFalse(isImplicit(cols, 0));
    assertTrue(isImplicit(cols, 1));
    assertTrue(isImplicit(cols, 2));
    assertTrue(isImplicit(cols, 3));
    assertTrue(isImplicit(cols, 4));
    assertTrue(isImplicit(cols, 5));
  }


  @Test
  public void testProvidedImplicitMatchesProject() {

    TupleMetadata providedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("fqn", MinorType.VARCHAR)
        // Sic, to test case sensitivity
        .add("filePath", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .build();
    SchemaUtils.markImplicit(providedSchema.metadata("fqn"), ColumnMetadata.IMPLICIT_FQN);
    SchemaUtils.markImplicit(providedSchema.metadata("filePath"), ColumnMetadata.IMPLICIT_FILEPATH.toUpperCase());
    SchemaUtils.markAsPartition(providedSchema.metadata("dir0"), 0);

    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectAll());
    parseFixture.tracker.applyProvidedSchema(providedSchema);
    parseFixture.options.maxPartitionDepth(1);
    ParseResult result = parseFixture.parseImplicit();

    assertEquals(3, result.columns().size());

    TupleMetadata expected = new SchemaBuilder()
        .add("fqn", MinorType.VARCHAR)
        .add("filePath", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .build();
    assertEquals(expected, result.schema());

    List<ColumnHandle> cols = parseFixture.tracker.internalSchema().columns();
    assertFalse(isImplicit(cols, 0));
    assertTrue(isImplicit(cols, 1));
    assertTrue(isImplicit(cols, 2));
    assertTrue(isImplicit(cols, 3));
  }

  @Test
  public void testProvidedImplicitColTypeConflict() {
    TupleMetadata providedSchema = new SchemaBuilder()
        .add("myFqn", MinorType.INT)
        .build();
    SchemaUtils.markImplicit(providedSchema.metadata("myFqn"), ColumnMetadata.IMPLICIT_FQN);

    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectAll());
    parseFixture.tracker.applyProvidedSchema(providedSchema);
    try {
      parseFixture.parseImplicit();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("wrong type"));
    }
  }

  @Test
  public void testProvidedImplicitColModeConflict() {
    TupleMetadata providedSchema = new SchemaBuilder()
        .addArray("myFqn", MinorType.VARCHAR)
        .build();
    SchemaUtils.markImplicit(providedSchema.metadata("myFqn"), ColumnMetadata.IMPLICIT_FQN);

    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectAll());
    parseFixture.tracker.applyProvidedSchema(providedSchema);
    try {
      parseFixture.parseImplicit();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("wrong type"));
    }
  }

  @Test
  public void testProvidedPartitionColTypeConflict() {
    TupleMetadata providedSchema = new SchemaBuilder()
        .addNullable("myDir", MinorType.INT)
        .build();
    SchemaUtils.markAsPartition(providedSchema.metadata("myDir"), 0);

    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectAll());
    parseFixture.tracker.applyProvidedSchema(providedSchema);
    parseFixture.options.maxPartitionDepth(1);
    try {
      parseFixture.parseImplicit();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("wrong type"));
    }
  }

  @Test
  public void testProvidedPartitionColModeConflict() {
    TupleMetadata providedSchema = new SchemaBuilder()
        .add("myDir", MinorType.VARCHAR)
        .build();
    SchemaUtils.markAsPartition(providedSchema.metadata("myDir"), 0);

    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectAll());
    parseFixture.tracker.applyProvidedSchema(providedSchema);
    parseFixture.options.maxPartitionDepth(1);
    try {
      parseFixture.parseImplicit();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("wrong type"));
    }
  }

  @Test
  public void testProvidedUndefinedImplicitCol() {
    TupleMetadata providedSchema = new SchemaBuilder()
        .add("myDir", MinorType.VARCHAR)
        .build();
    SchemaUtils.markImplicit(providedSchema.metadata("myDir"), "bogus");

    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectAll());
    parseFixture.tracker.applyProvidedSchema(providedSchema);
    parseFixture.options.maxPartitionDepth(1);
    try {
      parseFixture.parseImplicit();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("references an undefined implicit column"));
    }
  }

  @Test
  public void testImplicitOnly() {
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectList(
            ScanTestUtils.FULLY_QUALIFIED_NAME_COL,
            ScanTestUtils.FILE_NAME_COL));
    ParseResult result = parseFixture.parseImplicit();

    assertEquals(2, result.columns().size());
    assertTrue(parseFixture.tracker.isResolved());
    assertSame(ProjectionType.NONE, parseFixture.tracker.projectionType());
  }

  @Test
  public void testImplicitOnlyWildcard() {
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectList(
            ScanTestUtils.FULLY_QUALIFIED_NAME_COL,
            SchemaPath.DYNAMIC_STAR,
            ScanTestUtils.FILE_NAME_COL));
    ParseResult result = parseFixture.parseImplicit();

    assertEquals(2, result.columns().size());
    assertTrue(parseFixture.tracker.isResolved());
    assertSame(ProjectionType.ALL, parseFixture.tracker.projectionType());
  }

  /**
   * The scan framework should expand partitions after table columns.
   */
  @Test
  public void testPartitionExpansionPlacement() {
    // Parse out implicit columns at start of scan
    ParserFixture parseFixture = new ParserFixture(
        RowSetTestUtils.projectAll());
    parseFixture.options
        .maxPartitionDepth(2)
        .useLegacyWildcardExpansion(true);
    ParseResult result = parseFixture.parseImplicit();

    // Later resolve the table schema
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("f", MinorType.BIGINT)
        .build();
    parseFixture.tracker.applyReaderSchema(tableSchema, ERROR_CONTEXT);

    TupleMetadata expected = new SchemaBuilder()
        .addNullable(ScanTestUtils.partitionColName(0), MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(1), MinorType.VARCHAR)
        .build();
    assertEquals(expected, result.schema());

    // Implicit columns follow the table columns
    expected = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("f", MinorType.BIGINT)
        .addNullable(ScanTestUtils.partitionColName(0), MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(1), MinorType.VARCHAR)
        .build();
    assertEquals(expected, parseFixture.tracker.outputSchema());
  }

  @Test
  public void testImplicitWithDefinedSchema() {
    final ScanSchemaConfigBuilder builder = new ScanSchemaConfigBuilder()
        .projection(RowSetTestUtils.projectList(
            "a", "b", "c",
            ScanTestUtils.FILE_NAME_COL,
            ScanTestUtils.FILE_PATH_COL,
            ScanTestUtils.partitionColName(0),
            ScanTestUtils.partitionColName(2)));

    final TupleMetadata definedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.BIGINT)
        .add("c", MinorType.VARCHAR)
        .add(ScanTestUtils.FILE_NAME_COL, MinorType.VARCHAR)
        .add(ScanTestUtils.FILE_PATH_COL, MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(0), MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(2), MinorType.VARCHAR)
        .buildSchema();

    // With a defined schema, we have to explicitly mark implicit columns
    // so the schema is independent of system/session options.
    SchemaUtils.markImplicit(definedSchema.metadata(ScanTestUtils.FILE_NAME_COL), ColumnMetadata.IMPLICIT_FILENAME);
    SchemaUtils.markImplicit(definedSchema.metadata(ScanTestUtils.FILE_PATH_COL), ScanTestUtils.FILE_PATH_COL);
    SchemaUtils.markAsPartition(definedSchema.metadata(ScanTestUtils.partitionColName(0)), 0);
    SchemaUtils.markAsPartition(definedSchema.metadata(ScanTestUtils.partitionColName(2)), 2);
    builder.definedSchema(definedSchema);

    final ScanSchemaTracker schemaTracker = builder.build();
    assertTrue(schemaTracker instanceof SchemaBasedTracker);
    assertTrue(schemaTracker.isResolved());
    assertSame(ProjectionType.SOME, schemaTracker.projectionType());

    // Implicit columns should be marked
    MutableTupleSchema internalSchema = schemaTracker.internalSchema();
    assertFalse(internalSchema.find("a").isImplicit());
    assertTrue(internalSchema.find(ScanTestUtils.FILE_NAME_COL).isImplicit());
    assertTrue(internalSchema.find(ScanTestUtils.partitionColName(2)).isImplicit());

    ImplicitColumnOptions options = new ImplicitColumnOptions()
        .optionSet(fixture.getOptionManager());
    ImplicitColumnResolver parser = new ImplicitColumnResolver(options, ERROR_CONTEXT);
    ParseResult result = parser.parse(schemaTracker);
    assertEquals(4, result.columns().size());

    TupleMetadata readerInputSchema = schemaTracker.readerInputSchema();
    assertEquals(3, readerInputSchema.size());

    assertEquals(definedSchema, schemaTracker.outputSchema());
  }
}
