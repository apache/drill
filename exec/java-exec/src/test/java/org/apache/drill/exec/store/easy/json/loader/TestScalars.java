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
package org.apache.drill.exec.store.easy.json.loader;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.project.Projections;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

/**
 * Tests JSON scalar handling. Without a schema, the first non-null value
 * determines the type of the column. With a schema, then the schema determines
 * the column type independent of the first data row.
 * <p>
 * In either case, all scalars perform conversion from all other scalars. If data
 * is clean, then the conversion will never be used. If the data is messy, then
 * the result, when combined with a schema, produces a repeatable (if perhaps still
 * messy) result. The goal is that, with a schema, the query should not fail due
 * to a few messy rows a billion rows in, or due to the order that the scanners
 * see the data.
 */
public class TestScalars extends BaseJsonLoaderTest {

  /**
   * Test Boolean type using type inference to guess the type from the
   * first row of data. All other types can be converted to Boolean.
   */
  @Test
  public void testBoolean() {
    String json =
        "{a: true} {a: false} {a: null} " +
        "{a: 1} {a: 0} " +
        "{a: 1.0} {a: 0.0} " +
        "{a: \"true\"} {a: \"\"} {a: \"false\"} {a: \"other\"}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIT)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(true)   // true
        .addRow(false)  // false
        .addRow((Boolean) null)   // null
        .addRow(true)   // 1
        .addRow(false)  // 0
        .addRow(true)   // 1.0
        .addRow(false)  // 0.0
        .addRow(true)   // "true"
        .addRow((Boolean) null)  // ""
        .addRow(false)  // "false"
        .addRow(false)  // "other"
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testAllTextBoolean() {
    String json =
        "{a: true} {a: false} {a: null}";

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.allTextMode = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("true")   // true
        .addRow("false")  // false
        .addRow((String) null)    // null
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  /**
   * Test Boolean with a provided schema which states the column type
   * independent of the first value. Test with leading values which are
   * not Boolean.
   */
  @Test
  public void testBooleanWithSchema() {
    String json =
        "{a: 1} {a: 0} " +
        "{a: 1.0} {a: 0.0} " +
        "{a: \"true\"} {a: \"\"} {a: \"false\"} {a: \"other\"}" +
        "{a: true} {a: false} {a: null}";
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("a", MinorType.BIT)
        .build();

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.providedSchema = schema;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    RowSet expected = fixture.rowSetBuilder(schema)
        .addRow(true)   // 1
        .addRow(false)  // 0
        .addRow(true)   // 1.0
        .addRow(false)  // 0.0
        .addRow(true)   // "true"
        .addRow((Boolean) null)  // ""
        .addRow(false)  // "false"
        .addRow(false)  // "other"
        .addRow(true)   // true
        .addRow(false)  // false
        .addRow((Boolean) null)   // null
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testInt() {
    String json =
        "{a: 1} {a: 0} {a: -300} {a: null} " +
        "{a: true} {a: false} " +
        "{a: 1.0} {a: 1.4} {a: 1.5} {a: 0.0} " +
        "{a: \"\"} {a: \"3\"}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1)      // 1
        .addRow(0)      // 0
        .addRow(-300)   // -300
        .addRow((Long) null)   // null
        .addRow(1)      // true
        .addRow(0)      // false
        .addRow(1)      // 1.0
        .addRow(1)      // 1.4
        .addRow(2)      // 1.5
        .addRow(0)      // 0.0
        .addRow((Long) null)   // ""
        .addRow(3)      // "3"
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testIntAsDouble() {
    String json =
        "{a: 1} {a: 0} {a: -300} {a: null} " +
        "{a: true} {a: false} " +
        "{a: 1.0} {a: 1.5} {a: 0.0} " +
        "{a: \"\"} {a: \"3\"}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.readNumbersAsDouble = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1D)      // 1
        .addRow(0D)      // 0
        .addRow(-300D)   // -300
        .addRow((Double) null)   // null
        .addRow(1D)      // true
        .addRow(0D)      // false
        .addRow(1D)      // 1.0
        .addRow(1.5D)    // 1.5
        .addRow(0D)      // 0.0
        .addRow((Double) null)   // ""
        .addRow(3D)      // "3"
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testAllTextInt() {
    String json =
        "{a: 1} {a: 0} {a: -300} {a: null}";

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.allTextMode = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("1")    // 1
        .addRow("0")    // 0
        .addRow("-300") // -300
        .addRow((String) null)    // null
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testIntWithSchema() {
    String json =
        "{a: true} {a: false} " +
        "{a: 1.0} {a: 1.4} {a: 1.5} {a: 0.0} " +
        "{a: \"\"} {a: \"3\"} " +
        "{a: 1} {a: 0} {a: -300} {a: null}";
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.providedSchema = schema;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    RowSet expected = fixture.rowSetBuilder(schema)
        .addRow(1)      // true
        .addRow(0)      // false
        .addRow(1)      // 1.0
        .addRow(1)      // 1.4
        .addRow(2)      // 1.5
        .addRow(0)      // 0.0
        .addRow((Long) null)   // ""
        .addRow(3)      // "3"
        .addRow(1)      // 1
        .addRow(0)      // 0
        .addRow(-300)   // -300
        .addRow((Long) null)   // null
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  /**
   * There are limits on Drill's generosity. If no conversion exists
   * to int, the query will fail with a descriptive error.
   */
  @Test
  public void testIntWithError() {
    String json =
        "{a: 1}\n{a: \"abc\"}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    try {
      loader.next();
      fail();
    } catch (UserException e) {

      // Robust check of error contents. Only need test once, error
      // code is generic
      String msg = e.getMessage();
      assertTrue(msg.contains("not compatible"));
      assertTrue(msg.contains("Column: a"));
      assertTrue(msg.contains("Column type: BIGINT"));
      assertTrue(msg.contains("JSON token type: string"));
      assertTrue(msg.contains("JSON token: abc"));
      assertTrue(msg.contains("Line: 2"));
    } finally {
      loader.close();
    }
  }

  @Test
  public void testFloat() {
    String json =
        "{a: 0.0} {a: 1.0} {a: 1.25} {a: -123.125} {a: null} " +
        "{a: -Infinity} {a: NaN} {a: Infinity} " +
        "{a: 0} {a: 12} " +
        "{a: true} {a: false} " +
        "{a: \"\"} {a: \"3.75\"}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.allowNanInf = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(0.0)      // 0.0
        .addRow(1.0)      // 1.0
        .addRow(1.25)     // 1.25
        .addRow(-123.125) // -123.125
        .addRow((Double) null)   // null
        .addRow(Double.NEGATIVE_INFINITY) // -Inf
        .addRow(Double.NaN) // Nan
        .addRow(Double.POSITIVE_INFINITY) // Inf
        .addRow(0.0)      // 0
        .addRow(12.0)     // 12
        .addRow(1.0)      // true
        .addRow(0.0)      // false
        .addRow((Double) null)   // ""
        .addRow(3.75)     // "3.75"
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testAllTextFloat() {
    String json =
        "{a: 0.0} {a: 1.0} {a: 1.25} {a: -123.125} {a: null} " +
        "{a: -Infinity} {a: NaN} {a: Infinity}";

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.allTextMode = true;
    loader.jsonOptions.allowNanInf = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("0.0")
        .addRow("1.0")
        .addRow("1.25")
        .addRow("-123.125")
        .addRow((String) null)
        .addRow("-Infinity")
        .addRow("NaN")
        .addRow("Infinity")
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testFloatWithSchema() {
    String json =
        "{a: 0} {a: 12} " +
        "{a: true} {a: false} " +
        "{a: \"\"} {a: \"3.75\"} " +
        "{a: 0.0} {a: 1.0} {a: 1.25} {a: -123.125} {a: null} " +
        "{a: -Infinity} {a: NaN} {a: Infinity}";
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .build();

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.providedSchema = schema;
    loader.jsonOptions.allowNanInf = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    RowSet expected = fixture.rowSetBuilder(schema)
        .addRow(0.0)      // 0
        .addRow(12.0)     // 12
        .addRow(1.0)      // true
        .addRow(0.0)      // false
        .addRow((Double) null)   // ""
        .addRow(3.75)     // "3.75"
        .addRow(0.0)      // 0.0
        .addRow(1.0)      // 1.0
        .addRow(1.25)     // 1.25
        .addRow(-123.125) // -123.125
        .addRow((Double) null)   // null
        .addRow(Double.NEGATIVE_INFINITY) // -Inf
        .addRow(Double.NaN) // Nan
        .addRow(Double.POSITIVE_INFINITY) // Inf
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testFloatWithError() {
    String json =
        "{a: 1.25}\n{a: \"abc\"}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    try {
      loader.next();
      fail();
    } catch (UserException e) {
      String msg = e.getMessage();
      assertTrue(msg.contains("not compatible"));
      assertTrue(msg.contains("Column type: DOUBLE"));
    } finally {
      loader.close();
    }
  }

  @Test
  public void testString() {
    String json =
        "{a: \"\"} {a: \"foo\"} {a: \" bar \"} {a: null} " +
        "{a: 0} {a: 12} " +
        "{a: true} {a: false} " +
        "{a: 0.0} {a: 1.25}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("")       // ""
        .addRow("foo")    // "foo"
        .addRow(" bar ")  // " bar "
        .addRow((String) null) // null
        .addRow("0")      // 0
        .addRow("12")     // 12
        .addRow("true")   // true
        .addRow("false")  // false
        .addRow("0.0")    // 0.0
        .addRow("1.25")   // 1.25
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testAllTextString() {
    String json =
        "{a: \"\"} {a: \"foo\"} {a: \" bar \"} {a: null}";

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.allTextMode = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("")       // ""
        .addRow("foo")    // "foo"
        .addRow(" bar ")  // " bar "
        .addRow((String) null) // null
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testStringWithSchema() {
    String json =
        "{a: 0} {a: 12} " +
        "{a: true} {a: false} " +
        "{a: 0.0} {a: 1.25} " +
        "{a: \"\"} {a: \"foo\"} {a: \" bar \"} {a: null}";
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.providedSchema = schema;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    RowSet expected = fixture.rowSetBuilder(schema)
        .addRow("0")      // 0
        .addRow("12")     // 12
        .addRow("true")   // true
        .addRow("false")  // false
        .addRow("0.0")    // 0.0
        .addRow("1.25")   // 1.25
        .addRow("")       // ""
        .addRow("foo")    // "foo"
        .addRow(" bar ")  // " bar "
        .addRow((String) null) // null
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testProjection() {
    String json =
        "{a: 10, b: true}\n" +
        "{a: 20, b: [\"what?\"]}\n" +
        "{a: 30, b: {c: \"oh, my!\"}}";

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.rsLoaderOptions.projection(
        Projections.parse(RowSetTestUtils.projectList("a")));
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(10)
        .addRow(20)
        .addRow(30)
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }
}
