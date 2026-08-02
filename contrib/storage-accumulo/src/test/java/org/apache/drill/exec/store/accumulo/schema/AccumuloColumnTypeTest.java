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
package org.apache.drill.exec.store.accumulo.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

/**
 * Unit tests for AccumuloColumnType.
 */
public class AccumuloColumnTypeTest extends BaseTest {

  @Test
  public void testSqlTypeMapping() {
    assertEquals(SqlTypeName.VARCHAR, AccumuloColumnType.VARCHAR.getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, AccumuloColumnType.INT.getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, AccumuloColumnType.INTEGER.getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, AccumuloColumnType.BIGINT.getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, AccumuloColumnType.LONG.getSqlTypeName());
    assertEquals(SqlTypeName.FLOAT, AccumuloColumnType.FLOAT.getSqlTypeName());
    assertEquals(SqlTypeName.DOUBLE, AccumuloColumnType.DOUBLE.getSqlTypeName());
    assertEquals(SqlTypeName.BOOLEAN, AccumuloColumnType.BOOLEAN.getSqlTypeName());
    assertEquals(SqlTypeName.DATE, AccumuloColumnType.DATE.getSqlTypeName());
    assertEquals(SqlTypeName.TIME, AccumuloColumnType.TIME.getSqlTypeName());
    assertEquals(SqlTypeName.TIMESTAMP, AccumuloColumnType.TIMESTAMP.getSqlTypeName());
    assertEquals(SqlTypeName.VARBINARY, AccumuloColumnType.VARBINARY.getSqlTypeName());
    assertEquals(SqlTypeName.ANY, AccumuloColumnType.ANY.getSqlTypeName());
  }

  @Test
  public void testFromString() {
    // Direct matches
    assertEquals(AccumuloColumnType.VARCHAR, AccumuloColumnType.fromString("VARCHAR"));
    assertEquals(AccumuloColumnType.INTEGER, AccumuloColumnType.fromString("INTEGER"));
    assertEquals(AccumuloColumnType.BIGINT, AccumuloColumnType.fromString("BIGINT"));
    assertEquals(AccumuloColumnType.DOUBLE, AccumuloColumnType.fromString("DOUBLE"));
    assertEquals(AccumuloColumnType.BOOLEAN, AccumuloColumnType.fromString("BOOLEAN"));

    // Case insensitive
    assertEquals(AccumuloColumnType.VARCHAR, AccumuloColumnType.fromString("varchar"));
    assertEquals(AccumuloColumnType.INTEGER, AccumuloColumnType.fromString("integer"));
    assertEquals(AccumuloColumnType.BOOLEAN, AccumuloColumnType.fromString("Boolean"));
  }

  @Test
  public void testFromStringAliases() {
    // String aliases
    assertEquals(AccumuloColumnType.VARCHAR, AccumuloColumnType.fromString("STRING"));
    assertEquals(AccumuloColumnType.VARCHAR, AccumuloColumnType.fromString("TEXT"));

    // Integer aliases
    assertEquals(AccumuloColumnType.INTEGER, AccumuloColumnType.fromString("INT"));

    // Long aliases
    assertEquals(AccumuloColumnType.BIGINT, AccumuloColumnType.fromString("LONG"));

    // Boolean aliases
    assertEquals(AccumuloColumnType.BOOLEAN, AccumuloColumnType.fromString("BOOL"));

    // Binary aliases
    assertEquals(AccumuloColumnType.VARBINARY, AccumuloColumnType.fromString("BYTES"));
    assertEquals(AccumuloColumnType.VARBINARY, AccumuloColumnType.fromString("BINARY"));
  }

  @Test
  public void testFromStringDefault() {
    // Unknown types should default to VARCHAR
    assertEquals(AccumuloColumnType.VARCHAR, AccumuloColumnType.fromString("UNKNOWN"));
    assertEquals(AccumuloColumnType.VARCHAR, AccumuloColumnType.fromString(""));
    assertEquals(AccumuloColumnType.VARCHAR, AccumuloColumnType.fromString(null));
    assertEquals(AccumuloColumnType.VARCHAR, AccumuloColumnType.fromString("  "));
  }

  @Test
  public void testIsNumeric() {
    assertTrue(AccumuloColumnType.INT.isNumeric());
    assertTrue(AccumuloColumnType.INTEGER.isNumeric());
    assertTrue(AccumuloColumnType.BIGINT.isNumeric());
    assertTrue(AccumuloColumnType.LONG.isNumeric());
    assertTrue(AccumuloColumnType.SMALLINT.isNumeric());
    assertTrue(AccumuloColumnType.TINYINT.isNumeric());
    assertTrue(AccumuloColumnType.FLOAT.isNumeric());
    assertTrue(AccumuloColumnType.DOUBLE.isNumeric());
    assertTrue(AccumuloColumnType.DECIMAL.isNumeric());

    assertFalse(AccumuloColumnType.VARCHAR.isNumeric());
    assertFalse(AccumuloColumnType.BOOLEAN.isNumeric());
    assertFalse(AccumuloColumnType.DATE.isNumeric());
    assertFalse(AccumuloColumnType.VARBINARY.isNumeric());
  }

  @Test
  public void testIsTemporal() {
    assertTrue(AccumuloColumnType.DATE.isTemporal());
    assertTrue(AccumuloColumnType.TIME.isTemporal());
    assertTrue(AccumuloColumnType.TIMESTAMP.isTemporal());

    assertFalse(AccumuloColumnType.VARCHAR.isTemporal());
    assertFalse(AccumuloColumnType.INT.isTemporal());
    assertFalse(AccumuloColumnType.BOOLEAN.isTemporal());
    assertFalse(AccumuloColumnType.VARBINARY.isTemporal());
  }
}
