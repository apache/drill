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

package org.apache.drill.exec.store.daffodil.schema;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.junit.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDaffodilToDrillMetadataConversion {

  @Test
  public void testSimple() throws Exception {
    URI schemaURI = getClass().getResource("/schema/simple.dfdl.xsd").toURI();
    TupleMetadata schema = DrillDaffodilSchemaUtils.processSchema(schemaURI, "row", null);

    TupleMetadata expectedSchema  = new SchemaBuilder()
        .add("col", MinorType.INT)
      .buildSchema();
    assertTrue(expectedSchema.isEquivalent(schema));
  }

  @Test
  public void testComplex1() throws Exception {
    URI schemaURI = getClass().getResource("/schema/complex1.dfdl.xsd").toURI();
    TupleMetadata schema = DrillDaffodilSchemaUtils.processSchema(schemaURI, "row", null);

    TupleMetadata expectedSchema  = new SchemaBuilder()
        .add("a1", MinorType.INT)
        .add("a2", MinorType.INT)
        .buildSchema();
    assertTrue(expectedSchema.isEquivalent(schema));
  }

  @Test
  public void testComplex2() throws Exception {
    URI schemaURI = getClass().getResource("/schema/complex2.dfdl.xsd").toURI();
    TupleMetadata schema = DrillDaffodilSchemaUtils.processSchema(schemaURI, "row", null);

    TupleMetadata expectedSchema  = new SchemaBuilder()
        .add("a1", MinorType.INT)
        .add("a2", MinorType.INT)
        .addMap("b")
        .add("b1", MinorType.INT)
        .add("b2", MinorType.INT)
        .resumeSchema()
        .buildSchema();
    assertTrue(expectedSchema.isEquivalent(schema));
  }

}
