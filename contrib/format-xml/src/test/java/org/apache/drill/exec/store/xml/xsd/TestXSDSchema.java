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

package org.apache.drill.exec.store.xml.xsd;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.junit.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestXSDSchema {

  @Test
  public void testSimpleXSD() throws Exception {
    File simple_xsd = DrillFileUtils.getResourceAsFile("/xsd/simple.xsd");
    TupleMetadata schema = DrillXSDSchemaUtils.getSchema(simple_xsd.getPath());

    TupleMetadata expectedSchema  = new SchemaBuilder()
        .addMap("shiporder")
          .addMap("attributes")
            .addNullable("orderid", MinorType.VARCHAR)
          .resumeMap()
          .addNullable("orderperson", MinorType.VARCHAR)
          .addMap("shipto")
            .addNullable("name", MinorType.VARCHAR)
            .addNullable("address", MinorType.VARCHAR)
            .addNullable("city", MinorType.VARCHAR)
            .addNullable("country", MinorType.VARCHAR)
        .resumeMap()
          .addMapArray("item")
            .addNullable("title", MinorType.VARCHAR)
            .addNullable("note", MinorType.VARCHAR)
            .addNullable("quantity", MinorType.VARDECIMAL)
            .addNullable("price", MinorType.VARDECIMAL)
          .resumeMap()
        .resumeSchema()
      .buildSchema();
    assertTrue(expectedSchema.isEquivalent(schema));
  }


  @Test
  public void testComplexXSD() throws Exception {
    File complex_xsd = DrillFileUtils.getResourceAsFile("/xsd/complex.xsd");
    TupleMetadata schema = DrillXSDSchemaUtils.getSchema(complex_xsd.getPath());

    SchemaBuilder sb1 = new SchemaBuilder();
    MapBuilder sb2 = sb1
        .addNullable("comment", MinorType.VARCHAR) // global comment element
        .addMap("infoType")
          .addMap("attributes")
            .addNullable("kind", MinorType.VARCHAR)
          .resumeMap()
        .resumeSchema()
        .addMap("purchaseOrder") // global purchaseOrder element
          .addMap("attributes")
            .addNullable("orderDate", MinorType.DATE) // an attribute
            .addNullable("confirmDate", MinorType.DATE) // an attribute
          .resumeMap()
          .addMap("shipTo")
            .addMap("attributes")
              .addNullable("country", MinorType.VARCHAR) // an attribute
            .resumeMap()
            .addNullable("name", MinorType.VARCHAR)
            .addNullable("street", MinorType.VARCHAR)
            .addNullable("city", MinorType.VARCHAR)
            .addNullable("state", MinorType.VARCHAR)
            .addNullable("zip", MinorType.VARDECIMAL)
          .resumeMap(); // end shipTo
    MapBuilder sb3 = sb2
          .addMap("billTo")
            .addMap("attributes")
              .addNullable("country", MinorType.VARCHAR) // an attribute
            .resumeMap()
            .addNullable("name", MinorType.VARCHAR)
            .addNullable("street", MinorType.VARCHAR)
             .addNullable("city", MinorType.VARCHAR)
            .addNullable("state", MinorType.VARCHAR)
            .addNullable("zip", MinorType.VARDECIMAL)
          .resumeMap();
    MapBuilder sb4 = sb3
          .addNullable("comment", MinorType.VARCHAR)
          .addMap("items")
            .addMapArray("item")
              .addMap("attributes")
                .addNullable("partNum", MinorType.VARCHAR) // an attribute
             .resumeMap()
              .addNullable("productName", MinorType.VARCHAR)
              .addNullable("quantity", MinorType.VARDECIMAL)
              .addNullable("USPrice", MinorType.VARDECIMAL)
              .addNullable("comment", MinorType.VARCHAR)
              .addNullable("shipDate", MinorType.DATE)
            .resumeMap() // end item
          .resumeMap(); // end items

    TupleMetadata expectedSchema = sb4.resumeSchema().build();
    assertTrue(expectedSchema.isEquivalent(schema));
  }
}
