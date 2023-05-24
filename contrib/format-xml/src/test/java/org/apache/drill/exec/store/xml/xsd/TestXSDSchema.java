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
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.junit.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestXSDSchema {

  @Test
  public void testSimpleXSD() throws Exception {
    File simple_xsd = DrillFileUtils.getResourceAsFile("/xsd/simple.xsd");
    TupleMetadata schema = XSDSchemaUtils.getSchema(simple_xsd.getPath());

    TupleMetadata expectedSchema  = new SchemaBuilder()
        .addMap("shiporder")
          .addNullable("orderperson", MinorType.VARCHAR)
          .addMap("shipto")
            .addNullable("name", MinorType.VARCHAR)
            .addNullable("address", MinorType.VARCHAR)
            .addNullable("city", MinorType.VARCHAR)
            .addNullable("country", MinorType.VARCHAR)
          .resumeMap()
          .addMap("item")
            .addNullable("title", MinorType.VARCHAR)
            .addNullable("note", MinorType.VARCHAR)
            .addNullable("quantity", MinorType.INT)
            .addNullable("price", MinorType.VARDECIMAL)
          .resumeMap()
        .resumeSchema().buildSchema();
    System.out.println(expectedSchema);
    System.out.println("ACTUAL: " + schema);
    assertTrue(expectedSchema.isEquivalent(schema));
  }
}
