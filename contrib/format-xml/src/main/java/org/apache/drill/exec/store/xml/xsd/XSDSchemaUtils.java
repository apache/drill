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
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaObject;
import org.apache.ws.commons.schema.walker.XmlSchemaWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.stream.StreamSource;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class XSDSchemaUtils {
  private static final Logger logger = LoggerFactory.getLogger(XSDSchemaUtils.class);

  private static final MinorType DEFAULT_TYPE = MinorType.VARCHAR;


  public static final ImmutableMap<String, MinorType> XML_TYPE_MAPPINGS = ImmutableMap.<String, MinorType>builder()
    .put("BASE64BINARY", MinorType.VARBINARY)
    .put("BOOLEAN", MinorType.BIT)
    .put("DATE", MinorType.DATE)
    .put("DATETIME", MinorType.TIMESTAMP)
    .put("DECIMAL", MinorType.VARDECIMAL)
    .put("DOUBLE", MinorType.FLOAT8)
    .put("DURATION", MinorType.INTERVAL)
    .put("FLOAT", MinorType.FLOAT4)
    .put("HEXBINARY", MinorType.VARBINARY)
    .put("STRING", MinorType.VARCHAR)
    .put("TIME", MinorType.TIME)
    .build();

  public static TupleMetadata getSchema(String filename) throws IOException {
    InputStream inputStream = Files.newInputStream(Paths.get(filename));
    return processSchema(inputStream);
  }

  public static TupleMetadata getSchema(InputStream inputStream) {
    return processSchema(inputStream);
  }

  private static TupleMetadata processSchema(InputStream inputStream) {
    XmlSchemaCollection schemaCollection = new XmlSchemaCollection();
    schemaCollection.read(new StreamSource(inputStream));

    DrillXSDSchemaVisitor schemaVisitor = new DrillXSDSchemaVisitor(new SchemaBuilder());
    XmlSchemaWalker walker = new XmlSchemaWalker(schemaCollection, schemaVisitor);

    XmlSchema[] schemas = schemaCollection.getXmlSchemas();
    walker.walk((XmlSchemaElement) schemas[0].getItems().get(0));

    return schemaVisitor.getDrillSchema();
  }

  /**
   * Returns a {@link MinorType} of the corresponding XML Data Type.  Defaults to VARCHAR if unknown
   * @param xmlType A String of the XML Data Type
   * @return A {@link MinorType} of the Drill data type.
   */
  public static MinorType getDrillDataType(String xmlType) {
    try {
      MinorType type = XSDSchemaUtils.XML_TYPE_MAPPINGS.get(xmlType);
      if (type == null) {
        return DEFAULT_TYPE;
      } else {
        return type;
      }
    } catch (NullPointerException e) {
      return DEFAULT_TYPE;
    }
  }


  public static void getDrillSchema(XmlSchema[] schema) throws UnsupportedEncodingException {
    SchemaBuilder drillSchemaBuilder = new SchemaBuilder();

    // Iterate over schema elements
    for (XmlSchema currentElement : schema) {
      List<XmlSchemaObject> schemaObjects = currentElement.getItems();

      for (XmlSchemaObject schemaObject : schemaObjects) {
        if (schemaObject instanceof XmlSchemaElement) {
          XmlSchemaElement element = (XmlSchemaElement) schemaObject;
          System.out.println(element.getName());
        }

      }
    }
  }
}
