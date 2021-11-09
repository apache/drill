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

import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
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
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class XSDSchemaUtils {
  private static final Logger logger = LoggerFactory.getLogger(XSDSchemaUtils.class);



  public static XmlSchema[] getSchema(String filename) throws FileNotFoundException {
    InputStream inputStream = new FileInputStream(filename);
    XmlSchemaCollection schemaCollection = new XmlSchemaCollection();
    schemaCollection.read(new StreamSource(inputStream));
    SchemaBuilder builder = new SchemaBuilder();

    XmlSchemaWalker walker = new XmlSchemaWalker(schemaCollection, new DrillXSDSchemaVisitor(builder));

    XmlSchema[] schemas = schemaCollection.getXmlSchemas();
    walker.walk((XmlSchemaElement) schemas[0].getItems().get(0));

    return schemas;
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

  public static TupleMetadata getColumnMetadata(XmlSchemaObject schemaObject) {
    return null;
  }
}
