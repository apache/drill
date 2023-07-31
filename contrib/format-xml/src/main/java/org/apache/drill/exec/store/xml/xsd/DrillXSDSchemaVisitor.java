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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaAny;
import org.apache.ws.commons.schema.XmlSchemaAnyAttribute;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaSequence;
import org.apache.ws.commons.schema.walker.XmlSchemaAttrInfo;
import org.apache.ws.commons.schema.walker.XmlSchemaTypeInfo;
import org.apache.ws.commons.schema.walker.XmlSchemaVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class transforms an XSD schema into a Drill Schema.
 */
public class DrillXSDSchemaVisitor implements XmlSchemaVisitor {
  private static final Logger logger = LoggerFactory.getLogger(DrillXSDSchemaVisitor.class);
  private SchemaBuilder builder;
  private MapBuilder currentMapBuilder;
  private int nestingLevel;

  public DrillXSDSchemaVisitor(SchemaBuilder builder) {
    this.builder = builder;
    this.nestingLevel = -1;
  }

  /**
   * Returns a {@link TupleMetadata} representation of the schema contained in an XSD file. This method should only
   * be called after the walk method of XmlSchemaWalker has been called.
   * @return A {@link TupleMetadata} representation of the XSD schema.
   */
  public TupleMetadata getDrillSchema() {
    return builder.build();
  }

  @Override
  public void onEnterElement(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo, boolean b) {
    boolean isRepeated = xmlSchemaElement.getMaxOccurs() > 1;
    String fieldName = xmlSchemaElement.getName();
    if (xmlSchemaTypeInfo.getType().name().equalsIgnoreCase("COMPLEX")) {
      // Start a map here.
      logger.debug("Starting map {}.", xmlSchemaElement.getName());

      // There are two cases, if the element belongs to a complex object or not.  If it does not, the currentMapBuilder
      // will be null. We therefore have to get a MapBuilder object from the SchemaBuilder and save it as the
      // current MapBuilder.
      //
      // In either case, we also need to determine whether the element in question is an array or not.  If it is,
      // we set the data mode to repeated.
      if (currentMapBuilder == null) {
        // If the current schema element is repeated (IE an array) record it as such.
        if (isRepeated) {
          currentMapBuilder = builder.addMapArray(fieldName);
        } else {
          currentMapBuilder = builder.addMap(fieldName);
        }
      } else {
        // If the current schema element is repeated (IE an array) record it as such.
        if (isRepeated) {
          currentMapBuilder = currentMapBuilder.addMapArray(fieldName);
        } else {
          currentMapBuilder = currentMapBuilder.addMap(fieldName);
        }
      }
      nestingLevel++;
    } else {
      // If the field is a scalar, simply add it to the schema.
      MinorType dataType = DrillXSDSchemaUtils.getDrillDataType(xmlSchemaTypeInfo.getBaseType().name());
      if (currentMapBuilder == null) {
        // If the current map is null, it means we are not in a nested construct
        if (isRepeated) {
          builder.add(fieldName, dataType, DataMode.REPEATED);
        } else {
          builder.addNullable(fieldName, dataType);
        }
      } else {
        // Otherwise, write to the current map builder
        if (isRepeated) {
          currentMapBuilder.add(fieldName, dataType, DataMode.REPEATED);
        } else {
          currentMapBuilder.addNullable(fieldName, dataType);
        }
      }
    }
  }

  @Override
  public void onExitElement(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo, boolean b) {
    // no op
  }

  @Override
  public void onVisitAttribute(XmlSchemaElement xmlSchemaElement, XmlSchemaAttrInfo xmlSchemaAttrInfo) {
    String fieldName = xmlSchemaAttrInfo.getAttribute().getName();
    MinorType dataType = DrillXSDSchemaUtils.getDrillDataType(xmlSchemaAttrInfo.getType().getBaseType().name());
    currentMapBuilder.addNullable(fieldName, dataType);
  }

  @Override
  public void onEndAttributes(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo) {
    // no op
  }

  @Override
  public void onEnterSubstitutionGroup(XmlSchemaElement xmlSchemaElement) {
    // no op
  }

  @Override
  public void onExitSubstitutionGroup(XmlSchemaElement xmlSchemaElement) {
    // no op
  }

  @Override
  public void onEnterAllGroup(XmlSchemaAll xmlSchemaAll) {
    // no op
  }

  @Override
  public void onExitAllGroup(XmlSchemaAll xmlSchemaAll) {
    // no op
  }

  @Override
  public void onEnterChoiceGroup(XmlSchemaChoice xmlSchemaChoice) {
    // no op
  }

  @Override
  public void onExitChoiceGroup(XmlSchemaChoice xmlSchemaChoice) {
    // no op
  }

  @Override
  public void onEnterSequenceGroup(XmlSchemaSequence xmlSchemaSequence) {
    // no op
  }

  @Override
  public void onExitSequenceGroup(XmlSchemaSequence xmlSchemaSequence) {
    // This section closes out a nested object. If the nesting level is greater than 0, we make a call to
    // resumeMap which gets us the parent map.  If we have arrived at the root level, then we need to get a
    // schema builder and clear out the currentMapBuilder by setting it to null.
    if (currentMapBuilder != null) {
      if (nestingLevel > 0) {
        currentMapBuilder = currentMapBuilder.resumeMap();
      } else {
        builder = currentMapBuilder.resumeSchema();
        currentMapBuilder = null;
      }
      nestingLevel--;
    }
  }

  @Override
  public void onVisitAny(XmlSchemaAny xmlSchemaAny) {
    // no op
  }

  @Override
  public void onVisitAnyAttribute(XmlSchemaElement xmlSchemaElement, XmlSchemaAnyAttribute xmlSchemaAnyAttribute) {
    // no op
  }
}
