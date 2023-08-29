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

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.drill.exec.store.xml.XMLReader.ATTRIBUTE_MAP_NAME;


/**
 * This class transforms an XSD schema into a Drill Schema.
 */
public class DrillXSDSchemaVisitor implements XmlSchemaVisitor {
  private static final Logger logger = LoggerFactory.getLogger(DrillXSDSchemaVisitor.class);
  private SchemaBuilder builder;
  private MapBuilder currentMapBuilder;
  private int nestingLevel;

  /**
   * Table to hold attribute info as it is traversed. We construct the
   * attributes map for all the attributes when the walker tells us we're
   * at the end of all the element decl's attributes.
   * <b/>
   * Uses {@link LinkedHashMap} to ensure deterministic behavior which facilitates testability.
   * In this situation it probably does not matter, but it's a good practice.
   */
  private HashMap<XmlSchemaElement, List<XmlSchemaAttrInfo>> attributeInfoTable =
      new LinkedHashMap<>();

  public DrillXSDSchemaVisitor(SchemaBuilder builder) {
    this.builder = builder;
    this.nestingLevel = 0;
  }

  /**
   * Returns a {@link TupleMetadata} representation of the schema contained in an XSD file. This method should only
   * be called after the walk method of XmlSchemaWalker has been called.
   * @return A {@link TupleMetadata} representation of the XSD schema.
   */
  public TupleMetadata getDrillSchema() {
    return builder.build();
  }

  /**
   * Handles global elements establishing a map for the child elements and attributes (if any).
   * <p/>
   * TBD: Does not handle case where multiple elements have the same name as in:
   * <pre>{@code
   * <element name="a" .../>
   * <element name="b" .../>
   * <element name="a" .../>
   * }</pre>
   * There is also the case where they are ambiguous unless namespaces are used:
   * <pre>{@code
   * <element name="a" .../>
   * <element ref="pre:a" .../> <!-- without namespace, ambiguous with prior "a" -->
   * }</pre>
   */
  @Override
  public void onEnterElement(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo, boolean b) {
    assert nestingLevel >= 0;
    boolean isRepeated = xmlSchemaElement.getMaxOccurs() > 1;
    String fieldName = xmlSchemaElement.getName();
    //
    // Note that the child name in constant ATTRIBUTE_MAP_NAME is reserved and cannot be used
    // by any child element.
    // TODO: There are many other things we want to refuse. E.g., if there are mixed content elements.
    //
    if (StringUtils.equals(ATTRIBUTE_MAP_NAME, fieldName)) {
      throw UserException.dataReadError()
          .message("XML schema contains a field named " + ATTRIBUTE_MAP_NAME + " which is a " +
              "reserved word for XML schemata.")
          .build(logger);
    }

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
        // global element declaration
        assert nestingLevel == 0;
        assert xmlSchemaElement.getMaxOccurs() == 1;
        assert xmlSchemaElement.getMinOccurs() == 1;
        currentMapBuilder = builder.addMap(fieldName);
      } else {
        // local element decl or element reference
        // If the current schema element is repeated (IE an array) record it as such.
        if (isRepeated) {
          currentMapBuilder = currentMapBuilder.addMapArray(fieldName);
        } else {
          currentMapBuilder = currentMapBuilder.addMap(fieldName);
        }
      }
      nestingLevel++;
    } else {
      // If the field is a simple type, simply add it to the schema.
      MinorType dataType = DrillXSDSchemaUtils.getDrillDataType(xmlSchemaTypeInfo.getBaseType().name());
      if (currentMapBuilder == null) {
        // global element decl case
        // Now, strictly speaking an XML document cannot just be a single simple type
        // element, but for testing reasons, it is convenient to allow this.
        // If the current map is null, it means we are not in a nested construct
        assert nestingLevel == 0;
        assert xmlSchemaElement.getMaxOccurs() == 1;
        assert xmlSchemaElement.getMinOccurs() == 1;
        builder.addNullable(fieldName, dataType);
      } else {
        // Otherwise, write to the current map builder
        if (isRepeated) {
          currentMapBuilder.add(fieldName, dataType, DataMode.REPEATED);
          logger.debug("Adding array {}.", xmlSchemaElement.getName());
        } else {
          currentMapBuilder.addNullable(fieldName, dataType);
          logger.debug("Adding field {}.", xmlSchemaElement.getName());
        }
      }
      // For simple types, nestingLevel is not increased.
    }
  }

  @Override
  public void onExitElement(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo, boolean b) {
    assert nestingLevel >= 0;
    if (xmlSchemaTypeInfo.getType().name().equalsIgnoreCase("COMPLEX")) {
      assert nestingLevel >= 1;
      // This section closes out a nested object. If the nesting level is greater than 0, we make a call to
      // resumeMap which gets us the parent map.  If we have arrived at the root level, then we need to get a
      // schema builder and clear out the currentMapBuilder by setting it to null.
      assert currentMapBuilder != null;
      logger.debug("Ending map {}.", xmlSchemaElement.getName());
      if (nestingLevel > 1) {
        currentMapBuilder = currentMapBuilder.resumeMap();
      } else {
        builder = currentMapBuilder.resumeSchema();
        currentMapBuilder = null;
      }
      nestingLevel--;
    }
  }

  /**
   * This method just gathers the elements up into a table.
   */
  @Override
  public void onVisitAttribute(XmlSchemaElement xmlSchemaElement, XmlSchemaAttrInfo xmlSchemaAttrInfo) {
    List<XmlSchemaAttrInfo> list =
        attributeInfoTable.getOrDefault(xmlSchemaElement, new ArrayList<>());
    list.add(xmlSchemaAttrInfo);
    attributeInfoTable.put(xmlSchemaElement, list);
  }

  /**
   * Called for each element decl once all its attributes have been previously
   * processed by onVisitAttribute.
   * <b/>
   * Constructs the map for the special attributes child element of each element.
   * Note: does not construct an attribute child map if there are no attributes.
   * <b/>
   * Only supports attributes with no-namespace on their qnames.
   * Or rather, ignores namespaces. Only deals with local names.
   * <b/>
   * TBD: needs to check for attributes with namespaced names
   * and at minimum reject them.
   */
  @Override
  public void onEndAttributes(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo) {
    List<XmlSchemaAttrInfo> attrs = attributeInfoTable.get(xmlSchemaElement);
    attributeInfoTable.remove(xmlSchemaElement); // clean up the table
    // the currentMapBuilder can be null for a global element decl of simple type.
    if (attrs != null && currentMapBuilder != null) {
      logger.debug("Starting map {}.", xmlSchemaElement.getName() + "/attributes");
      assert attrs.size() >= 1;
      currentMapBuilder = currentMapBuilder.addMap(ATTRIBUTE_MAP_NAME);
      attrs.forEach(attr -> {
        String attrName = attr.getAttribute().getName();
        MinorType dataType = DrillXSDSchemaUtils.getDrillDataType(attr.getType().getBaseType().name());
        currentMapBuilder = currentMapBuilder.addNullable(attrName, dataType);
        logger.debug("Adding attribute {}.", attrName);

      });
      logger.debug("Ending map {}.", xmlSchemaElement.getName() + "/attributes");
      currentMapBuilder = currentMapBuilder.resumeMap();
    }
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
    // no op
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
