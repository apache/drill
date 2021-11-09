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

import java.util.Locale;

public class DrillXSDSchemaVisitor implements XmlSchemaVisitor {
  private static final Logger logger = LoggerFactory.getLogger(DrillXSDSchemaVisitor.class);
  private final SchemaBuilder builder;
  private DrillSchemaField currentField;

  public DrillXSDSchemaVisitor(SchemaBuilder builder) {
    this.builder = builder;
  }

  @Override
  public void onEnterElement(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo, boolean b) {
    logger.debug("Entering element: {}", xmlSchemaElement.getName());
    if (xmlSchemaTypeInfo.getType().name().equalsIgnoreCase("COMPLEX")) {
      logger.debug("Found map in opening element.");
    }
  }

  @Override
  public void onExitElement(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo, boolean b) {
    currentField = new DrillSchemaField();
    currentField.fieldName = xmlSchemaElement.getName();
    currentField.type = currentField.getDrillDataType(xmlSchemaTypeInfo.getBaseType().name().toUpperCase(Locale.ROOT));
    logger.debug("Current field {}", currentField);
  }

  @Override
  public void onVisitAttribute(XmlSchemaElement xmlSchemaElement, XmlSchemaAttrInfo xmlSchemaAttrInfo) {
    logger.debug("Attributes: {}", xmlSchemaAttrInfo);
  }

  @Override
  public void onEndAttributes(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo) {

  }

  @Override
  public void onEnterSubstitutionGroup(XmlSchemaElement xmlSchemaElement) {
    logger.debug("Enter substitution group: {}", xmlSchemaElement.getName());
  }

  @Override
  public void onExitSubstitutionGroup(XmlSchemaElement xmlSchemaElement) {
    logger.debug("Leaving substitution group: {}", xmlSchemaElement.getName());
  }

  @Override
  public void onEnterAllGroup(XmlSchemaAll xmlSchemaAll) {
    logger.debug("Enter all group");
  }

  @Override
  public void onExitAllGroup(XmlSchemaAll xmlSchemaAll) {
    logger.debug("Leaving all group");
  }

  @Override
  public void onEnterChoiceGroup(XmlSchemaChoice xmlSchemaChoice) {

  }

  @Override
  public void onExitChoiceGroup(XmlSchemaChoice xmlSchemaChoice) {

  }

  @Override
  public void onEnterSequenceGroup(XmlSchemaSequence xmlSchemaSequence) {

  }

  @Override
  public void onExitSequenceGroup(XmlSchemaSequence xmlSchemaSequence) {
    logger.debug("Leaving map: ");
  }

  @Override
  public void onVisitAny(XmlSchemaAny xmlSchemaAny) {

  }

  @Override
  public void onVisitAnyAttribute(XmlSchemaElement xmlSchemaElement, XmlSchemaAnyAttribute xmlSchemaAnyAttribute) {

  }
}
