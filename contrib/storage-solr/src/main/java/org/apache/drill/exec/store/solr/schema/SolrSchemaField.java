/**
 * Licensed to the Apache Software Foundation import java.util.List;
ore contributor license agreements.  See the NOTICE file
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
package org.apache.drill.exec.store.solr.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class SolrSchemaField {

  private String fieldName;
  private String type;
  private boolean skipdelete;
  private boolean docValues;

  @JsonIgnore
  private boolean indexed;
  @JsonIgnore
  private boolean stored;
  @JsonIgnore
  private boolean multiValued;
  @JsonIgnore
  private boolean termVectors;
  @JsonIgnore
  private boolean omitPositions;
  @JsonIgnore
  private boolean omitNorms;
  @JsonIgnore
  private boolean required;
  @JsonIgnore
  private boolean omitTermFreqAndPositions;

  public SolrSchemaField() {
    indexed = stored = false;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public boolean isIndexed() {
    return indexed;
  }

  public void setIndexed(boolean indexed) {
    this.indexed = indexed;
  }

  public boolean isStored() {
    return stored;
  }

  public void setStored(boolean stored) {
    this.stored = stored;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public boolean isMultiValued() {
    return multiValued;
  }

  public void setMultiValued(boolean multiValued) {
    this.multiValued = multiValued;
  }

  public boolean isTermVectors() {
    return termVectors;
  }

  public void setTermVectors(boolean termVectors) {
    this.termVectors = termVectors;
  }

  public boolean isOmitPositions() {
    return omitPositions;
  }

  public void setOmitPositions(boolean omitPositions) {
    this.omitPositions = omitPositions;
  }

  public boolean isSkipdelete() {
    return skipdelete;
  }

  public void setSkipdelete(boolean skipdelete) {
    this.skipdelete = skipdelete;
  }

  public boolean isOmitNorms() {
    return omitNorms;
  }

  public void setOmitNorms(boolean omitNorms) {
    this.omitNorms = omitNorms;
  }

  public boolean isRequired() {
    return required;
  }

  public void setRequired(boolean required) {
    this.required = required;
  }

  public boolean isDocValues() {
    return docValues;
  }

  public void setDocValues(boolean docValues) {
    this.docValues = docValues;
  }

  public boolean isOmitTermFreqAndPositions() {
    return omitTermFreqAndPositions;
  }

  public void setOmitTermFreqAndPositions(boolean omitTermFreqAndPositions) {
    this.omitTermFreqAndPositions = omitTermFreqAndPositions;
  }

}
