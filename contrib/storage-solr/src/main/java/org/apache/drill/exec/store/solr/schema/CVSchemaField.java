/**
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
package org.apache.drill.exec.store.solr.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CVSchemaField implements Serializable {
  protected String fieldName = null;

  protected String type = null;

  protected Boolean indexed = null;

  protected Boolean stored = null;

  protected Boolean multiValued = null;

  protected Boolean required = null;

  protected Boolean termVectors = null;

  protected Boolean omitNorms = null;

  protected Boolean omitPositions = null;

  protected Boolean omitTermFreqAndPositions = null;

  protected Boolean skipdelete = null;

  protected Boolean dynamicField = null;

  /*
   * *will copy to default field
   */
  protected Boolean searchDefault = null;

  /*
   * *will copy to autocomplete field
   */
  protected Boolean autocomplete = null;

  /*
   * *will copy to spell field
   */
  protected Boolean spellcheck = null;

  protected List<String> copyFields = null;

  protected Boolean idxCopyField = null;

  /**
   * copy constructors
   **/
  public CVSchemaField(CVSchemaField obj) {
    if (obj == null)
      return;
    if (obj.fieldName != null) {
      this.fieldName = new String(obj.fieldName);
    }
    if (obj.type != null) {
      this.type = new String(obj.type);
    }
    if (obj.indexed != null) {
      this.indexed = new Boolean(obj.indexed);
    }
    if (obj.stored != null) {
      this.stored = new Boolean(obj.stored);
    }
    if (obj.multiValued != null) {
      this.multiValued = new Boolean(obj.multiValued);
    }
    if (obj.required != null) {
      this.required = new Boolean(obj.required);
    }
    if (obj.termVectors != null) {
      this.termVectors = new Boolean(obj.termVectors);
    }
    if (obj.omitNorms != null) {
      this.omitNorms = new Boolean(obj.omitNorms);
    }
    if (obj.omitPositions != null) {
      this.omitPositions = new Boolean(obj.omitPositions);
    }
    if (obj.omitTermFreqAndPositions != null) {
      this.omitTermFreqAndPositions = new Boolean(obj.omitTermFreqAndPositions);
    }
    if (obj.skipdelete != null) {
      this.skipdelete = new Boolean(obj.skipdelete);
    }
    if (obj.dynamicField != null) {
      this.dynamicField = new Boolean(obj.dynamicField);
    }
    if (obj.searchDefault != null) {
      this.searchDefault = new Boolean(obj.searchDefault);
    }
    if (obj.autocomplete != null) {
      this.autocomplete = new Boolean(obj.autocomplete);
    }
    if (obj.spellcheck != null) {
      this.spellcheck = new Boolean(obj.spellcheck);
    }
    if (obj.copyFields != null) {
      this.copyFields = new ArrayList<String>(obj.getCopyFields().size());
      for (String listObj : obj.getCopyFields()) {
        this.copyFields.add(new String(listObj));
      }
    }
    if (obj.idxCopyField != null) {
      this.idxCopyField = new Boolean(obj.idxCopyField);
    }
  }

  public CVSchemaField() {
  }

  public String getFieldName(boolean init) {
    if (init) {
      return getFieldName();
    } else {
      return this.fieldName;
    }
  }

  public Boolean getIdxCopyField() {
    return idxCopyField;
  }

  public Boolean getIdxCopyField(boolean init) {
    if (init) {
      return getIdxCopyField();
    } else {

      return this.idxCopyField;
    }
  }

  public String getFieldName() {
    if (fieldName == null) {
      this.fieldName = "";
    }
    return this.fieldName;
  }

  public void setFieldName(String name) {
    this.fieldName = name;

  }

  public String getType(boolean init) {
    if (init) {
      return getType();
    } else {
      return this.type;
    }
  }

  public String getType() {
    if (type == null) {
      this.type = "";
    }
    return this.type;
  }

  public void setType(String type) {
    this.type = type;

  }

  public Boolean getIndexed(boolean init) {
    if (init) {
      return getIndexed();
    } else {
      return this.indexed;
    }
  }

  public Boolean getIndexed() {
    if (indexed == null) {
      this.indexed = Boolean.valueOf(false);
    }
    return this.indexed;
  }

  public void setIndexed(Boolean indexed) {
    this.indexed = indexed;

  }

  public Boolean getStored(boolean init) {
    if (init) {
      return getStored();
    } else {
      return this.stored;
    }
  }

  public Boolean getStored() {
    if (stored == null) {
      this.stored = Boolean.valueOf(false);
    }
    return this.stored;
  }

  public void setStored(Boolean stored) {
    this.stored = stored;

  }

  public Boolean getMultiValued(boolean init) {
    if (init) {
      return getMultiValued();
    } else {
      return this.multiValued;
    }
  }

  public Boolean getMultiValued() {
    if (multiValued == null) {
      this.multiValued = Boolean.valueOf(false);
    }
    return this.multiValued;
  }

  public void setMultiValued(Boolean multiValued) {
    this.multiValued = multiValued;

  }

  public Boolean getRequired(boolean init) {
    if (init) {
      return getRequired();
    } else {
      return this.required;
    }
  }

  public Boolean getRequired() {
    if (required == null) {
      this.required = Boolean.valueOf(false);
    }
    return this.required;
  }

  public void setRequired(Boolean required) {
    this.required = required;

  }

  public Boolean getTermVectors(boolean init) {
    if (init) {
      return getTermVectors();
    } else {
      return this.termVectors;
    }
  }

  public Boolean getTermVectors() {
    if (termVectors == null) {
      this.termVectors = Boolean.valueOf(false);
    }
    return this.termVectors;
  }

  public void setTermVectors(Boolean termVectors) {
    this.termVectors = termVectors;

  }

  public Boolean getOmitNorms(boolean init) {
    if (init) {
      return getOmitNorms();
    } else {
      return this.omitNorms;
    }
  }

  public Boolean getOmitNorms() {
    if (omitNorms == null) {
      this.omitNorms = Boolean.valueOf(false);
    }
    return this.omitNorms;
  }

  public void setOmitNorms(Boolean omitNorms) {
    this.omitNorms = omitNorms;

  }

  public Boolean getOmitPositions(boolean init) {
    if (init) {
      return getOmitPositions();
    } else {
      return this.omitPositions;
    }
  }

  public Boolean getOmitPositions() {
    if (omitPositions == null) {
      this.omitPositions = Boolean.valueOf(false);
    }
    return this.omitPositions;
  }

  public void setOmitPositions(Boolean omitPositions) {
    this.omitPositions = omitPositions;

  }

  public Boolean getOmitTermFreqAndPositions(boolean init) {
    if (init) {
      return getOmitTermFreqAndPositions();
    } else {
      return this.omitTermFreqAndPositions;
    }
  }

  public Boolean getOmitTermFreqAndPositions() {
    if (omitTermFreqAndPositions == null) {
      this.omitTermFreqAndPositions = Boolean.valueOf(false);
    }
    return this.omitTermFreqAndPositions;
  }

  public void setOmitTermFreqAndPositions(Boolean omitTermFreqAndPositions) {
    this.omitTermFreqAndPositions = omitTermFreqAndPositions;

  }

  public Boolean getSkipdelete(boolean init) {
    if (init) {
      return getSkipdelete();
    } else {
      return this.skipdelete;
    }
  }

  public Boolean getSkipdelete() {
    if (skipdelete == null) {
      this.skipdelete = Boolean.valueOf(false);
    }
    return this.skipdelete;
  }

  public void setSkipdelete(Boolean skipdelete) {
    this.skipdelete = skipdelete;

  }

  public Boolean getDynamicField(boolean init) {
    if (init) {
      return getDynamicField();
    } else {
      return this.dynamicField;
    }
  }

  public Boolean getDynamicField() {
    if (dynamicField == null) {
      this.dynamicField = Boolean.valueOf(false);
    }
    return this.dynamicField;
  }

  public void setDynamicField(Boolean dynamicField) {
    this.dynamicField = dynamicField;

  }

  public Boolean getSearchDefault(boolean init) {
    if (init) {
      return getSearchDefault();
    } else {
      return this.searchDefault;
    }
  }

  public Boolean getSearchDefault() {
    if (searchDefault == null) {
      this.searchDefault = Boolean.valueOf(false);
    }
    return this.searchDefault;
  }

  public void setSearchDefault(Boolean searchDefault) {
    this.searchDefault = searchDefault;

  }

  public Boolean getAutocomplete(boolean init) {
    if (init) {
      return getAutocomplete();
    } else {
      return this.autocomplete;
    }
  }

  public Boolean getAutocomplete() {
    if (autocomplete == null) {
      this.autocomplete = Boolean.valueOf(false);
    }
    return this.autocomplete;
  }

  public void setAutocomplete(Boolean autocomplete) {
    this.autocomplete = autocomplete;

  }

  public Boolean getSpellcheck(boolean init) {
    if (init) {
      return getSpellcheck();
    } else {
      return this.spellcheck;
    }
  }

  public Boolean getSpellcheck() {
    if (spellcheck == null) {
      this.spellcheck = Boolean.valueOf(false);
    }
    return this.spellcheck;
  }

  public void setSpellcheck(Boolean spellcheck) {
    this.spellcheck = spellcheck;

  }

  public List<String> getCopyFields(boolean init) {
    if (init) {
      return getCopyFields();
    } else {
      return this.copyFields;
    }
  }

  public List<String> getCopyFields() {
    if (copyFields == null) {
      this.copyFields = new ArrayList<String>();
    }
    return this.copyFields;
  }

  public void setCopyFields(List<String> copyFields) {
    this.copyFields = copyFields;

  }

  public static List<String> getAllFields() {
    ArrayList<String> list = new ArrayList<String>();
    list.add(Fields.FIELD_NAME);
    list.add(Fields.TYPE);
    list.add(Fields.INDEXED);
    list.add(Fields.STORED);
    list.add(Fields.MULTI_VALUED);
    list.add(Fields.REQUIRED);
    list.add(Fields.TERM_VECTORS);
    list.add(Fields.OMIT_NORMS);
    list.add(Fields.OMIT_POSITIONS);
    list.add(Fields.OMIT_TERM_FREQ_AND_POSITIONS);
    list.add(Fields.SKIPDELETE);
    list.add(Fields.DYNAMIC_FIELD);
    list.add(Fields.SEARCH_DEFAULT);
    list.add(Fields.AUTOCOMPLETE);
    list.add(Fields.SPELLCHECK);
    list.add(Fields.COPY_FIELDS);
    list.add(Fields.IDX_COPY_FIELD);
    return list;
  }

  public static interface Fields {
    String FIELD_NAME = "name";
    String TYPE = "type";
    String INDEXED = "indexed";
    String STORED = "stored";
    String MULTI_VALUED = "multiValued";
    String REQUIRED = "required";
    String TERM_VECTORS = "termVectors";
    String OMIT_NORMS = "omitNorms";
    String OMIT_POSITIONS = "omitPositions";
    String OMIT_TERM_FREQ_AND_POSITIONS = "omitTermFreqAndPositions";
    String SKIPDELETE = "skipdelete";
    String DYNAMIC_FIELD = "dynamicField";
    String SEARCH_DEFAULT = "searchDefault";
    String AUTOCOMPLETE = "autocomplete";
    String SPELLCHECK = "spellcheck";
    String COPY_FIELDS = "copyFields";
    String IDX_COPY_FIELD = "idxCopyField";
  }

}
