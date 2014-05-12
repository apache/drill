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

<@pp.dropOutputFile />
<@pp.changeOutputFile name="org/apache/drill/exec/store/EventBasedRecordWriter.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.store;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordValueAccessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/** Reads records from the RecordValueAccessor and writes into RecordWriter. */
public class EventBasedRecordWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EventBasedRecordWriter.class);

  private BatchSchema schema;
  private RecordValueAccessor rva;
  private RecordWriter recordWriter;
  private List<FieldWriter> fieldWriters;

  static private Map<MajorType, Class<? extends FieldWriter>> typeClassMap;

  static {
    typeClassMap = Maps.newHashMap();

<#list vv.types as type>
  <#list type.minor as minor>
    <#list vv.modes as mode>
    typeClassMap.put(${mode.prefix}${minor.class}Holder.TYPE, ${mode.prefix}${minor.class}FieldWriter.class);
    </#list>
  </#list>
</#list>
  }

  public EventBasedRecordWriter(BatchSchema schema, RecordValueAccessor rva, RecordWriter recordWriter)
      throws IOException {
    this.schema = schema;
    this.rva = rva;
    this.recordWriter = recordWriter;

    initFieldWriters();
  }

  public int write() throws IOException {
    int counter = 0;

    rva.resetIterator();
    while(rva.next()) {
      recordWriter.startRecord();
      // write the current record
      int fieldId = 0;
      for (MaterializedField field : schema) {
        fieldWriters.get(fieldId).writeField();
        fieldId++;
      }
      recordWriter.endRecord();
      counter++;
    }

    return counter;
  }

  private void initFieldWriters() throws IOException {
    fieldWriters = Lists.newArrayList();
    try {
      for (int i = 0; i < schema.getFieldCount(); i++) {
        MajorType mt = schema.getColumn(i).getType();
        MajorType newMt = MajorType.newBuilder().setMinorType(mt.getMinorType()).setMode(mt.getMode()).build();
        fieldWriters.add(i, typeClassMap.get(newMt)
                .getConstructor(EventBasedRecordWriter.class, int.class).newInstance(this, i));
      }
    } catch(Exception e) {
      logger.error("Failed to create FieldWriter.", e);
      throw new IOException("Failed to initialize FieldWriters.", e);
    }
  }

  abstract class FieldWriter {
    protected int fieldId;

    public FieldWriter(int fieldId) {
      this.fieldId = fieldId;
    }

    public abstract void writeField() throws IOException;
  }

<#list vv.types as type>
  <#list type.minor as minor>
    <#list vv.modes as mode>
  class ${mode.prefix}${minor.class}FieldWriter extends FieldWriter {
    private ${mode.prefix}${minor.class}Holder holder = new ${mode.prefix}${minor.class}Holder();

    public ${mode.prefix}${minor.class}FieldWriter(int fieldId) {
      super(fieldId);
    }

    public void writeField() throws IOException {
      rva.getFieldById(fieldId, holder);
      recordWriter.add${mode.prefix}${minor.class}Holder(fieldId, holder);
    }
  }

    </#list>
  </#list>
</#list>
}