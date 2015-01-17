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
<#list ["Single", "Repeated"] as mode>
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/${mode}MapWriter.java" />
<#if mode == "Single">
<#assign containerClass = "MapVector" />
<#assign index = "idx()">
<#else>
<#assign containerClass = "RepeatedMapVector" />
<#assign index = "currentChildIndex">
</#if>

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
import java.util.Map;

import org.apache.drill.exec.expr.holders.RepeatedMapHolder;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

import com.google.common.collect.Maps;

/* This class is generated using freemarker and the MapWriters.java template */
@SuppressWarnings("unused")
public class ${mode}MapWriter extends AbstractFieldWriter{
  
  protected final ${containerClass} container;
  private final Map<String, FieldWriter> fields = Maps.newHashMap();
  <#if mode == "Repeated">private int currentChildIndex = 0;</#if>
  
  public ${mode}MapWriter(${containerClass} container, FieldWriter parent) {
    super(parent);
    this.container = container;
  }

  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  public MaterializedField getField() {
      return container.getField();
  }

  public void checkValueCapacity(){
    <#if mode == "Repeated">
    if (container.getValueCapacity() <= idx()) {
      container.reAlloc();
    }
    </#if>
  }

  public MapWriter map(String name){
    FieldWriter writer = fields.get(name);
    if(writer == null){
      int vectorCount = container.size();
      MapVector vector = container.addOrGet(name, MapVector.TYPE, MapVector.class);
      writer = new SingleMapWriter(vector, this);
      if(vectorCount != container.size()) writer.allocate();
      writer.setPosition(${index});
      fields.put(name, writer);
    }
    return writer;
  }
  
  public void allocate(){
    inform(container.allocateNewSafe());
    for(FieldWriter w : fields.values()){
      w.allocate();
    }
  }
  
  public void clear(){
    container.clear();
    for(FieldWriter w : fields.values()){
      w.clear();
    }
    
  }
  
  
  public ListWriter list(String name){
    FieldWriter writer = fields.get(name);
    if(writer == null){
      writer = new SingleListWriter(name, container, this);
      writer.setPosition(${index});
      fields.put(name, writer);
    }
    return writer;
  }
  

  <#if mode == "Repeated">
  public void start(){
    if(ok()){
      checkValueCapacity();
      if (!ok()) return;
      // update the repeated vector to state that there is current+1 objects.
      
      RepeatedMapHolder h = new RepeatedMapHolder();
      container.getAccessor().get(idx(), h);
      if(h.start >= h.end){
        container.getMutator().startNewGroup(idx());  
      }
      currentChildIndex = container.getMutator().add(idx());
      if(currentChildIndex == -1){
        inform(false);
      }else{
        for(FieldWriter w: fields.values()){
          w.setPosition(currentChildIndex);  
        }
      }
    }
  }
  

  public void end(){
    // noop
  }
  <#else>

  public void setValueCount(int count){
    container.getMutator().setValueCount(count);
  }

  public void setPosition(int index){
    super.setPosition(index);
    for(FieldWriter w: fields.values()){
      w.setPosition(index);  
    }
  }
  public void start(){
  }
  
  public void end(){
    //  noop
  }
  </#if>
  
  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#assign vectName = capName />
  <#assign vectName = "Nullable${capName}" />
  
  private static final MajorType ${upperName}_TYPE = Types.optional(MinorType.${upperName});
  public ${minor.class}Writer ${lowerName}(String name){
    FieldWriter writer = fields.get(name);
    if(writer == null){
      ${vectName}Vector vector = container.addOrGet(name, ${upperName}_TYPE, ${vectName}Vector.class);
      vector.allocateNewSafe();
      writer = new ${vectName}WriterImpl(vector, this);
      writer.setPosition(${index});
      fields.put(name, writer);
    }
    return writer;
  }
  
  
  </#list></#list>

  

}
</#list>


