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
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/writer/BaseWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.writer;

<#include "/@includes/vv_imports.ftl" />

@SuppressWarnings("unused")
public interface BaseWriter extends Positionable{
  FieldWriter getParent();
  boolean ok();
  WriteState getState();
  int getValueCapacity();
  void resetState();

  public interface MapWriter extends BaseWriter{

    MaterializedField getField();
    void checkValueCapacity();

    <#list vv.types as type><#list type.minor as minor>
    <#assign lowerName = minor.class?uncap_first />
    <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
    <#assign upperName = minor.class?upper_case />
    <#assign capName = minor.class?cap_first />
    ${capName}Writer ${lowerName}(String name);
    </#list></#list>
    
    void copyReaderToField(String name, FieldReader reader);
    MapWriter map(String name);
    ListWriter list(String name);
    void start();
    void end();
  }
  
  public interface ListWriter extends BaseWriter{
    void start();
    void end();
    MapWriter map();
    ListWriter list();
    void copyReader(FieldReader reader);
    
    <#list vv.types as type><#list type.minor as minor>
    <#assign lowerName = minor.class?uncap_first />
    <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
    <#assign upperName = minor.class?upper_case />
    <#assign capName = minor.class?cap_first />
    ${capName}Writer ${lowerName}();
    </#list></#list>
  }
  
  public interface ScalarWriter extends  
  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first /> ${name}Writer, </#list></#list> BaseWriter {}
  
  public interface ComplexWriter{
    void allocate();
    void clear();
    void copyReader(FieldReader reader);
    MapWriter rootAsMap();
    ListWriter rootAsList();
    boolean ok();
    
    public void setPosition(int index);
    public void setValueCount(int count);
    public void reset();
  }
}

