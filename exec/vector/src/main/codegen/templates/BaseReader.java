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
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/reader/BaseReader.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.reader;

<#include "/@includes/vv_imports.ftl" />



@SuppressWarnings("unused")
public interface BaseReader extends Positionable{
  MajorType getType();
  MaterializedField getField();
  void reset();
  void read(UnionHolder holder);
  void read(int index, UnionHolder holder);
  void copyAsValue(UnionWriter writer);
  boolean isSet();

  public interface MapReader extends BaseReader, Iterable<String>{
    FieldReader reader(String name);
  }
  
  public interface RepeatedMapReader extends MapReader{
    boolean next();
    int size();
    void copyAsValue(MapWriter writer);
  }
  
  public interface ListReader extends BaseReader{
    FieldReader reader(); 
  }
  
  public interface RepeatedListReader extends ListReader{
    boolean next();
    int size();
    void copyAsValue(ListWriter writer);
  }
  
  public interface ScalarReader extends  
  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first /> ${name}Reader, </#list></#list> 
  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first /> Repeated${name}Reader, </#list></#list>
  BaseReader {}
  
  interface ComplexReader{
    MapReader rootAsMap();
    ListReader rootAsList();
    boolean rootIsMap();
    boolean ok();
  }
}

