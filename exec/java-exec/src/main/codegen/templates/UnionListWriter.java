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
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/UnionListWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

@SuppressWarnings("unused")
public class UnionListWriter extends AbstractFieldWriter {

  ListVector vector;
  UnionVector data;
  UInt4Vector offsets;
  private UnionWriter writer;
  private boolean inMap = false;
  private String mapName;
  private int lastIndex = 0;

  public UnionListWriter(ListVector vector) {
    super(null);
    this.vector = vector;
    this.data = (UnionVector) vector.getDataVector();
    this.writer = new UnionWriter(data);
    this.offsets = vector.getOffsetVector();
  }

  @Override
  public void allocate() {
    vector.allocateNew();
  }

  @Override
  public void clear() {
    vector.clear();
  }

  @Override
  public MaterializedField getField() {
    return null;
  }

  @Override
  public int getValueCapacity() {
    return vector.getValueCapacity();
  }

  @Override
  public void close() throws Exception {

  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>

  <#if !minor.class?starts_with("Decimal")>

  @Override
  public ${name}Writer <#if uncappedName == "int">integer<#else>${uncappedName}</#if>() {
    return this;
  }

  @Override
  public ${name}Writer <#if uncappedName == "int">integer<#else>${uncappedName}</#if>(String name) {
    assert inMap;
    mapName = name;
    return this;
  }

  </#if>

  </#list></#list>

  @Override
  public MapWriter map() {
    inMap = true;
    return this;
  }

  @Override
  public ListWriter list() {
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
    writer.setPosition(nextOffset);
    return writer;
  }

  @Override
  public ListWriter list(String name) {
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    data.getMutator().setType(nextOffset, MinorType.MAP);
    writer.setPosition(nextOffset);
    ListWriter listWriter = writer.list(name);
    return listWriter;
  }

  @Override
  public MapWriter map(String name) {
    MapWriter mapWriter = writer.map(name);
    return mapWriter;
  }

  @Override
  public void startList() {
    vector.getMutator().startNewValue(idx());
  }

  @Override
  public void endList() {

  }

  @Override
  public void start() {
    assert inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    data.getMutator().setType(nextOffset, MinorType.MAP);
    offsets.getMutator().setSafe(idx() + 1, nextOffset);
    writer.setPosition(nextOffset);
  }

  @Override
  public void end() {
    if (inMap) {
      inMap = false;
      final int nextOffset = offsets.getAccessor().get(idx() + 1);
      offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
    }
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>

  <#if !minor.class?starts_with("Decimal")>

  @Override
  public void write${name}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    if (inMap) {
      final int nextOffset = offsets.getAccessor().get(idx() + 1);
      vector.getMutator().setNotNull(idx());
      data.getMutator().setType(nextOffset, MinorType.MAP);
      writer.setPosition(nextOffset);
      ${name}Writer ${uncappedName}Writer = writer.<#if uncappedName == "int">integer<#else>${uncappedName}</#if>(mapName);
      ${uncappedName}Writer.write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    } else {
      final int nextOffset = offsets.getAccessor().get(idx() + 1);
      vector.getMutator().setNotNull(idx());
      writer.setPosition(nextOffset);
      writer.write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
      offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
    }
  }

  </#if>

  </#list></#list>

}
