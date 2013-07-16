/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.record;

import java.util.Iterator;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.proto.SchemaDefProtos.FieldDef;
import org.apache.drill.exec.proto.SchemaDefProtos.NamePart;
import org.apache.drill.exec.proto.SchemaDefProtos.NamePart.Type;
import org.apache.drill.exec.vector.TypeHelper;

public class MaterializedField{
  private final FieldDef def;

  public MaterializedField(FieldDef def) {
    this.def = def;
  }
  
  public static MaterializedField create(FieldDef def){
    return new MaterializedField(def);
  }
  
  public static MaterializedField create(SchemaPath path, MajorType type) {
    FieldDef.Builder b = FieldDef.newBuilder();
    b.setMajorType(type);
    addSchemaPathToFieldDef(path, b);
    return create(b.build());
  }

  private static void addSchemaPathToFieldDef(SchemaPath path, FieldDef.Builder builder) {
    for (PathSegment p = path.getRootSegment();; p = p.getChild()) {
      NamePart.Builder b = NamePart.newBuilder();
      if (p.isArray()) {
        b.setType(Type.ARRAY);
      } else {
        b.setName(p.getNameSegment().getPath().toString());
        b.setType(Type.NAME);
      }
      builder.addName(b.build());
      if(p.isLastPath()) break;
    }
  }

  public FieldDef getDef() {
    return def;
  }
  
  public String getName(){
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for(NamePart np : def.getNameList()){
      if(np.getType() == Type.ARRAY){
        sb.append("[]");
      }else{
        if(first){
          first = false;
        }else{
          sb.append(".");
        }
        sb.append(np.getName());
        
      }
    }
    return sb.toString();
  }

  public int getWidth() {
    return def.getMajorType().getWidth();
  }

  public MajorType getType() {
    return def.getMajorType();
  }

  public boolean isNullable() {
    return def.getMajorType().getMode() == DataMode.OPTIONAL;
  }

  public DataMode getDataMode() {
    return def.getMajorType().getMode();
  }
  
  public MaterializedField getOtherNullableVersion(){
    MajorType mt = def.getMajorType();
    DataMode newDataMode = null;
    switch(mt.getMode()){
    case OPTIONAL:
      newDataMode = DataMode.REQUIRED;
      break;
    case REQUIRED:
      newDataMode = DataMode.OPTIONAL;
      break;
    default:
      throw new UnsupportedOperationException();
    }
    return new MaterializedField(def.toBuilder().setMajorType(mt.toBuilder().setMode(newDataMode).build()).build());
  }
  
  public Class<?> getValueClass() {
    return TypeHelper.getValueVectorClass(getType().getMinorType(), getDataMode());
  }

  public boolean matches(SchemaPath path) {
    Iterator<NamePart> iter = def.getNameList().iterator();
    
    for (PathSegment p = path.getRootSegment();; p = p.getChild()) {
      if(p == null) break;
      if (!iter.hasNext()) return false;
      NamePart n = iter.next();

      if (p.isArray()) {
        if (n.getType() == Type.ARRAY) continue;
        return false;
      } else {
        if (p.getNameSegment().getPath().equals(n.getName())) continue;
        return false;
      }
      
    }
    // we've reviewed all path segments. confirm that we don't have any extra name parts.
    return !iter.hasNext();
  }
  
  @Override
  public String toString() {
    return "MaterializedField [" + def.toString() + "]";
  }

  
}