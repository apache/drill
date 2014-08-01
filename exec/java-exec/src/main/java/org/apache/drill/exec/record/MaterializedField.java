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
package org.apache.drill.exec.record;

import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;

import com.google.common.collect.Lists;

public class MaterializedField {
  private Key key;
  private List<MaterializedField> children = Lists.newArrayList();

  private MaterializedField(SchemaPath path, MajorType type) {
    super();
    key = new Key(path, type);
  }

  public static MaterializedField create(SerializedField serField){
    return new MaterializedField(SchemaPath.create(serField.getNamePart()), serField.getMajorType());
  }

  public SerializedField.Builder getAsBuilder(){
    return SerializedField.newBuilder() //
        .setMajorType(key.type) //
        .setNamePart(key.path.getAsNamePart());
  }

  public List<MaterializedField> getChildren() {
    return children;
  }

  public void addChild(MaterializedField field){
    children.add(field);
  }

  public MaterializedField clone(FieldReference ref){
    return create(ref, key.type);
  }

  public String getLastName(){
    PathSegment seg = key.path.getRootSegment();
    while(seg.getChild() != null) seg = seg.getChild();
    return seg.getNameSegment().getPath();
  }


  // TODO: rewrite without as direct match rather than conversion then match.
  public boolean matches(SerializedField field){
    MaterializedField f = create(field);
    return f.equals(this);
  }

  public static MaterializedField create(String path, MajorType type){
    SchemaPath p = SchemaPath.getSimplePath(path);
    return create(p, type);
  }

  public static MaterializedField create(SchemaPath path, MajorType type) {
    return new MaterializedField(path, type);
  }

  public SchemaPath getPath(){
    return key.path;
  }

  /**
   * Get the schema path.  Deprecated, use getPath() instead.
   * @return the SchemaPath of this field.
   */
  @Deprecated
  public SchemaPath getAsSchemaPath(){
    return getPath();
  }

//  public String getName(){
//    StringBuilder sb = new StringBuilder();
//    boolean first = true;
//    for(NamePart np : def.getNameList()){
//      if(np.getType() == Type.ARRAY){
//        sb.append("[]");
//      }else{
//        if(first){
//          first = false;
//        }else{
//          sb.append(".");
//        }
//        sb.append('`');
//        sb.append(np.getName());
//        sb.append('`');
//
//      }
//    }
//    return sb.toString();
//  }

  public int getWidth() {
    return key.type.getWidth();
  }

  public MajorType getType() {
    return key.type;
  }

  public int getScale() {
      return key.type.getScale();
  }
  public int getPrecision() {
      return key.type.getPrecision();
  }
  public boolean isNullable() {
    return key.type.getMode() == DataMode.OPTIONAL;
  }

  public DataMode getDataMode() {
    return key.type.getMode();
  }

  public MaterializedField getOtherNullableVersion(){
    MajorType mt = key.type;
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
    return new MaterializedField(key.path, mt.toBuilder().setMode(newDataMode).build());
  }

  public Class<?> getValueClass() {
    return TypeHelper.getValueVectorClass(getType().getMinorType(), getDataMode());
  }

  public boolean matches(SchemaPath path) {
    if(!path.isSimplePath()) return false;

    return key.path.equals(path);
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((children == null) ? 0 : children.hashCode());
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MaterializedField other = (MaterializedField) obj;
    if (children == null) {
      if (other.children != null)
        return false;
    } else if (!children.equals(other.children))
      return false;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "MaterializedField [path=" + key.path + ", type=" + Types.toString(key.type) + "]";
  }

  public Key key() {
    return key;
  }

  public String toExpr(){
    return key.path.toExpr();
  }

  /**
   * Since the {@code MaterializedField) itself is mutable, in certain cases, it is not suitable
   * as a key of a {@link Map}. This inner class allows the {@link MaterializedField} object to be
   * used for this purpose.
   */
  public class Key {

    private SchemaPath path;
    private MajorType type;

    private Key(SchemaPath path, MajorType type) {
      this.path = path;
      this.type = type;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((path == null) ? 0 : path.hashCode());
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Key other = (Key) obj;
      if (path == null) {
        if (other.path != null)
          return false;
      } else if (!path.equals(other.path))
        return false;
      if (type == null) {
        if (other.type != null)
          return false;
      } else if (!type.equals(other.type))
        return false;
      return true;
    }

  }

}