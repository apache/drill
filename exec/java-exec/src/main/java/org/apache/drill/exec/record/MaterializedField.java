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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;


public class MaterializedField {
  private Key key;
  // use an ordered set as existing code relies on order (e,g. parquet writer)
  private Set<MaterializedField> children = Sets.newLinkedHashSet();

  private MaterializedField(SchemaPath path, MajorType type) {
    super();
    key = new Key(path, type);
  }

  public static MaterializedField create(SerializedField serField){
    MaterializedField field = new MaterializedField(SchemaPath.create(serField.getNamePart()), serField.getMajorType());
    for (SerializedField sf:serField.getChildList()) {
      field.addChild(MaterializedField.create(sf));
    }
    return field;
  }

  /**
   * Create and return a serialized field based on the current state.
   */
  public SerializedField getSerializedField() {
    SerializedField.Builder serializedFieldBuilder = getAsBuilder();
    for(MaterializedField childMaterializedField : getChildren()) {
      serializedFieldBuilder.addChild(childMaterializedField.getSerializedField());
    }
    return serializedFieldBuilder.build();
  }


  public SerializedField.Builder getAsBuilder(){
    return SerializedField.newBuilder() //
        .setMajorType(key.type) //
        .setNamePart(key.path.getAsNamePart());
  }

  public Collection<MaterializedField> getChildren() {
    return children;
  }

  public void addChild(MaterializedField field){
    children.add(field);
  }

  public MaterializedField cloneWithType(MajorType type) {
    final MaterializedField clone = new MaterializedField(key.path, type);
    clone.children = Sets.newLinkedHashSet(children);
    return clone;
  }

  public MaterializedField clone(FieldReference ref){
    return create(ref, key.type);
  }

  public String getLastName(){
    PathSegment seg = key.path.getRootSegment();
    while (seg.getChild() != null) {
      seg = seg.getChild();
    }
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
    switch (mt.getMode()){
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
    if (!path.isSimplePath()) {
      return false;
    }

    return key.path.equals(path);
  }


  @Override
  public int hashCode() {
    int result = 1;
    // DRILL-1872: Compute hashCode only on key. See also the comment
    // in MapVector$MapTransferPair
    result = ((key == null) ? 0 : key.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MaterializedField other = (MaterializedField) obj;
    // DRILL-1872: Compute equals only on key. See also the comment
    // in MapVector$MapTransferPair

    if (key == null) {
      if (other.key != null) {
        return false;
      }
    } else if (!key.equals(other.key)) {
      return false;
    }
    return true;
  }


  @Override
  public String toString() {
    final int maxLen = 10;
    String childStr = children != null && !children.isEmpty() ? toString(children, maxLen) : "";
    return key.path + "(" + key.type.getMinorType().name() + ":" + key.type.getMode().name() + ")" + childStr;
  }


  private String toString(Collection<?> collection, int maxLen) {
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    int i = 0;
    for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && i < maxLen; i++) {
      if (i > 0){
        builder.append(", ");
      }
      builder.append(iterator.next());
    }
    builder.append("]");
    return builder.toString();
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
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Key other = (Key) obj;
      if (path == null) {
        if (other.path != null) {
          return false;
        }
      } else if (!path.equals(other.path)) {
        return false;
      }

      return true;
    }

  }

}
