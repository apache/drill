/*
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.proto.UserBitShared.NamePart;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;

/**
 * Meta-data description of a column characterized by a name and a type
 * (including both data type and cardinality AKA mode). For map types,
 * the description includes the nested columns.)
 */

public class MaterializedField {
  private final String name;
  private MajorType type;
  // use an ordered set as existing code relies on order (e,g. parquet writer)
  private final LinkedHashSet<MaterializedField> children;

  private MaterializedField(String name, MajorType type, LinkedHashSet<MaterializedField> children) {
    this.name = name;
    this.type = type;
    this.children = children;
  }

  public static MaterializedField create(SerializedField serField) {
    LinkedHashSet<MaterializedField> children = new LinkedHashSet<>();
    for (SerializedField sf : serField.getChildList()) {
      children.add(MaterializedField.create(sf));
    }
    return new MaterializedField(serField.getNamePart().getName(), serField.getMajorType(), children);
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

  public SerializedField.Builder getAsBuilder() {
    return SerializedField.newBuilder()
        .setMajorType(type)
        .setNamePart(NamePart.newBuilder().setName(name).build());
  }

  public Collection<MaterializedField> getChildren() {
    return new ArrayList<>(children);
  }

  public MaterializedField newWithChild(MaterializedField child) {
    MaterializedField newField = clone();
    newField.addChild(child);
    return newField;
  }

  public void addChild(MaterializedField field) {
    children.add(field);
  }

  public void removeChild(MaterializedField field) {
    children.remove(field);
  }

  /**
   * Replace the type with a new one that has the same minor type
   * and mode, but with perhaps different details.
   * <p>
   * The type is immutable. But, it contains subtypes, used or lists
   * and unions. To add a subtype, we must create a whole new major type.
   * <p>
   * It appears that the <tt>MaterializedField</tt> class was also meant
   * to be immutable. But, it holds the children for a map, and contains
   * methods to add children. So, it is not immutable.
   * <p>
   * This method allows evolving a list or union without the need to create
   * a new <tt>MaterializedField</tt>. Doing so is problematic for nested
   * maps because the map (or list, or union) holds onto the
   * <tt>MaterializedField</tt>'s of its children. There is no way for
   * an inner map to reach out and change the child of its parent.
   * <p>
   * By allowing the non-critical metadata to change, we preserve the
   * child relationships as a list or union evolves.
   * @param type
   */

  public void replaceType(MajorType newType) {
    assert type.getMinorType() == newType.getMinorType();
    assert type.getMode() == newType.getMode();
    type = newType;
  }

  @Override
  public MaterializedField clone() {
    return withPathAndType(name, getType());
  }

  public MaterializedField cloneEmpty() {
    return create(name, type.toBuilder()
        .clearSubType()
        .build());
  }

  public MaterializedField withType(MajorType type) {
    return withPathAndType(name, type);
  }

  public MaterializedField withPath(String name) {
    return withPathAndType(name, getType());
  }

  public MaterializedField withPathAndType(String name, final MajorType type) {
    final LinkedHashSet<MaterializedField> newChildren = new LinkedHashSet<>(children.size());
    for (final MaterializedField child:children) {
      newChildren.add(child.clone());
    }
    return new MaterializedField(name, type, newChildren);
  }

  // TODO: rewrite without as direct match rather than conversion then match.
  public boolean matches(SerializedField field) {
    MaterializedField f = create(field);
    return f.equals(this);
  }

  public static MaterializedField create(String name, MajorType type) {
    return new MaterializedField(name, type, new LinkedHashSet<MaterializedField>());
  }

  public String getName() { return name; }
  public int getWidth() { return type.getWidth(); }
  public MajorType getType() { return type; }
  public int getScale() { return type.getScale(); }
  public int getPrecision() { return type.getPrecision(); }
  public boolean isNullable() { return type.getMode() == DataMode.OPTIONAL; }
  public DataMode getDataMode() { return type.getMode(); }

  public MaterializedField getOtherNullableVersion() {
    MajorType mt = type;
    DataMode newDataMode;
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
    return new MaterializedField(name, mt.toBuilder().setMode(newDataMode).build(), children);
  }

  public Class<?> getValueClass() {
    return BasicTypeHelper.getValueVectorClass(getType().getMinorType(), getDataMode());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.name, this.type, this.children);
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

    return this.name.equalsIgnoreCase(other.name) &&
            Objects.equals(this.type, other.type);
  }

  /**
   * Determine if one column is logically equivalent to another. This is
   * a tricky issue. The rules here:
   * <ul>
   * <li>The other schema is assumed to be non-null (unlike
   * <tt>equals()</tt>).</li>
   * <li>Names must be identical, ignoring case. (Drill, like SQL, is
   * case insensitive.)
   * <li>Type, mode, precision and scale must be identical.</li>
   * <li>Child columns are ignored unless the type is a map. That is, the
   * hidden "$bits" and "$offsets" vector columns are not compared, as
   * one schema may be an "original" (without these hidden columns) while
   * the other may come from a vector (which has the hidden columns added.
   * The standard <tt>equals()</tt> comparison does consider hidden
   * columns.</li>
   * <li>For maps, the child columns are compared recursively. This version
   * requires that the two sets of columns appear in the same order. (It
   * assumes it is being used in a context where column indexes make
   * sense.) Operators that want to reconcile two maps that differ only in
   * column order need a different comparison.</li>
   * </ul>
   *
   * @param other another field
   * @return <tt>true</tt> if the columns are identical according to the
   * above rules, <tt>false</tt> if they differ
   */

  public boolean isEquivalent(MaterializedField other) {
    if (this == other) {
      return true;
    }
    if (! name.equalsIgnoreCase(other.name)) {
      return false;
    }

    // Requires full type equality, including fields such as precision and scale.
    // But, unset fields are equivalent to 0. Can't use the protobuf-provided
    // isEquals(), that treats set and unset fields as different.

    if (! Types.isEquivalent(type, other.type)) {
      return false;
    }

    // Compare children -- but only for maps, not the internal children
    // for Varchar, repeated or nullable types.

    if (type.getMinorType() != MinorType.MAP) {
      return true;
    }

    if (children == null || other.children == null) {
      return children == other.children;
    }
    if (children.size() != other.children.size()) {
      return false;
    }

    // Maps are name-based, not position. But, for our
    // purposes, we insist on identical ordering.

    Iterator<MaterializedField> thisIter = children.iterator();
    Iterator<MaterializedField> otherIter = other.children.iterator();
    while (thisIter.hasNext()) {
      MaterializedField thisChild = thisIter.next();
      MaterializedField otherChild = otherIter.next();
      if (! thisChild.isEquivalent(otherChild)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Determine if the present column schema can be promoted to the
   * given schema. Promotion is possible if the schemas are
   * equivalent, or if required mode is promoted to nullable, or
   * if scale or precision can be increased.
   *
   * @param other the field to which this one is to be promoted
   * @return true if promotion is possible, false otherwise
   */

  public boolean isPromotableTo(MaterializedField other, boolean allowModeChange) {
    if (! name.equalsIgnoreCase(other.name)) {
      return false;
    }

    // Requires full type equality, including fields such as precision and scale.
    // But, unset fields are equivalent to 0. Can't use the protobuf-provided
    // isEquals(), that treats set and unset fields as different.

    if (type.getMinorType() != other.type.getMinorType()) {
      return false;
    }
    if (type.getMode() != other.type.getMode()) {

      // Modes differ, but type can be promoted from required to
      // nullable

      if (! allowModeChange) {
        return false;
      }
      if (! (type.getMode() == DataMode.REQUIRED && other.type.getMode() == DataMode.OPTIONAL)) {
        return false;
      }
    }
    if (type.getScale() > other.type.getScale()) {
      return false;
    }
    if (type.getPrecision() > other.type.getPrecision()) {
      return false;
    }

    // Compare children -- but only for maps, not the internal children
    // for Varchar, repeated or nullable types.

    if (type.getMinorType() != MinorType.MAP) {
      return true;
    }

    if (children == null  ||  other.children == null) {
      return children == other.children;
    }
    if (children.size() != other.children.size()) {
      return false;
    }

    // Maps are name-based, not position. But, for our
    // purposes, we insist on identical ordering.

    Iterator<MaterializedField> thisIter = children.iterator();
    Iterator<MaterializedField> otherIter = other.children.iterator();
    while (thisIter.hasNext()) {
      MaterializedField thisChild = thisIter.next();
      MaterializedField otherChild = otherIter.next();
      if (! thisChild.isPromotableTo(otherChild, allowModeChange)) {
        return false;
      }
    }
    return true;
  }

  /**
   * <p>Creates materialized field string representation.
   * Includes field name, its type with precision and scale if any and data mode.
   * Nested fields if any are included. Number of nested fields to include is limited to 10.</p>
   *
   * <b>FIELD_NAME(TYPE(PRECISION,SCALE):DATA_MODE)[NESTED_FIELD_1, NESTED_FIELD_2]</b><br>
   * <p>Example: ok(BIT:REQUIRED), col(VARCHAR(3):OPTIONAL), emp_id(DECIMAL28SPARSE(6,0):REQUIRED)</p>
   *
   * @return materialized field string representation
   */

  @Override
  public String toString() {
    final int maxLen = 10;
    StringBuilder builder = new StringBuilder();
    builder
      .append("[`")
      .append(name)
      .append("` (")
      .append(type.getMinorType().name());

    if (type.hasPrecision()) {
      builder.append("(");
      builder.append(type.getPrecision());
      if (type.hasScale()) {
        builder.append(", ");
        builder.append(type.getScale());
      }
      builder.append(")");
    }

    builder
      .append(":")
      .append(type.getMode().name())
      .append(")");

    if (type.getSubTypeCount() > 0) {
      builder
        .append(", subtypes=(")
        .append(type.getSubTypeList().toString())
        .append(")");
    }

    if (children != null && ! children.isEmpty()) {
      builder
        .append(", children=(")
        .append(toString(children, maxLen))
        .append(")");
    }

    return builder
        .append("]")
        .toString();
  }

  /**
   * Return true if two fields have identical MinorType and Mode.
   * @param that
   * @return
   */
  public boolean hasSameTypeAndMode(MaterializedField that) {
    return (getType().getMinorType() == that.getType().getMinorType())
        && (getType().getMode() == that.getType().getMode());
  }

  private String toString(Collection<?> collection, int maxLen) {
    StringBuilder builder = new StringBuilder();
    int i = 0;
    for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && i < maxLen; i++) {
      if (i > 0){
        builder.append(", ");
      }
      builder.append(iterator.next());
    }
    return builder.toString();
  }
}
