/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.types;

import org.apache.arrow.vector.BaseValueVector;
import org.apache.drill.exec.proto.UserBitShared.NamePart;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.common.util.MajorTypeHelper;

import java.util.LinkedHashSet;

public class SerializedFieldHelper {
  public static SerializedField.Builder getAsBuilder(MaterializedField field){
    return SerializedField.newBuilder()
            .setMajorType(MajorTypeHelper.getDrillMajorType(field.getType()))
            .setNamePart(NamePart.newBuilder().setName(field.getName()).build());
  }

  public static SerializedField getMetadata(BaseValueVector vector) {
    return getMetadataBuilder(vector).build();
  }

  public static SerializedField.Builder getMetadataBuilder(BaseValueVector vector) {
    return SerializedFieldHelper.getAsBuilder(vector.getField())
            .setValueCount(vector.getAccessor().getValueCount())
            .setBufferLength(vector.getBufferSize());
  }

  public static SerializedField getSerializedField(MaterializedField field) {
    SerializedField.Builder serializedFieldBuilder = getAsBuilder(field);
    for(MaterializedField childMaterializedField : field.getChildren()) {
      serializedFieldBuilder.addChild(getSerializedField(childMaterializedField));
    }
    return serializedFieldBuilder.build();
  }

  public static MaterializedField create(SerializedField serField){
    LinkedHashSet<MaterializedField> children = new LinkedHashSet<>();
    for (SerializedField sf:serField.getChildList()) {
      children.add(create(sf));
    }
    return new MaterializedField(serField.getNamePart().getName(), MajorTypeHelper.getArrowMajorType(serField.getMajorType()), children);
  }

  public static boolean matches(MaterializedField mField, SerializedField field){
    MaterializedField f = create(field);
    return f.equals(mField);
  }
}
