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
package org.apache.drill.common.expression;

import java.io.IOException;

import org.apache.drill.common.expression.FieldReference.De;
import org.apache.drill.common.expression.FieldReference.Se;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.types.TypeProtos.MajorType;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

@JsonSerialize(using = Se.class)
@JsonDeserialize(using = De.class)
public class FieldReference extends SchemaPath {
  MajorType overrideType;

  public FieldReference(SchemaPath sp) {
    super(sp);
    checkData();
  }

  private void checkData() {
    if (getRootSegment().getChild() != null) {
      throw new UnsupportedOperationException("Field references must be singular names.");
    }

  }


  private void checkSimpleString(CharSequence value) {
    if (value.toString().contains(".")) {
      throw new UnsupportedOperationException(
          String.format(
              "Unhandled field reference \"%s\"; a field reference identifier"
              + " must not have the form of a qualified name (i.e., with \".\").",
              value));
    }
  }

  public FieldReference(CharSequence value) {
    this(value, ExpressionPosition.UNKNOWN);
    checkSimpleString(value);
  }

  public static FieldReference getWithQuotedRef(CharSequence safeString) {
    return new FieldReference(safeString, ExpressionPosition.UNKNOWN, false);
  }


  public FieldReference(CharSequence value, ExpressionPosition pos) {
    this(value, pos, true);
  }

  public FieldReference(CharSequence value, ExpressionPosition pos, boolean check) {
    super(new NameSegment(value), pos);
    if (check) {
      checkData();
      checkSimpleString(value);
    }

  }

  public FieldReference(String value, ExpressionPosition pos, MajorType dataType) {
    this(value, pos);
    this.overrideType = dataType;
  }

  @Override
  public MajorType getMajorType() {
    if (overrideType == null) {
      return super.getMajorType();
    } else {
      return overrideType;
    }
  }

  public static class De extends StdDeserializer<FieldReference> {

    public De() {
      super(FieldReference.class);
    }

    @Override
    public FieldReference deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      String ref = this._parseString(jp, ctxt);
      ref = ref.replace("`", "");
      return new FieldReference(ref, ExpressionPosition.UNKNOWN, false);
    }

  }

  public static class Se extends StdSerializer<FieldReference> {

    public Se() {
      super(FieldReference.class);
    }

    @Override
    public void serialize(FieldReference value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
        JsonGenerationException {
      jgen.writeString('`' + value.getRootSegment().getNameSegment().getPath() + '`');
    }

  }

}
