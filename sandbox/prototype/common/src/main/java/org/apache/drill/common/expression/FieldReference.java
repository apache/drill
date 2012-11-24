package org.apache.drill.common.expression;

import java.io.IOException;

import org.apache.drill.common.expression.FieldReference.De;
import org.apache.drill.common.expression.FieldReference.Se;

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

@JsonSerialize(using=Se.class)
@JsonDeserialize(using=De.class)
public class FieldReference extends ValueExpressions.Identifier {
  private String refName;

  public FieldReference(String value) {
    super(value);
  }

  public static class De extends StdDeserializer<FieldReference> {

    public De() {
      super(FieldReference.class);
    }

    @Override
    public FieldReference deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      String s = jp.getText();
      return new FieldReference(s);
    }

  }

  public static class Se extends StdSerializer<FieldReference> {

    public Se() {
      super(FieldReference.class);
    }

    @Override
    public void serialize(FieldReference value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
        JsonGenerationException {
      jgen.writeString(value.refName);
    }

  }

}
