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
package org.apache.drill.exec.vector.complex.fn;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;

public class JsonWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonWriter.class);

  private final JsonFactory factory = new JsonFactory();
  private final JsonGenerator gen;

  public JsonWriter(OutputStream out, boolean pretty) throws IOException{
    JsonGenerator writer = factory.createJsonGenerator(out);
    gen = pretty ? writer.useDefaultPrettyPrinter() : writer;
  }

  public void write(FieldReader reader) throws JsonGenerationException, IOException{
    writeValue(reader);
    gen.flush();
  }

  private void writeValue(FieldReader reader) throws JsonGenerationException, IOException{
    final DataMode m = reader.getType().getMode();
    final MinorType mt = reader.getType().getMinorType();

    switch(m){
    case OPTIONAL:
      if(!reader.isSet()){
        gen.writeNull();
        break;
      }

    case REQUIRED:


      switch (mt) {
      case FLOAT4:
        gen.writeNumber(reader.readFloat());
        break;
      case FLOAT8:
        gen.writeNumber(reader.readDouble());
        break;
      case INT:
        Integer i = reader.readInteger();
        if(i == null){
          gen.writeNull();
        }else{
          gen.writeNumber(reader.readInteger());
        }
        break;
      case SMALLINT:
        gen.writeNumber(reader.readShort());
        break;
      case TINYINT:
        gen.writeNumber(reader.readByte());
        break;
      case BIGINT:
        Long l = reader.readLong();
        if(l == null){
          gen.writeNull();
        }else{
          gen.writeNumber(reader.readLong());
        }

        break;
      case BIT:
        gen.writeBoolean(reader.readBoolean());
        break;

      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        gen.writeString(reader.readDateTime().toString());

      case INTERVALYEAR:
      case INTERVALDAY:
      case INTERVAL:
        gen.writeString(reader.readPeriod().toString());
        break;
      case DECIMAL28DENSE:
      case DECIMAL28SPARSE:
      case DECIMAL38DENSE:
      case DECIMAL38SPARSE:
      case DECIMAL9:
      case DECIMAL18:
        gen.writeNumber(reader.readBigDecimal());
        break;

      case LIST:
        // this is a pseudo class, doesn't actually contain the real reader so we have to drop down.
        writeValue(reader.reader());
        break;
      case MAP:
        gen.writeStartObject();
        if (reader.isSet()) {
          for(String name : reader){
            FieldReader childReader = reader.reader(name);
            if(childReader.isSet()){
              gen.writeFieldName(name);
              writeValue(childReader);
            }
          }
        }
        gen.writeEndObject();
        break;
      case NULL:
        gen.writeNull();
        break;

      case VAR16CHAR:
        gen.writeString(reader.readString());
        break;
      case VARBINARY:
        gen.writeBinary(reader.readByteArray());
        break;
      case VARCHAR:
        gen.writeString(reader.readText().toString());
        break;

      }
      break;

    case REPEATED:
      gen.writeStartArray();
      switch (mt) {
      case FLOAT4:
        for(int i = 0; i < reader.size(); i++){
          gen.writeNumber(reader.readFloat(i));
        }

        break;
      case FLOAT8:
        for(int i = 0; i < reader.size(); i++){
          gen.writeNumber(reader.readDouble(i));
        }
        break;
      case INT:
        for(int i = 0; i < reader.size(); i++){
          gen.writeNumber(reader.readInteger(i));
        }
        break;
      case SMALLINT:
        for(int i = 0; i < reader.size(); i++){
          gen.writeNumber(reader.readShort(i));
        }
        break;
      case TINYINT:
        for(int i = 0; i < reader.size(); i++){
          gen.writeNumber(reader.readByte(i));
        }
        break;
      case BIGINT:
        for(int i = 0; i < reader.size(); i++){
          gen.writeNumber(reader.readLong(i));
        }
        break;
      case BIT:
        for(int i = 0; i < reader.size(); i++){
        gen.writeBoolean(reader.readBoolean(i));
        }
        break;

      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        for(int i = 0; i < reader.size(); i++){
        gen.writeString(reader.readDateTime(i).toString());
        }

      case INTERVALYEAR:
      case INTERVALDAY:
      case INTERVAL:
        for(int i = 0; i < reader.size(); i++){
        gen.writeString(reader.readPeriod(i).toString());
        }
        break;
      case DECIMAL28DENSE:
      case DECIMAL28SPARSE:
      case DECIMAL38DENSE:
      case DECIMAL38SPARSE:
      case DECIMAL9:
      case DECIMAL18:
        for(int i = 0; i < reader.size(); i++){
        gen.writeNumber(reader.readBigDecimal(i));
        }
        break;

      case LIST:
        for(int i = 0; i < reader.size(); i++){
          while(reader.next()){
            writeValue(reader.reader());
          }
        }
        break;
      case MAP:
        while(reader.next()){
          gen.writeStartObject();
          for(String name : reader){
            FieldReader mapField = reader.reader(name);
            if(mapField.isSet()){
              gen.writeFieldName(name);
              writeValue(mapField);
            }
          }
          gen.writeEndObject();
        }
        break;
      case NULL:
        break;

      case VAR16CHAR:
        for(int i = 0; i < reader.size(); i++){
          gen.writeString(reader.readString(i));
        }
        break;
      case VARBINARY:
        for(int i = 0; i < reader.size(); i++){
        gen.writeBinary(reader.readByteArray(i));
        }
        break;
      case VARCHAR:
        for(int i = 0; i < reader.size(); i++){
        gen.writeString(reader.readText(i).toString());
        }
        break;

      }
      gen.writeEndArray();
      break;
    }

  }

}
