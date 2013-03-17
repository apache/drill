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
package org.apache.drill.common.physical;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.drill.common.physical.FieldSet.De;
import org.apache.drill.common.physical.FieldSet.Se;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.Lists;

@JsonSerialize(using = Se.class)
@JsonDeserialize(using = De.class)
public class FieldSet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FieldSet.class);
  
  private List<RecordField> incoming = Lists.newArrayList();
  private List<RecordField> outgoing = Lists.newArrayList();
  
  public FieldSet(Iterable<RecordField> fields){
    for(RecordField f : fields){
      if(f.getRoute().isIn()){
        incoming.add(f);
      }
      
      if(f.getRoute().isOut()){
        outgoing.add(f);
      }
    }
  }
  

  public static class De extends StdDeserializer<FieldSet> {
    
    public De() {
      super(FieldSet.class);
    }

    @Override
    public FieldSet deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      Iterable<RecordField> fields = jp.readValueAs(new TypeReference<List<RecordField>>(){});
      logger.debug("Fields {}", fields);
      return new FieldSet(fields);
    }

  }

  public static class Se extends StdSerializer<FieldSet> {

    public Se() {
      super(FieldSet.class);
    }

    @Override
    public void serialize(FieldSet value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
        JsonGenerationException {
      HashSet<RecordField> fields = new HashSet<RecordField>();
      for(RecordField f: value.incoming){
        fields.add(f);
      }
      for(RecordField f: value.outgoing){
        fields.add(f);
      }
      jgen.writeObject(Lists.newArrayList(fields));
    }

  }
}
