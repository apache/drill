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
package org.apache.drill.exec.vector.complex.writer;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.fn.JsonWriter;
import org.apache.drill.exec.vector.complex.fn.ReaderJSONRecordSplitter;
import org.apache.drill.exec.vector.complex.impl.ComplexWriterImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class TestJsonReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJsonReader.class);

  private static BufferAllocator allocator;

  @BeforeClass
  public static void setupAllocator(){
    allocator = new TopLevelAllocator();
  }

  @AfterClass
  public static void destroyAllocator(){
    allocator.close();
  }

  @Test
  public void testReader() throws Exception{
    final int repeatSize = 10;

    String simple = " { \"b\": \"hello\", \"c\": \"goodbye\"}\n " +
        "{ \"b\": \"yellow\", \"c\": \"red\", \"p\":" +
    "{ \"integer\" : 2001, \n" +
        "  \"float\"   : 1.2,\n" +
        "  \"x\": {\n" +
        "    \"y\": \"friends\",\n" +
        "    \"z\": \"enemies\"\n" +
        "  },\n" +
        "  \"z\": [\n" +
        "    {\"orange\" : \"black\" },\n" +
        "    {\"pink\" : \"purple\" }\n" +
        "  ]\n" +
        "  \n" +
        "}\n }";

    String compound = simple;
    for(int i =0; i < repeatSize; i++) compound += simple;

//    simple = "{ \"integer\" : 2001, \n" +
//        "  \"float\"   : 1.2\n" +
//        "}\n" +
//        "{ \"integer\" : -2002,\n" +
//        "  \"float\"   : -1.2 \n" +
//        "}";
    MapVector v = new MapVector("", allocator);
    ComplexWriterImpl writer = new ComplexWriterImpl("col", v);
    writer.allocate();


    JsonReader jsonReader = new JsonReader(new ReaderJSONRecordSplitter(compound));
    int i =0;
    List<Integer> batchSizes = Lists.newArrayList();

    outside: while(true){
      writer.setPosition(i);
      switch(jsonReader.write(writer)){
      case WRITE_SUCCEED:
        i++;
        break;
      case NO_MORE:
        batchSizes.add(i);
        System.out.println("no more records - main loop");
        break outside;

      case WRITE_FAILED:
        System.out.println("==== hit bounds at " + i);
        //writer.setValueCounts(i - 1);
        batchSizes.add(i);
        i = 0;
        writer.allocate();
        writer.reset();

        switch(jsonReader.write(writer)){
        case NO_MORE:
          System.out.println("no more records - new alloc loop.");
          break outside;
        case WRITE_FAILED:
          throw new RuntimeException("Failure while trying to write.");
        case WRITE_SUCCEED:
          i++;
        };

      };
    }

    int total = 0;
    int lastRecordCount = 0;
    for(Integer records : batchSizes){
      total += records;
      lastRecordCount = records;
    }


    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

    ow.writeValueAsString(v.getAccessor().getObject(0));
    ow.writeValueAsString(v.getAccessor().getObject(1));
    FieldReader reader = v.get("col", MapVector.class).getAccessor().getReader();

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    JsonWriter jsonWriter = new JsonWriter(stream, true);

    reader.setPosition(0);
    jsonWriter.write(reader);
    reader.setPosition(1);
    jsonWriter.write(reader);
    System.out.print("Json Read: ");
    System.out.println(new String(stream.toByteArray(), Charsets.UTF_8));
//    System.out.println(compound);

    System.out.println("Total Records Written " + batchSizes);

    reader.setPosition(lastRecordCount - 2);
    assertEquals("goodbye", reader.reader("c").readText().toString());
    reader.setPosition(lastRecordCount - 1);
    assertEquals("red", reader.reader("c").readText().toString());
    assertEquals((repeatSize+1) * 2, total);

    writer.clear();

  }
}
