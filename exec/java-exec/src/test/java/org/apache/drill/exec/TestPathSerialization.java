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
package org.apache.drill.exec;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.drill.common.util.JacksonUtils;
import org.apache.drill.exec.serialization.PathSerDe;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.test.DrillTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestPathSerialization extends DrillTest {

  @Test
  public void testDeSerializingWithJsonCreator() throws IOException {

    String jsonString = "{\"start\": 1, \"length\": 2, \"path\": \"/tmp/drill/test\"}";

    SimpleModule module = new SimpleModule();
    module.addSerializer(Path.class, new PathSerDe.Se());
    ObjectMapper testMapper = JacksonUtils.createObjectMapper();
    testMapper.registerModule(module);

    CompleteFileWork.FileWorkImpl bean = testMapper.readValue(jsonString, CompleteFileWork.FileWorkImpl.class);

    assertThat(bean.getStart() == 1,  equalTo( true ));
    assertThat(bean.getLength() == 2, equalTo( true ));
    assertThat(bean.getPath().equals(new Path("/tmp/drill/test")), equalTo( true ));
  }

  @Test
  public void testHadoopPathSerDe() throws IOException {
    CompleteFileWork.FileWorkImpl fileWork = new CompleteFileWork.FileWorkImpl(5, 6, new Path("/tmp"));
    SimpleModule module = new SimpleModule();
    module.addSerializer(Path.class, new PathSerDe.Se());
    ObjectMapper testMapper = JacksonUtils.createObjectMapper();
    testMapper.registerModule(module);

    CompleteFileWork.FileWorkImpl bean =
        testMapper.readValue(testMapper.writeValueAsString(fileWork), CompleteFileWork.FileWorkImpl.class);

    assertThat(bean.getStart() == 5,  equalTo( true ));
    assertThat(bean.getLength() == 6, equalTo( true ));
    assertThat(bean.getPath().equals(new Path("/tmp")), equalTo( true ));
  }
}
