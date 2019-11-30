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
package org.apache.drill.exec.store.json;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonOptions;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.SubOperatorTest;

public abstract class BaseTestJsonLoader extends SubOperatorTest {

  static class JsonTester {
    public OptionBuilder loaderOptions = new OptionBuilder();
    private final JsonOptions options;
    private ResultSetLoader tableLoader;

    public JsonTester(JsonOptions options) {
      this.options = options;
    }

    public JsonTester() {
      this(new JsonOptions());
    }

    public RowSet parseFile(String resourcePath) {
      try {
        return parse(ClusterFixture.getResource(resourcePath));
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public RowSet parse(String json) {
      tableLoader = new ResultSetLoaderImpl(fixture.allocator(),
          loaderOptions.build());
      InputStream inStream = new
          ReaderInputStream(new StringReader(json));
      options.context = "test Json";
      JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);
      readBatch(tableLoader, loader);
      try {
        inStream.close();
      } catch (IOException e) {
        fail();
      }
      loader.close();
      return DirectRowSet.fromContainer(tableLoader.harvest());
    }

    public void close() {
      tableLoader.close();
    }
  }

  protected static class MultiBatchJson {

    private final ResultSetLoader tableLoader;
    private final InputStream inStream;
    private final JsonLoader loader;

    public MultiBatchJson(JsonOptions options, String json) {
      options.context = "test Json";
      inStream = new
          ReaderInputStream(new StringReader(json));
      tableLoader = new ResultSetLoaderImpl(fixture.allocator());
      loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);
    }

    public MultiBatchJson(String json) {
      this(new JsonOptions(), json);
    }

    public RowSet parse(int rowCount) {
      readBatch(tableLoader, loader, rowCount);
      return fixture.wrap(tableLoader.harvest());
    }

    public RowSet parse() {
      readBatch(tableLoader, loader);
      return fixture.wrap(tableLoader.harvest());
    }

    public void close() {
      try {
        inStream.close();
      } catch (IOException e) {
        fail();
      }
      loader.close();
      tableLoader.close();
    }
  }

  protected static boolean readBatch(ResultSetLoader tableLoader, JsonLoader loader) {
    tableLoader.startBatch();
    RowSetLoader writer = tableLoader.writer();
    boolean more = false;
    while (writer.start()) {
      more = loader.next();
      if (! more) {
        break;
      }
      writer.save();
    }
    loader.endBatch();
    return more;
  }

  protected static void readBatch(ResultSetLoader tableLoader, JsonLoader loader, int count) {
    tableLoader.startBatch();
    RowSetLoader writer = tableLoader.writer();
    for (int i = 0; i < count; i++) {
      writer.start();
      assertTrue(loader.next());
      writer.save();
    }
    loader.endBatch();
  }

  static void expectError(JsonTester tester, String json) {
    try {
      tester.parse(json);
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("test Json"));
    }
    tester.close();
  }

  protected JsonTester jsonTester() {
    return new JsonTester();
  }

  protected JsonTester jsonTester(JsonOptions options) {
    return new JsonTester(options);
  }

  protected void expectError(String json) {
    JsonTester tester = jsonTester();
    expectError(tester, json);
  }

}
