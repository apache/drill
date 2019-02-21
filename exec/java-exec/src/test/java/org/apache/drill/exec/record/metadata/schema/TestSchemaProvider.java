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
package org.apache.drill.exec.record.metadata.schema;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StorageStrategy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestSchemaProvider {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testInlineProviderExists() throws Exception {
    SchemaProvider provider = new InlineSchemaProvider("(i int)", null);
    assertTrue(provider.exists());
  }

  @Test
  public void testInlineProviderDelete() throws Exception {
    SchemaProvider provider = new InlineSchemaProvider("(i int)", null);
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Schema deletion is not supported");
    provider.delete();
  }

  @Test
  public void testInlineProviderStore() throws Exception {
    SchemaProvider provider = new InlineSchemaProvider("(i int)", null);
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Schema storage is not supported");
    provider.store("i int", null, StorageStrategy.DEFAULT);
  }

  @Test
  public void testInlineProviderRead() throws Exception {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("k1", "v1");
    SchemaProvider provider = new InlineSchemaProvider("(i int)", properties);

    SchemaContainer schemaContainer = provider.read();
    assertNotNull(schemaContainer);

    assertNull(schemaContainer.getTable());
    TupleMetadata metadata = schemaContainer.getSchema();
    assertNotNull(metadata);
    assertEquals(1, metadata.size());
    assertEquals(TypeProtos.MinorType.INT, metadata.metadata("i").type());

    assertEquals(properties, schemaContainer.getProperties());

    SchemaContainer.Version version = schemaContainer.getVersion();
    assertFalse(version.isUndefined());
    assertEquals(SchemaContainer.Version.CURRENT_DEFAULT_VERSION, version.getValue());
  }

  @Test
  public void testPathProviderExists() throws Exception {
    File schema = new File(folder.getRoot(), "schema");
    SchemaProvider provider = new PathSchemaProvider(new org.apache.hadoop.fs.Path(schema.getPath()));
    assertFalse(provider.exists());

    assertTrue(schema.createNewFile());
    assertTrue(provider.exists());
  }

  @Test
  public void testPathProviderDelete() throws Exception {
    File schema = folder.newFile("schema");
    assertTrue(schema.exists());
    SchemaProvider provider = new PathSchemaProvider(new org.apache.hadoop.fs.Path(schema.getPath()));
    provider.delete();
    assertFalse(schema.exists());
  }

  @Test
  public void testPathProviderDeleteAbsentFile() throws Exception {
    File schema = new File(folder.getRoot(), "absent_file");
    SchemaProvider provider = new PathSchemaProvider(new org.apache.hadoop.fs.Path(schema.getPath()));
    assertFalse(schema.exists());
    provider.delete();
    assertFalse(schema.exists());
  }

  @Test
  public void testPathProviderStore() throws Exception {
    File schema = new File(folder.getRoot(), "schema");
    SchemaProvider provider = new PathSchemaProvider(new org.apache.hadoop.fs.Path(schema.getPath()));

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("k1", "v1");
    properties.put("k2", "v2");

    assertFalse(provider.exists());
    provider.store("i int, v varchar(10)", properties, StorageStrategy.DEFAULT);
    assertTrue(provider.exists());

    String expectedContent =
        "{\n"
      + "  \"schema\" : [\n"
      + "    \"`i` INT\",\n"
      + "    \"`v` VARCHAR(10)\"\n"
      + "  ],\n"
      + "  \"properties\" : {\n"
      + "    \"k1\" : \"v1\",\n"
      + "    \"k2\" : \"v2\"\n"
      + "  },\n"
      + "  \"version\" : 1\n"
      + "}";
    List<String> lines = Files.readAllLines(schema.toPath());
    assertEquals(expectedContent, String.join("\n", lines));
  }

  @Test
  public void testPathProviderStoreInExistingFile() throws Exception {
    File schemaFile = folder.newFile("schema");
    org.apache.hadoop.fs.Path schema = new org.apache.hadoop.fs.Path(schemaFile.getPath());
    SchemaProvider provider = new PathSchemaProvider(schema);
    assertTrue(provider.exists());

    thrown.expect(IOException.class);
    thrown.expectMessage("File already exists");

    provider.store("i int", null, StorageStrategy.DEFAULT);
  }

  @Test
  public void testPathProviderRead() throws Exception {
    Path schemaPath = folder.newFile("schema").toPath();
    Files.write(schemaPath, Collections.singletonList(
          "{  \n"
        + "   \"table\":\"tbl\",\n"
        + "   \"schema\":[  \n"
        + "      \"`i` INT\",\n"
        + "      \"`v` VARCHAR\"\n"
        + "   ],\n"
        + "   \"properties\" : {\n"
        + "      \"k1\" : \"v1\",\n"
        + "      \"k2\" : \"v2\"\n"
        + "   }\n"
        + "}\n"
    ));
    SchemaProvider provider = new PathSchemaProvider(new org.apache.hadoop.fs.Path(schemaPath.toUri().getPath()));
    assertTrue(provider.exists());
    SchemaContainer schemaContainer = provider.read();
    assertNotNull(schemaContainer);
    assertEquals("tbl", schemaContainer.getTable());

    TupleMetadata metadata = schemaContainer.getSchema();
    assertNotNull(metadata);
    assertEquals(2, metadata.size());
    assertEquals(TypeProtos.MinorType.INT, metadata.metadata("i").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, metadata.metadata("v").type());

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("k1", "v1");
    properties.put("k2", "v2");
    assertEquals(properties, schemaContainer.getProperties());

    assertTrue(schemaContainer.getVersion().isUndefined());
  }

  @Test
  public void testPathProviderReadAbsentFile() throws Exception {
    org.apache.hadoop.fs.Path schema = new org.apache.hadoop.fs.Path(new File(folder.getRoot(), "absent_file").getPath());
    SchemaProvider provider = new PathSchemaProvider(schema);
    assertFalse(provider.exists());

    thrown.expect(FileNotFoundException.class);

    provider.read();
  }

  @Test
  public void testPathProviderReadSchemaWithComments() throws Exception {
    Path schemaPath = folder.newFile("schema").toPath();
    Files.write(schemaPath, Collections.singletonList(
          "// my schema file start\n"
        + "{  \n"
        + "   \"schema\":[  // start columns list\n"
        + "      \"`i` INT\"\n"
        + "   ]\n"
        + "}\n"
        + "// schema file end\n"
        + "/* multiline comment */"
    ));

    SchemaProvider provider = new PathSchemaProvider(new org.apache.hadoop.fs.Path(schemaPath.toUri().getPath()));
    assertTrue(provider.exists());
    assertNotNull(provider.read());
  }

}
