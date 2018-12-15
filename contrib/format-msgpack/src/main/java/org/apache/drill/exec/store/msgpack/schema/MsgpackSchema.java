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
package org.apache.drill.exec.store.msgpack.schema;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeRuntimeException;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.AbstractColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * <p>
 * This class handles loading, merging and saving the schema of msgpack files.
 * Note, at the moment this class does not handle concurency. In "learning" mode
 * the user is expected to issue "select * from `datafile.mp`" statements until
 * the schema is satisfactory. What this class handles is merging schema between
 * batches of that single file.
 * </p>
 * <p>
 * The merging is not sofisticated and only works if the types are the same. It
 * does not handle promoting an INTEGER to FLAOT for example. However you
 * probably want to run in "lenient" mode so that if a DOUBLE is encoutered
 * after the reader has learned it was an INTEGER it will simply skip over it
 * and not fail processing any other fields it might discover. The same applies
 * for arrays of INTEGER but then sees some arrays with DOUBLE values, it will
 * just skip them.
 * </p>
 * <p>
 * After learing the model logs can be analized the see if anything got skipped
 * and why. You can then manually modify the schema file to get the desired
 * affect. For example coercing the values to DOUBLE.
 * </p>
 * <p>
 * Another example is an array of STRING, DOUBLE, BINARY values. The discovered
 * schema would be STRING but you can manually change that to BINARY so that all
 * types of values coereced into BINARY are stored in the array.
 * </p>
 */
public class MsgpackSchema {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackSchema.class);

  public static final String SCHEMA_FILE_NAME = ".schema.json";
  private final MsgpackSchemaWriter schemaWriter = new MsgpackSchemaWriter();
  private final DrillFileSystem dfs;
  private final Path schemaLocation;
  private MaterializedField schema = null;

  public MsgpackSchema(DrillFileSystem dfs, Path dirLocation) {
    this.dfs = dfs;
    this.schemaLocation = new Path(dirLocation, SCHEMA_FILE_NAME);
  }

  /**
   * Delete the schema file on disk.
   *
   * @return
   * @throws IOException
   */
  public MsgpackSchema delete() throws IOException {
    dfs.delete(schemaLocation, false);
    schema = null;
    return this;
  }

  /**
   * In "learning" mode this method is called after a batch is read from the
   * msgpack file. The drill vector writer contains the schema of that batch. This
   * method will extract the schema from it and merge it with any prior schema it
   * had.
   *
   * @param writer
   * @return
   * @throws Exception
   */
  public MsgpackSchema learnSchema(VectorContainerWriter writer) throws Exception {
    // Load any previous schema if any.
    load();
    if (schema != null) {
      logger.debug("Found previous schema. Merging.");
      MaterializedField current = writer.getMapVector().getField();
      MaterializedField merged = merge(schema, current);
      logger.debug("Saving {} merged schema content is: {}", schemaLocation, merged);
      save(merged);
    } else {
      MaterializedField current = writer.getMapVector().getField();
      logger.debug("Saving {} schema content is: {}", schemaLocation, current);
      save(current);
    }
    return this;
  }

  /**
   * When in "useSchema" mode this method is called after a batch is read from the
   * msgpack file. The writer has the schema of that batch but it might not have
   * encoutered all the possible fields. This method will use the desired schema
   * from disk and apply it to the writer. Filling in any missing field in the
   * schema of the writer. Effectively making sure that the schema of the writer
   * is always the same accross batches.
   *
   * @param writer
   * @return
   * @throws Exception
   */
  public MsgpackSchema applySchema(VectorContainerWriter writer) throws Exception {
    load();
    if (schema != null) {
      logger.debug("Applying schema to fill in missing fields.");
      schemaWriter.applySchema(schema, writer);
    }
    return this;
  }

  /**
   * Get the current state of the desired schema.
   *
   * @return
   */
  public MaterializedField getSchema() {
    return schema;
  }

  public TupleMetadata getTupleMetadata() {
    if (schema == null) {
      return null;
    }
    AbstractColumnMetadata s = MetadataUtils.fromField(schema);
    if (!s.isMap()) {
      return null;
    }
    return s.mapSchema();
  }

  /**
   * Save the current state of the schema to disk.
   *
   * @param mapField
   * @return
   * @throws IOException
   */
  public MsgpackSchema save(MaterializedField mapField) throws IOException {
    try (FSDataOutputStream out = dfs.create(schemaLocation, true);
        OutputStreamWriter writer = new OutputStreamWriter(out)) {
      JsonObject json = materializedFieldToJson(mapField);
      Gson gson = new Gson();
      gson.toJson(json, writer);
    }
    this.schema = mapField;
    return this;
  }

  /**
   * Load the schema from disk.
   */
  public MsgpackSchema load() throws Exception {
    MaterializedField newlyLoadedSchema = null;
    if (schemaLocation != null && dfs.exists(schemaLocation)) {
      try (FSDataInputStream in = dfs.open(schemaLocation); Reader reader = new InputStreamReader(in)) {
        Gson gson = new Gson();
        JsonElement json = gson.fromJson(reader, JsonElement.class);
        newlyLoadedSchema = jsonToMaterializedField(json.getAsJsonObject());
      }
    }
    schema = newlyLoadedSchema;
    return this;
  }

  /**
   * Merge two schema together.
   *
   * @param currentSchema
   * @param newSchema
   * @return
   */
  public MaterializedField merge(MaterializedField currentSchema, MaterializedField newSchema) {
    Preconditions.checkArgument(currentSchema.hasSameTypeAndMode(newSchema),
        "Field " + currentSchema + " and " + newSchema + " not same.");

    MaterializedField merged = currentSchema.clone();
    return privateMerge(merged, newSchema);
  }

  private MaterializedField privateMerge(MaterializedField currentSchema, MaterializedField newSchema) {
    if (currentSchema.getType().getMinorType() != MinorType.MAP) {
      return newSchema;
    }

    for (MaterializedField newChild : newSchema.getChildren()) {
      String newChildName = newChild.getName();
      MaterializedField foundExistingChild = getFieldByName(newChildName, currentSchema);
      if (foundExistingChild != null) {
        if (foundExistingChild.hasSameTypeAndMode(newChild)) {
          if (foundExistingChild.getType().getMinorType() == MinorType.MAP) {
            privateMerge(foundExistingChild, newChild);
          } // else we already have it
        } else {
          // error
          throw new SchemaChangeRuntimeException("Not the same schema for " + foundExistingChild + " and " + newChild);
        }
      } else {
        currentSchema.addChild(newChild.clone());
      }
    }
    return currentSchema;
  }

  private MaterializedField getFieldByName(String newChildName, MaterializedField existingField) {
    for (MaterializedField f : existingField.getChildren()) {
      if (newChildName.equalsIgnoreCase(f.getName())) {
        return f;
      }
    }
    return null;
  }

  private MaterializedField jsonToMaterializedField(JsonObject json) {
    String name = json.get("name").getAsString();
    String mode = json.get("mode").getAsString();
    String type = json.get("type").getAsString();
    MajorType majorType = MajorType.newBuilder().setMode(DataMode.valueOf(mode)).setMinorType(MinorType.valueOf(type))
        .build();
    MaterializedField materializedField = MaterializedField.create(name, majorType);
    JsonArray asJsonArray = json.getAsJsonArray("child");
    if (asJsonArray != null && asJsonArray.size() > 0) {
      for (JsonElement jsonElement : asJsonArray) {
        JsonObject asJsonObject = jsonElement.getAsJsonObject();
        MaterializedField child = jsonToMaterializedField(asJsonObject);
        materializedField.addChild(child);
      }
    }
    return materializedField;
  }

  private JsonObject materializedFieldToJson(MaterializedField f) {
    JsonObject o = new JsonObject();
    o.addProperty("name", f.getName());
    o.addProperty("mode", f.getDataMode().toString());
    o.addProperty("type", f.getType().getMinorType().toString());
    Collection<MaterializedField> children = f.getChildren();

    if (children != null && children.size() > 0) {
      List<MaterializedField> list = new ArrayList<>();
      list.addAll(children);
      list.sort(new Comparator<MaterializedField>() {
        public int compare(MaterializedField a, MaterializedField b) {
          return a.getName().compareTo(b.getName());
        }
      });

      JsonArray jsonArray = new JsonArray();
      for (MaterializedField child : list) {
        jsonArray.add(materializedFieldToJson(child));
      }
      o.add("child", jsonArray);
    }
    return o;
  }

}
