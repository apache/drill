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

import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;

import org.apache.commons.io.IOUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeRuntimeException;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.proto.UserBitShared.SerializedField.Builder;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class MsgpackSchema {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackSchema.class);

  public static final String SCHEMA_FILE_NAME = ".schema.proto";
  private final MsgpackSchemaWriter schemaWriter = new MsgpackSchemaWriter();
  private final DrillFileSystem dfs;
  private final Path schemaLocation;
  private MaterializedField schema = null;

  public MsgpackSchema(DrillFileSystem dfs, Path dirLocation) {
    this.dfs = dfs;
    this.schemaLocation = new Path(dirLocation, SCHEMA_FILE_NAME);
  }

  public MsgpackSchema delete() throws IOException {
    dfs.delete(schemaLocation, false);
    schema = null;
    return this;
  }

  public MsgpackSchema learnSchema(VectorContainerWriter writer) throws Exception {
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

  public MsgpackSchema applySchemaIfAny(VectorContainerWriter writer) throws Exception {
    load();
    if (schema != null) {
      logger.debug("Applying schema to fill in missing fields.");
      schemaWriter.applySchema(schema, writer);
    }
    return this;
  }

  public MaterializedField getSchema() {
    return schema;
  }

  public MsgpackSchema save(MaterializedField mapField) throws IOException {
    try (FSDataOutputStream out = dfs.create(schemaLocation, true)) {
      SerializedField serializedMapField = mapField.getSerializedField();
      String data = TextFormat.printToString(serializedMapField);
      IOUtils.write(data, out);
    }
    this.schema = mapField;
    return this;
  }

  public MsgpackSchema load() throws Exception {
    MaterializedField previousMapField = null;
    if (schemaLocation != null && dfs.exists(schemaLocation)) {
      try (FSDataInputStream in = dfs.open(schemaLocation)) {
        String schemaData = IOUtils.toString(in);
        Builder newBuilder = SerializedField.newBuilder();
        try {
          TextFormat.merge(schemaData, newBuilder);
        } catch (ParseException e) {
          throw new DrillRuntimeException("Failed to merge schema files: " + schemaLocation, e);
        }
        SerializedField read = newBuilder.build();
        previousMapField = MaterializedField.create(read);
      }
    }
    schema = previousMapField;
    return this;
  }

  public MaterializedField merge(MaterializedField existingField, MaterializedField newField) {
    if (existingField.getType().getMinorType() != MinorType.MAP) {
      return newField;
    }

    Preconditions.checkArgument(existingField.hasSameTypeAndMode(newField),
        "Field " + existingField + " and " + newField + " not same.");

    MaterializedField merged = existingField.clone();
    privateMerge(merged, newField);
    return merged;
  }

  private void privateMerge(MaterializedField existingField, MaterializedField newField) {
    Preconditions.checkArgument(existingField.getType().getMinorType() == MinorType.MAP,
        "Field " + existingField + " is not a MAP type.");
    for (MaterializedField newChild : newField.getChildren()) {
      String newChildName = newChild.getName();
      MaterializedField foundExistingChild = getFieldByName(newChildName, existingField);
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
        existingField.addChild(newChild.clone());
      }
    }
  }

  private MaterializedField getFieldByName(String newChildName, MaterializedField existingField) {
    for (MaterializedField f : existingField.getChildren()) {
      if (newChildName.equalsIgnoreCase(f.getName())) {
        return f;
      }
    }
    return null;
  }

}
