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
import org.apache.drill.exec.record.metadata.AbstractColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

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

  public static final String SCHEMA_FILE_NAME = ".schema.proto";
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
    try (FSDataOutputStream out = dfs.create(schemaLocation, true)) {
      SerializedField serializedMapField = mapField.getSerializedField();
      String data = TextFormat.printToString(serializedMapField);
      IOUtils.write(data, out);
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
      try (FSDataInputStream in = dfs.open(schemaLocation)) {
        String schemaData = IOUtils.toString(in);
        Builder newBuilder = SerializedField.newBuilder();
        try {
          // Calling merge here is strange but that's the only way I found out how to read
          // the schema from disk.
          TextFormat.merge(schemaData, newBuilder);
        } catch (ParseException e) {
          throw new DrillRuntimeException("Failed to merge schema files: " + schemaLocation, e);
        }
        SerializedField read = newBuilder.build();
        newlyLoadedSchema = MaterializedField.create(read);
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
    // TODO look at using the TupleSchema instead.
    for (MaterializedField f : existingField.getChildren()) {
      if (newChildName.equalsIgnoreCase(f.getName())) {
        return f;
      }
    }
    return null;
  }

}
