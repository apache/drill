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
package org.apache.drill.exec.store.pmml;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileDescrip;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.udfs.ml.MLUtils;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.hadoop.fs.Path;
import org.pmml4s.data.DataVal;
import org.pmml4s.metadata.Field;
import org.pmml4s.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads a PMML file as a single row describing the model it contains. Nothing is scored here;
 * scoring is the {@code predict()} function in {@code org.apache.drill.exec.udfs.ml}.
 */
public class PmmlBatchReader implements ManagedReader {

  private static final Logger logger = LoggerFactory.getLogger(PmmlBatchReader.class);
  /** A model file is XML metadata, not data. Anything larger is not a model. */
  private static final int MAX_MODEL_BYTES = 64 * 1024 * 1024;

  private final CustomErrorContext errorContext;
  private final Path path;
  private final Model model;
  private final RowSetLoader rowWriter;
  private boolean emitted;

  public PmmlBatchReader(FileSchemaNegotiator negotiator) {
    this.errorContext = negotiator.errorContext();
    FileDescrip file = negotiator.file();
    this.path = file.split().getPath();
    this.model = readModel(file);
    negotiator.tableSchema(buildSchema(), true);
    ResultSetLoader loader = negotiator.build();
    this.rowWriter = loader.writer();
  }

  private static TupleMetadata buildSchema() {
    return new SchemaBuilder()
      .add("model_name", MinorType.VARCHAR, DataMode.OPTIONAL)
      .add("model_type", MinorType.VARCHAR, DataMode.OPTIONAL)
      .add("classification", MinorType.BIT)
      .add("clustering", MinorType.BIT)
      .add("regression", MinorType.BIT)
      .add("input_fields", MinorType.VARCHAR, DataMode.REPEATED)
      .add("prediction_classes", MinorType.VARCHAR, DataMode.REPEATED)
      .addMapArray("input_schema")
        .addNullable("field_name", MinorType.VARCHAR)
        .addNullable("data_type", MinorType.VARCHAR)
      .resumeSchema()
      .buildSchema();
  }

  @Override
  public boolean next() {
    if (emitted) {
      return false;
    }
    emitted = true;
    rowWriter.start();

    setNullableString("model_name", model.modelName().isDefined()
      ? model.modelName().get() : null);
    setNullableString("model_type", model.modelElement() == null
      ? null : model.modelElement().toString());
    rowWriter.scalar("classification").setBoolean(model.isClassification());
    rowWriter.scalar("clustering").setBoolean(model.isClustering());
    rowWriter.scalar("regression").setBoolean(model.isRegression());

    for (Field field : model.inputFields()) {
      rowWriter.array("input_fields").scalar().setString(field.name());
    }
    // A regression model has no classes; pmml4s returns an empty array rather than null.
    for (DataVal value : model.classes()) {
      rowWriter.array("prediction_classes").scalar().setString(String.valueOf(value));
    }
    for (Field field : model.inputFields()) {
      TupleWriter entry = rowWriter.array("input_schema").tuple();
      entry.scalar("field_name").setString(field.name());
      entry.scalar("data_type").setString(String.valueOf(field.dataType()));
      rowWriter.array("input_schema").save();
    }

    rowWriter.save();
    return false;
  }

  private void setNullableString(String column, String value) {
    if (value == null) {
      rowWriter.scalar(column).setNull();
    } else {
      rowWriter.scalar(column).setString(value);
    }
  }

  @Override
  public void close() {
    // The stream is closed in readModel; the parsed model holds no resources.
  }

  /**
   * Reads the whole file and screens it before pmml4s sees it. pmml4s builds its own
   * {@code XMLInputFactory} with default settings, which resolves external entities, so a model
   * carrying a DOCTYPE could read local files and make the Drillbit fetch URLs. The same guard
   * runs in {@code MLUtils} for the {@code predict()} function.
   */
  private Model readModel(FileDescrip file) {
    byte[] bytes;
    try (InputStream in = file.fileSystem().openPossiblyCompressedStream(path)) {
      bytes = readFully(in);
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .message("Unable to open model file %s", path)
        .addContext(errorContext)
        .build(logger);
    }
    if (MLUtils.hasDoctype(bytes)) {
      throw UserException.dataReadError()
        .message("Unable to read %s as a PMML model: it declares a DOCTYPE. A model file must "
          + "not contain a DOCTYPE declaration, because a DTD can read local files and make the "
          + "server fetch URLs.", path)
        .addContext(errorContext)
        .build(logger);
    }
    try {
      return Model.fromInputStream(new ByteArrayInputStream(bytes));
    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .message("Unable to parse %s as a PMML model", path)
        .addContext(errorContext)
        .build(logger);
    }
  }

  private byte[] readFully(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int read;
    while ((read = in.read(buffer)) != -1) {
      out.write(buffer, 0, read);
      if (out.size() > MAX_MODEL_BYTES) {
        throw new IOException("Model file exceeds the " + (MAX_MODEL_BYTES / (1024 * 1024))
          + " MB limit for a PMML model");
      }
    }
    return out.toByteArray();
  }
}
