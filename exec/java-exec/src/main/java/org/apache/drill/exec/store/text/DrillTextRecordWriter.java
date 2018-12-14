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
package org.apache.drill.exec.store.text;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.store.StorageStrategy;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.store.StringOutputRecordWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.drill.shaded.guava.com.google.common.base.Joiner;

public class DrillTextRecordWriter extends StringOutputRecordWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillTextRecordWriter.class);

  private final StorageStrategy storageStrategy;

  private Path cleanUpLocation;

  private String location;
  private String prefix;

  private String fieldDelimiter;
  private String extension;

  private static String eol = System.getProperty("line.separator");
  private int index;
  private PrintStream stream = null;
  private FileSystem fs = null;

  // Record write status
  private boolean fRecordStarted = false; // true once the startRecord() is called until endRecord() is called
  private StringBuilder currentRecord; // contains the current record separated by field delimiter

  private Configuration fsConf;

  public DrillTextRecordWriter(BufferAllocator allocator, StorageStrategy storageStrategy, Configuration fsConf) {
    super(allocator);
    this.storageStrategy = storageStrategy == null ? StorageStrategy.DEFAULT : storageStrategy;
    this.fsConf = new Configuration(fsConf);
  }

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {
    this.location = writerOptions.get("location");
    this.prefix = writerOptions.get("prefix");
    this.fieldDelimiter = writerOptions.get("separator");
    this.extension = writerOptions.get("extension");

    this.fs = FileSystem.get(fsConf);

    this.currentRecord = new StringBuilder();
    this.index = 0;
  }

  @Override
  public void startNewSchema(List<String> columnNames) throws IOException {
    // wrap up the current file
    cleanup();

    // open a new file for writing data with new schema
    Path fileName = new Path(location, prefix + "_" + index + "." + extension);
    try {
      // drill text writer does not support partitions, so only one file can be created
      // and thus only one location should be deleted in case of abort
      // to ensure that our writer was the first to create output file,
      // we create empty output file first and fail if file exists
      cleanUpLocation = storageStrategy.createFileAndApply(fs, fileName);

      // since empty output file will be overwritten (some file systems may restrict append option)
      // we need to re-apply file permission
      DataOutputStream fos = fs.create(fileName);
      storageStrategy.applyToFile(fs, fileName);

      stream = new PrintStream(fos);
      logger.debug("Created file: {}", fileName);
    } catch (IOException ex) {
      logger.error("Unable to create file: " + fileName, ex);
      throw ex;
    }
    index++;

    stream.println(Joiner.on(fieldDelimiter).join(columnNames));
  }

  @Override
  public void addField(int fieldId, String value) throws IOException {
    currentRecord.append(value + fieldDelimiter);
  }

  @Override
  public void startRecord() throws IOException {
    if (fRecordStarted) {
      throw new IOException("Previous record is not written completely");
    }

    fRecordStarted = true;
  }

  @Override
  public void endRecord() throws IOException {
    if (!fRecordStarted) {
      throw new IOException("No record is in writing");
    }

    // remove the extra delimiter at the end
    currentRecord.deleteCharAt(currentRecord.length()-fieldDelimiter.length());

    stream.println(currentRecord.toString());

    // reset current record status
    currentRecord.delete(0, currentRecord.length());
    fRecordStarted = false;
  }

  @Override
  public FieldConverter getNewMapConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ComplexStringFieldConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewRepeatedMapConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ComplexStringFieldConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewRepeatedListConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ComplexStringFieldConverter(fieldId, fieldName, reader);
  }

  public class ComplexStringFieldConverter extends FieldConverter {

    public ComplexStringFieldConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeField() throws IOException {
      addField(fieldId, reader.readObject().toString());
    }
  }

  @Override
  public void cleanup() throws IOException {
    super.cleanup();
    if (stream != null) {
      stream.close();
      stream = null;
      logger.debug("closing file");
    }
  }

  @Override
  public void abort() throws IOException {
    if (cleanUpLocation != null) {
      fs.delete(cleanUpLocation, true);
      logger.info("Aborting writer. Location [{}] on file system [{}] is deleted.",
          cleanUpLocation.toUri().getPath(), fs.getUri());
    }
  }

}
