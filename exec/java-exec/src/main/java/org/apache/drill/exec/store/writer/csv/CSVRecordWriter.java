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
package org.apache.drill.exec.store.writer.csv;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import org.apache.drill.exec.store.StringOutputRecordWriter;
import org.apache.drill.exec.store.writer.RecordWriterTemplate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@RecordWriterTemplate(format = "csv")
public class CSVRecordWriter extends StringOutputRecordWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CSVRecordWriter.class);

  private String location;      // directory where to write the CSV files
  private String prefix;        // prefix to output file names.
  private int index;

  private PrintStream stream = null;
  private FileSystem fs = null;

  // Record write status
  private boolean fRecordStarted = false; // true once the startRecord() is called until endRecord() is called
  private StringBuilder currentRecord;    // contains the current record separated by commas

  private static String eol = System.getProperty("line.separator");

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {
    this.location = writerOptions.get("location");
    this.prefix = writerOptions.get("prefix");
    this.index = 0;

    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, writerOptions.get(FileSystem.FS_DEFAULT_NAME_KEY));
    this.fs = FileSystem.get(conf);

    currentRecord = new StringBuilder();
  }

  @Override
  public void startNewSchema(List<String> columnNames) throws IOException {
    // wrap up the current file
    cleanup();

    // open a new file for writing data with new schema
    Path fileName = new Path(location, prefix + "_" + index + ".csv");
    try {
      DataOutputStream fos = fs.create(fileName);
      stream = new PrintStream(fos);
      logger.debug("CSVWriter: created file: {}", fileName);
    } catch (IOException ex) {
      logger.error("Unable to create file: " + fileName, ex);
      throw ex;
    }
    index++;

    stream.println(Joiner.on(",").join(columnNames));
  }

  @Override
  public void addField(int fieldId, String value) throws IOException {
    currentRecord.append(value + ",");
  }

  @Override
  public void startRecord() throws IOException {
    if (fRecordStarted)
      throw new IOException("Previous record is not written completely");

    fRecordStarted = true;
  }

  @Override
  public void endRecord() throws IOException {
    if (!fRecordStarted)
      throw new IOException("No record is in writing");

    // remove the extra "," at the end
    currentRecord.deleteCharAt(currentRecord.length()-1);

    stream.println(currentRecord.toString());

    // reset current record status
    currentRecord.delete(0, currentRecord.length());
    fRecordStarted = false;
  }

  @Override
  public void cleanup() throws IOException {
    super.cleanup();
    if (stream != null) {
      stream.close();
      stream = null;
      logger.debug("CSVWriter: closing file");
    }
  }

  @Override
  public void abort() throws IOException {
    cleanup();
    try {
      fs.delete(new Path(location), true);
    } catch (IOException ex) {
      logger.error("Abort failed. There could be leftover output files");
      throw ex;
    }
  }
}