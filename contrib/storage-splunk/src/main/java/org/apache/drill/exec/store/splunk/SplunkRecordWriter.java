package org.apache.drill.exec.store.splunk;

import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.AbstractRecordWriter;

import java.io.IOException;
import java.util.Map;

public class SplunkRecordWriter extends AbstractRecordWriter {
  /**
   * Initialize the writer.
   *
   * @param writerOptions Contains key, value pair of settings.
   * @throws IOException
   */
  @Override
  public void init(Map<String, String> writerOptions) throws IOException {

  }

  /**
   * Update the schema in RecordWriter. Called at least once before starting writing the records.
   *
   * @param batch
   * @throws IOException
   */
  @Override
  public void updateSchema(VectorAccessible batch) throws IOException {

  }

  /**
   * Called before starting writing fields in a record.
   *
   * @throws IOException
   */
  @Override
  public void startRecord() throws IOException {

  }

  /**
   * Called after adding all fields in a particular record are added using add{TypeHolder}
   * (fieldId, TypeHolder) methods.
   *
   * @throws IOException
   */
  @Override
  public void endRecord() throws IOException {

  }

  @Override
  public void abort() throws IOException {

  }

  @Override
  public void cleanup() throws IOException {

  }
}
