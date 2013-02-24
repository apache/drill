package org.apache.drill.exec.ref;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.rse.JSONRecordReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestUtils {
  public static RecordIterator jsonToRecordIterator(String schemaPath, String j) throws IOException {
    InputStream is = new ByteArrayInputStream(j.getBytes());
    JSONRecordReader reader = new JSONRecordReader(new SchemaPath(schemaPath), DrillConfig.create(), is, null);
    return reader.getIterator();
  }

  public static int getIteratorCount(RecordIterator out) {
    RecordIterator.NextOutcome next;
    int counter = 0;
    while((next = out.next()) != RecordIterator.NextOutcome.NONE_LEFT) {
      counter++;
      //RecordPointer rp = out.getRecordPointer();
      //System.out.println(rp);
    }
    return counter;
  }
}
