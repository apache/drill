package org.apache.drill.exec.ref.rops;

import static org.junit.Assert.*;

import org.apache.drill.exec.ref.*;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.drill.common.logical.data.Limit;


public class LimitROPTest {
  final String input = "" +
      "{id: 1, name: \"jim\"}" +
      "{id: 2, name: \"bob\"}" +
      "{id: 3, name: \"larry\"}" +
      "{id: 4, name: \"fred\"}";

  private LimitROP buildROP(Limit l) {
    LimitROP limitROP = new LimitROP(l);
    limitROP.setupEvals(new BasicEvaluatorFactory(new IteratorRegistry()));
    return limitROP;
  }

  @Test
  public void limitStart() throws IOException {
    RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
    LimitROP limitROP = buildROP(new Limit(0, 2));
    limitROP.setInput(incoming);

    int nRecords = TestUtils.getIteratorCount(limitROP.getOutput());
    assertEquals(2, nRecords);
  }

  @Test
  public void limitMiddle() throws IOException {
    RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
    LimitROP limitROP = buildROP(new Limit(1, 2));
    limitROP.setInput(incoming);

    int nRecords = TestUtils.getIteratorCount(limitROP.getOutput());
    assertEquals(1, nRecords);
  }

  @Test
  public void limitEnd() throws IOException {
    RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
    LimitROP limitROP = buildROP(new Limit(3, 10));
    limitROP.setInput(incoming);

    int nRecords = TestUtils.getIteratorCount(limitROP.getOutput());
    assertEquals(1, nRecords);
  }

  @Test
  public void limitZeroLength() throws IOException {
    RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
    LimitROP limitROP = buildROP(new Limit(1, 1));
    limitROP.setInput(incoming);

    int nRecords = TestUtils.getIteratorCount(limitROP.getOutput());
    assertEquals(0, nRecords);
  }
}
