package org.apache.drill.exec.store.msgpack.valuewriter;

import java.util.Stack;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.MsgpackReaderContext;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

public abstract class AbstractValueWriter implements ValueWriter {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractValueWriter.class);

  protected MsgpackReaderContext context;

  private Stack<MaterializedField> stack = new Stack<MaterializedField>();

  public AbstractValueWriter() {
    super();
  }

  public void setup(MsgpackReaderContext context) {
    this.context = context;
  }

  public String printPath(MapWriter mapWriter, ListWriter listWriter) {
    FieldWriter f = null;
    if (mapWriter != null) {
      f = (FieldWriter) mapWriter;
    } else {
      f = (FieldWriter) listWriter;
    }
    while (f != null) {
      MaterializedField field = f.getField();
      stack.push(field);
      f = f.getParent();
    }
    StringBuilder s = new StringBuilder();
    int indent = 0;
    while (stack.peek() != null) {
      for (int i = 0; i < indent; i++) {
        s.append("\t");
      }
      s.append(stack.pop());
      s.append("\n");
      indent++;
    }
    return s.toString();
  }
}
