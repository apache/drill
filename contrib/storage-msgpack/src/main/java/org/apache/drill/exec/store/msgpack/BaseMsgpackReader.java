package org.apache.drill.exec.store.msgpack;

import java.io.IOException;
import java.io.InputStream;

import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

public abstract class BaseMsgpackReader {
  public static enum ReadState {
    END_OF_STREAM, JSON_RECORD_PARSE_ERROR, JSON_RECORD_PARSE_EOF_ERROR, WRITE_SUCCEED
  }

  protected MessageUnpacker unpacker;

  public abstract ReadState write(ComplexWriter writer) throws IOException;

  public abstract void ensureAtLeastOneField(ComplexWriter writer);

  public void setSource(InputStream stream) {
    unpacker = MessagePack.newDefaultUnpacker(stream);
  }

}
