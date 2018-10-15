package org.apache.drill.exec.store.msgpack;

import java.io.IOException;
import java.io.InputStream;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.msgpack.core.MessageInsufficientBufferException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import jline.internal.Log;

public abstract class BaseMsgpackReader {
  // @formatter:off
  public static enum ReadState {
    END_OF_STREAM,
    MSG_RECORD_PARSE_ERROR,
    MSG_RECORD_PARSE_EOF_ERROR,
    WRITE_SUCCEED
  }
  // @formatter:on

  protected MessageUnpacker unpacker;
  protected MsgpackReaderContext context;

  public BaseMsgpackReader(InputStream stream, MsgpackReaderContext context) {
    this.context = context;
    this.unpacker = MessagePack.newDefaultUnpacker(stream);
  }

  public ReadState write(ComplexWriter writer, MaterializedField schema) throws IOException {
    ReadState readState = ReadState.WRITE_SUCCEED;
    if (!unpacker.hasNext()) {
      readState = ReadState.END_OF_STREAM;
    }

    Value v = null;
    if (readState == ReadState.WRITE_SUCCEED) {
      try {
        v = unpacker.unpackValue();
      } catch (MessageInsufficientBufferException e) {
        Log.warn("Failed to unpack MAP, possibly because key/value tuples do not match.");
        readState = ReadState.MSG_RECORD_PARSE_ERROR;
      }
    }

    if (readState == ReadState.WRITE_SUCCEED) {
      ValueType type = v.getValueType();
      switch (type) {
      case MAP:
        readState = writeRecord(v, writer, schema);
        break;
      default:
        Log.warn("Value in root of message pack file is not of type MAP. Skipping type found: " + type);
        readState = ReadState.MSG_RECORD_PARSE_ERROR;
      }
    }

    return readState;
  }

  protected abstract ReadState writeRecord(Value mapValue, ComplexWriter writer, MaterializedField schema)
      throws IOException;

  public void ensureAtLeastOneField(ComplexWriter writer) {
  }
}
