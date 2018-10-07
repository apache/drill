package org.apache.drill.exec.store.msgpack;

import java.io.IOException;
import java.io.InputStream;

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
  protected boolean skipMalformedMsgRecords;

  public ReadState write(ComplexWriter writer) throws IOException {
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
        readState = writeRecord(v, writer);
        break;
      default:
        Log.warn("Value in root of message pack file is not of type MAP. Skipping type found: " + type);
        readState = ReadState.MSG_RECORD_PARSE_ERROR;
      }
    }

    return readState;
  }

  protected abstract ReadState writeRecord(Value mapValue, ComplexWriter writer) throws IOException;

  public abstract void ensureAtLeastOneField(ComplexWriter writer);

  public void setSource(InputStream stream) {
    unpacker = MessagePack.newDefaultUnpacker(stream);
  }

  public void setIgnoreMsgParseErrors(boolean skipMalformedMsgRecords) {
    this.skipMalformedMsgRecords = skipMalformedMsgRecords;
  }

}
