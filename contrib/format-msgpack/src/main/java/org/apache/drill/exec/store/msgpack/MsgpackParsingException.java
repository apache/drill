package org.apache.drill.exec.store.msgpack;

import org.apache.drill.common.exceptions.DrillRuntimeException;

public class MsgpackParsingException extends DrillRuntimeException {

  /**
   *
   */
  private static final long serialVersionUID = -6973175874851467807L;

  public MsgpackParsingException(String message) {
    super(message, null, false, false);
  }

  public MsgpackParsingException(String message, Exception e) {
    super(message, e, false, false);
  }

}
