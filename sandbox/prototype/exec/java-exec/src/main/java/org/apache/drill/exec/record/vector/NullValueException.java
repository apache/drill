package org.apache.drill.exec.record.vector;

import org.apache.drill.common.exceptions.DrillRuntimeException;

public class NullValueException extends DrillRuntimeException {
  public NullValueException(int index) {
    super("Element at index position: " + index + " is null");
  }
}
