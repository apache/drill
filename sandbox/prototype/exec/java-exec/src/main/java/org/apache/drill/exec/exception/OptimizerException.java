package org.apache.drill.exec.exception;

import org.apache.drill.common.exceptions.DrillException;

public class OptimizerException extends DrillException {
  public OptimizerException(String message, Throwable cause) {
    super(message, cause);
  }

  public OptimizerException(String s) {
        super(s);
    }
}
