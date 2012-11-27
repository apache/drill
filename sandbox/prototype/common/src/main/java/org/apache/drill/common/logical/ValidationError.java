package org.apache.drill.common.logical;

public class ValidationError extends RuntimeException{

  public ValidationError() {
    super();
  }

  public ValidationError(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public ValidationError(String message, Throwable cause) {
    super(message, cause);
  }

  public ValidationError(String message) {
    super(message);
  }

  public ValidationError(Throwable cause) {
    super(cause);
  }

}
