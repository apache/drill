/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.common.exceptions;

import org.apache.drill.exec.proto.UserBitShared.DrillPBError;

public class DrillUserException extends DrillRuntimeException {

  /**
   * Builder class for DrillUserException. You can wrap an existing exception, in this case it will first check if
   * this exception is, or wraps, a DrillUserException. If it does then the builder will use the user exception as it is
   * (it will ignore the message passed to the constructor) and will add any additional context information to the
   * exception's context
   */
  public static class Builder extends UserExceptionContext {

    private final Throwable cause;
    private final String message;
    private final DrillPBError.ErrorType errorType;

    private final DrillUserException uex;

    /**
     * builds a system error that wrap an existing exception. If the exception is, or wraps, a DrillUserException it
     * won't be converted to a system error.
     *
     * We should never need to call this, it will be done automatically before the exception is sent to the client.
     *
     * @param cause exception to wrap into a system error
     */
    Builder(Throwable cause) {
      this(DrillPBError.ErrorType.SYSTEM, cause);
    }

    /**
     * builds a new user exception of the specified type, with a defined error message
     *
     * @param errorType user exception's type
     * @param format A format string
     * @param args Arguments referenced by the format specifiers in the format string.
     */
    public Builder(DrillPBError.ErrorType errorType, String format, Object... args) {
      this(errorType, null, format, args);
    }

    /**
     * wraps an existing exception inside a user exception. If the exception is, or wraps, a user exception
     * already, the builder will extract the original user exception and use it instead. If the builder creates a new
     * user exception it will use the passed exception message, otherwise the builder won't change the message of the
     * exception
     *
     * @param errorType user exception type that should be created if the passed exception isn't, or doesn't wrap a user exception
     * @param cause exception to wrap inside a user exception
     */
    public Builder(DrillPBError.ErrorType errorType, Throwable cause) {
      this(errorType, cause, null);
    }

    /**
     * wraps an existing exception inside a user exception. If the exception is, or wraps, a user exception
     * already, the builder will extract the original user exception and use it instead.
     * If the builder creates a new user exception it will use the passed exception message, otherwise the builder
     * won't change the message of the exception and will add the passed message to the context instead
     *
     * @param errorType user exception type that should be created if the passed exception isn't, or doesn't wrap a user exception
     * @param cause exception to wrap inside a user exception
     * @param format A format string
     * @param args Arguments referenced by the format specifiers in the format string.
     */
    public Builder(DrillPBError.ErrorType errorType, Throwable cause, String format, Object... args) {
      super(ErrorHelper.getExceptionContextOrNew(cause));
      this.cause = cause;

      if (format == null) {
        this.message = cause != null ? cause.getMessage() : null;
      } else {
        this.message = String.format(format, args);
      }

      //TODO handle the improbable case where cause is a SYSTEM exception ?
      uex = ErrorHelper.findWrappedUserException(cause);
      if (uex != null) {
        this.errorType = null;
        if (format != null) {
          // we won't change the exception's message, so add it to the context
          add(this.message);
        }
      } else {
        // we will create a new user exception
        this.errorType = errorType;
      }
    }

    public DrillUserException build() {

      if (uex != null) {
        return uex;
      }

      return new DrillUserException(this);
    }
  }

  private final DrillPBError.ErrorType errorType;

  private final UserExceptionContext context;

  protected DrillUserException(DrillPBError.ErrorType errorType, String message, Throwable cause) {
    super(message, cause);

    this.errorType = errorType;
    this.context = new UserExceptionContext();
  }

  private DrillUserException(Builder builder) {
    super(builder.message, builder.cause);
    this.errorType = builder.errorType;
    this.context = builder;
  }

  public UserExceptionContext getContext() {
    return context;
  }

  /**
   * generates the message that will be displayed to the client without the stack trace.
   *
   * @return non verbose error message
   */
  @Override
  public String getMessage() {
    return generateMessage();
  }

  /**
   *
   * @return the error message that was passed to the builder
   */
  public String getOriginalMessage() {
    return super.getMessage();
  }

  /**
   * generates the message that will be displayed to the client. The message also contains the stack trace.
   *
   * @return verbose error message
   */
  public String getVerboseMessage() {
    return generateMessage() + "\n\n" + ErrorHelper.buildCausesMessage(getCause());
  }

  /**
   * returns or creates a DrillPBError object corresponding to this user exception.
   *
   * @param verbose should the error object contain the verbose error message ?
   * @return protobuf error object
   */
  public DrillPBError getOrCreatePBError(boolean verbose) {
    String message = verbose ? getVerboseMessage() : getMessage();

    DrillPBError.Builder builder = DrillPBError.newBuilder();
    builder.setErrorType(errorType);
    builder.setErrorId(context.getErrorId());
    if (context.getEndpoint() != null) {
      builder.setEndpoint(context.getEndpoint());
    }
    builder.setMessage(message);

    if (getCause() != null) {
      // some unit tests use this information to make sure a specific exception was thrown in the server
      builder.setException(ErrorHelper.getWrapper(getCause()));
    }
    return builder.build();
  }

  /**
   * Generates a user error message that has the following structure:
   * ERROR TYPE: ERROR_MESSAGE
   * CONTEXT
   * [ERROR_ID on DRILLBIT_IP:DRILLBIT_USER_PORT]
   *
   * @return generated user error message
   */
  private String generateMessage() {
    return errorType + " ERROR: " + super.getMessage() + "\n" +
      context.generateContextMessage();
  }

}
