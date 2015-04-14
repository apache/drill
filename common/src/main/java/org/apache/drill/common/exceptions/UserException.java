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

import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;

/**
 * Base class for all user exception. The goal is to separate out common error condititions where we can give users
 * useful feedback.
 * <p>Throwing a user exception will guarantee it's message will be displayed to the user, along with any context
 * information added to the exception at various levels while being sent to the client.
 * <p>A specific class of user exceptions are system exception. They represent system level errors that don't display
 * any specific error message to the user apart from "A system error has occurend" along with informations to retrieve
 * the details of the exception from the logs.
 * <p>although system exception should only display a generic message to the user, for now they will display the root
 * error message, until all user errors are properly sent from the server side.
 * <p>Any thrown exception that is not wrapped inside a user exception will automatically be converted to a system
 * exception before being sent to the client.
 *
 * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType
 */
public class UserException extends DrillRuntimeException {

  /**
   * wraps the passed exception inside a system error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build()} instead
   * of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#SYSTEM
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   *
   * @deprecated this method should never need to be used explicitely, unless you are passing the exception to the
   *             Rpc layer or UserResultListener.submitFailed()
   */
  @Deprecated
  public static Builder systemError(Throwable cause) {
    return new Builder(DrillPBError.ErrorType.SYSTEM, cause);
  }

  /**
   * creates a new user exception builder .
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#CONNECTION
   * @return user exception builder
   */
  public static Builder connectionError() {
    return connectionError(null);
  }

  /**
   * wraps the passed exception inside a connection error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build()} instead
   * of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#CONNECTION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder connectionError(Throwable cause) {
    return new Builder(DrillPBError.ErrorType.CONNECTION, cause);
  }

  /**
   * creates a new user exception builder .
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#DATA_READ
   * @return user exception builder
   */
  public static Builder dataReadError() {
    return dataReadError(null);
  }

  /**
   * wraps the passed exception inside a data read error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build()} instead
   * of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#DATA_READ
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder dataReadError(Throwable cause) {
    return new Builder(DrillPBError.ErrorType.DATA_READ, cause);
  }

  /**
   * creates a new user exception builder .
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#DATA_WRITE
   * @return user exception builder
   */
  public static Builder dataWriteError() {
    return dataWriteError(null);
  }

  /**
   * wraps the passed exception inside a data write error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build()} instead
   * of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#DATA_WRITE
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder dataWriteError(Throwable cause) {
    return new Builder(DrillPBError.ErrorType.DATA_WRITE, cause);
  }

  /**
   * creates a new user exception builder .
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#FUNCTION
   * @return user exception builder
   */
  public static Builder functionError() {
    return functionError(null);
  }

  /**
   * wraps the passed exception inside a function error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build()} instead
   * of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#FUNCTION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder functionError(Throwable cause) {
    return new Builder(DrillPBError.ErrorType.FUNCTION, cause);
  }

  /**
   * creates a new user exception builder .
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#PARSE
   * @return user exception builder
   */
  public static Builder parseError() {
    return parseError(null);
  }

  /**
   * wraps the passed exception inside a system error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build()} instead
   * of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#PARSE
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder parseError(Throwable cause) {
    return new Builder(DrillPBError.ErrorType.PARSE, cause);
  }

  /**
   * creates a new user exception builder .
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#PERMISSION
   * @return user exception builder
   */
  public static Builder permissionError() {
    return permissionError(null);
  }

  /**
   * wraps the passed exception inside a system error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build()} instead
   * of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#PERMISSION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder permissionError(Throwable cause) {
    return new Builder(DrillPBError.ErrorType.PERMISSION, cause);
  }

  /**
   * creates a new user exception builder .
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#PLAN
   * @return user exception builder
   */
  public static Builder planError() {
    return planError(null);
  }

  /**
   * wraps the passed exception inside a system error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build()} instead
   * of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#PLAN
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder planError(Throwable cause) {
    return new Builder(DrillPBError.ErrorType.PLAN, cause);
  }

  /**
   * creates a new user exception builder .
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#RESOURCE
   * @return user exception builder
   */
  public static Builder resourceError() {
    return resourceError(null);
  }

  /**
   * wraps the passed exception inside a system error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build()} instead
   * of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#RESOURCE
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder resourceError(Throwable cause) {
    return new Builder(DrillPBError.ErrorType.RESOURCE, cause);
  }

  /**
   * creates a new user exception builder .
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#UNSUPPORTED_OPERATION
   * @return user exception builder
   */
  public static Builder unsupportedError() {
    return unsupportedError(null);
  }

  /**
   * wraps the passed exception inside a system error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build()} instead
   * of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType#UNSUPPORTED_OPERATION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder unsupportedError(Throwable cause) {
    return new Builder(DrillPBError.ErrorType.UNSUPPORTED_OPERATION, cause);
  }

  /**
   * Builder class for DrillUserException. You can wrap an existing exception, in this case it will first check if
   * this exception is, or wraps, a DrillUserException. If it does then the builder will use the user exception as it is
   * (it will ignore the message passed to the constructor) and will add any additional context information to the
   * exception's context
   */
  public static class Builder {

    private final Throwable cause;
    private final DrillPBError.ErrorType errorType;
    private final UserException uex;
    private final UserExceptionContext context;

    private String message;

    /**
     * wraps an existing exception inside a user exception.
     *
     * @param errorType user exception type that should be created if the passed exception isn't,
     *                  or doesn't wrap a user exception
     * @param cause exception to wrap inside a user exception. Can be null
     */
    private Builder(DrillPBError.ErrorType errorType, Throwable cause) {
      this.cause = cause;

      //TODO handle the improbable case where cause is a SYSTEM exception ?
      uex = ErrorHelper.findWrappedUserException(cause);
      if (uex != null) {
        this.errorType = null;
        this.context = uex.context;
      } else {
        // we will create a new user exception
        this.errorType = errorType;
        this.context = new UserExceptionContext();
        this.message = cause != null ? cause.getMessage() : null;
      }
    }

    /**
     * sets or replaces the error message.
     * <p>This will be ignored if this builder is wrapping a user exception
     *
     * @see String#format(String, Object...)
     *
     * @param format format string
     * @param args Arguments referenced by the format specifiers in the format string
     * @return this builder
     */
    public Builder message(String format, Object... args) {
      // we can't replace the message of a user exception
      if (uex == null && format != null) {
        this.message = String.format(format, args);
      }
      return this;
    }

    /**
     * add DrillbitEndpoint identity to the context.
     * <p>if the context already has a drillbitEndpoint identity, the new identity will be ignored
     *
     * @param endpoint drillbit endpoint identity
     */
    public Builder addIdentity(CoordinationProtos.DrillbitEndpoint endpoint) {
      context.add(endpoint);
      return this;
    }

    /**
     * add a string line to the bottom of the context
     * @param value string line
     * @return this builder
     */
    public Builder addContext(String value) {
      context.add(value);
      return this;
    }

    /**
     * add a string value to the bottom of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder addContext(String name, String value) {
      context.add(name, value);
      return this;
    }

    /**
     * add a long value to the bottom of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder addContext(String name, long value) {
      context.add(name, value);
      return this;
    }

    /**
     * add a double value to the bottom of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder addContext(String name, double value) {
      context.add(name, value);
      return this;
    }

    /**
     * pushes a string value to the top of the context
     *
     * @param value context value
     * @return this builder
     */
    public Builder pushContext(String value) {
      context.push(value);
      return this;
    }

    /**
     * pushes a string value to the top of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder pushContext(String name, String value) {
      context.push(name, value);
      return this;
    }

    /**
     * pushes a long value to the top of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder pushContext(String name, long value) {
      context.push(name, value);
      return this;
    }

    /**
     * pushes a double value to the top of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder pushContext(String name, double value) {
      context.push(name, value);
      return this;
    }

    /**
     * builds a user exception or returns the wrapped one.
     *
     * @return user exception
     */
    public UserException build() {

      if (uex != null) {
        return uex;
      }

      return new UserException(this);
    }
  }

  private final DrillPBError.ErrorType errorType;

  private final UserExceptionContext context;

  protected UserException(DrillPBError.ErrorType errorType, String message, Throwable cause) {
    super(message, cause);

    this.errorType = errorType;
    this.context = new UserExceptionContext();
  }

  private UserException(Builder builder) {
    super(builder.message, builder.cause);
    this.errorType = builder.errorType;
    this.context = builder.context;
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
    return errorType + " ERROR: " + super.getMessage() + "\n\n" +
      context.generateContextMessage();
  }

}
