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

import org.apache.drill.exec.proto.UserBitShared.ExceptionWrapper;
import org.apache.drill.exec.proto.UserBitShared.StackTraceElementWrapper;

import java.util.regex.Pattern;

/**
 * Utility class that handles error message generation from protobuf error objects.
 */
public class ErrorHelper {

  private final static Pattern IGNORE= Pattern.compile("^(sun|com\\.sun|java).*");

  /**
   * Wraps the exception into a SYSTEM ERROR if it's not already a DrillUserException, or if it's not wrapping a
   * DrillUserException
   * @param ex exception to wrap
   * @return user exception
   */
  public static DrillUserException wrap(final Throwable ex) {
    return new DrillUserException.Builder(ex).build();
  }

  /**
   * returns the user exception context for DrillUserExceptions(s) even if they are wrapped multiple times. If no
   * DrillUserException is found, it will create a new one.
   * This is useful if we want to add context to user exception before re-throwing it. For all other exception the
   * context will be discarded.
   *
   * @param ex exception we are trying to get the context for
   * @return user exception context
   */
  public static UserExceptionContext getExceptionContextOrNew(final Throwable ex) {
    DrillUserException uex = findWrappedUserException(ex);
    if (uex != null) {
      //TODO if uex is SYSTEM exception the calling code will be able to add context information to it. Do we want this ?
      return uex.getContext();
    }

    return new UserExceptionContext();
  }

  static String buildCausesMessage(final Throwable t) {

    StringBuilder sb = new StringBuilder();
    Throwable ex = t;
    boolean cause = false;
    while(ex != null){

      sb.append("  ");

      if(cause){
        sb.append("Caused By ");
      }

      sb.append("(");
      sb.append(ex.getClass().getCanonicalName());
      sb.append(") ");
      sb.append(ex.getMessage());
      sb.append("\n");

      for(StackTraceElement st : ex.getStackTrace()){
        sb.append("    ");
        sb.append(st.getClassName());
        sb.append('.');
        sb.append(st.getMethodName());
        sb.append("():");
        sb.append(st.getLineNumber());
        sb.append("\n");
      }
      cause = true;

      if(ex.getCause() != null && ex.getCause() != ex){
        ex = ex.getCause();
      } else {
        ex = null;
      }
    }

    return sb.toString();
  }

  static ExceptionWrapper getWrapper(Throwable ex) {
    return getWrapperBuilder(ex).build();
  }

  private static ExceptionWrapper.Builder getWrapperBuilder(Throwable ex) {
    return getWrapperBuilder(ex, false);
  }

  private static ExceptionWrapper.Builder getWrapperBuilder(Throwable ex, boolean includeAllStack) {
    ExceptionWrapper.Builder ew = ExceptionWrapper.newBuilder();
    if(ex.getMessage() != null) {
      ew.setMessage(ex.getMessage());
    }
    ew.setExceptionClass(ex.getClass().getCanonicalName());
    boolean isHidden = false;
    StackTraceElement[] stackTrace = ex.getStackTrace();
    for(int i = 0; i < stackTrace.length; i++){
      StackTraceElement ele = ex.getStackTrace()[i];
      if(include(ele, includeAllStack)){
        if(isHidden){
          isHidden = false;
        }
        ew.addStackTrace(getSTWrapper(ele));
      }else{
        if(!isHidden){
          isHidden = true;
          ew.addStackTrace(getEmptyST());
        }
      }

    }

    if(ex.getCause() != null && ex.getCause() != ex){
      ew.setCause(getWrapper(ex.getCause()));
    }
    return ew;
  }

  private static boolean include(StackTraceElement ele, boolean includeAllStack) {
    return includeAllStack || !(IGNORE.matcher(ele.getClassName()).matches());
  }

  private static StackTraceElementWrapper.Builder getSTWrapper(StackTraceElement ele) {
    StackTraceElementWrapper.Builder w = StackTraceElementWrapper.newBuilder();
    w.setClassName(ele.getClassName());
    if(ele.getFileName() != null) {
      w.setFileName(ele.getFileName());
    }
    w.setIsNativeMethod(ele.isNativeMethod());
    w.setLineNumber(ele.getLineNumber());
    w.setMethodName(ele.getMethodName());
    return w;
  }

  private static StackTraceElementWrapper.Builder getEmptyST() {
    StackTraceElementWrapper.Builder w = StackTraceElementWrapper.newBuilder();
    w.setClassName("...");
    w.setIsNativeMethod(false);
    w.setLineNumber(0);
    w.setMethodName("...");
    return w;
  }

  /**
   * searches for a DrillUserException wrapped inside the exception
   * @param ex exception
   * @return null if exception is null or no DrillUserException was found
   */
  static DrillUserException findWrappedUserException(Throwable ex) {
    if (ex == null) {
      return null;
    }

    Throwable cause = ex;
    while (!(cause instanceof DrillUserException)) {
      if (cause.getCause() != null && cause.getCause() != cause) {
        cause = cause.getCause();
      } else {
        return null;
      }
    }

    return (DrillUserException) cause;
  }

}
