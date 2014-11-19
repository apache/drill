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
package org.apache.drill.exec.work;

import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.ExceptionWrapper;
import org.apache.drill.exec.proto.UserBitShared.StackTraceElementWrapper;
import org.apache.drill.exec.rpc.RemoteRpcException;
import org.slf4j.Logger;


public class ErrorHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ErrorHelper.class);

  final static Pattern IGNORE= Pattern.compile("^(sun|com\\.sun|java).*");

  /**
   * Manages message conversion before returning to user.  If the exception is a remote rpc exception, will simply return user friendly message.  Otherwise, will log and return.
   * TODO: this should really be done client side but we don't currently have any way to maintain session state on the client.
   *
   * @param endpoint
   * @param message
   * @param t
   * @param logger
   * @param verbose
   * @return
   */
  public static DrillPBError logAndConvertMessageError(DrillbitEndpoint endpoint, String message, Throwable t, Logger logger, boolean verbose) {

    DrillPBError baseError = t instanceof RemoteRpcException ? ((RemoteRpcException) t).getRemoteError() : logAndConvertError(endpoint, message, t, logger);
    String userMessage = getErrorMessage(baseError, verbose);
    return DrillPBError.newBuilder() //
      .setEndpoint(baseError.getEndpoint()) //
      .setErrorId(baseError.getErrorId()) //
      .setMessage(userMessage) //
      .build();
  }

  public static DrillPBError logAndConvertError(DrillbitEndpoint endpoint, String message, Throwable t, Logger logger) {
    String id = UUID.randomUUID().toString();
    DrillPBError.Builder builder = DrillPBError.newBuilder();
    builder.setEndpoint(endpoint);
    builder.setErrorId(id);
    if(message != null){
      builder.setMessage(message);
    }
    if(t == null){
      t = new DrillException("Undefined failure occurred.");
    }
    builder.setException(getWrapper(t));

    // record the error to the log for later reference.
    logger.error("Error {}: {}", id, message, t);

    return builder.build();
  }

  public static ExceptionWrapper getWrapper(Throwable ex){
    return getWrapperBuilder(ex).build();
  }

  public static ExceptionWrapper.Builder getWrapperBuilder(Throwable ex){
    return getWrapperBuilder(ex, false);
  }

  public static ExceptionWrapper.Builder getWrapperBuilder(Throwable ex, boolean includeAllStack){



    ExceptionWrapper.Builder ew = ExceptionWrapper.newBuilder();
    if(ex.getMessage() != null) {
      ew.setMessage(ex.getMessage());
    }
    ew.setExceptionClass(ex.getClass().getCanonicalName());
    boolean isHidden = false;
    StackTraceElementWrapper[] wrappers = new StackTraceElementWrapper[ex.getStackTrace().length];
    for(int i = 0; i < wrappers.length; i++){
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

  private static boolean include(StackTraceElement ele, boolean includeAllStack){
    if(includeAllStack) {
      return true;
    }
    return !(IGNORE.matcher(ele.getClassName()).matches());
  }

  private static StackTraceElementWrapper.Builder getEmptyST(){
    StackTraceElementWrapper.Builder w = StackTraceElementWrapper.newBuilder();
    w.setClassName("...");
    w.setIsNativeMethod(false);
    w.setLineNumber(0);
    w.setMethodName("...");
    return w;
  }
  public static StackTraceElementWrapper.Builder getSTWrapper(StackTraceElement ele){
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


  public static String getErrorMessage(final DrillPBError error, final boolean verbose) {

    String finalMessage = null;
    ExceptionWrapper ex = error.getException();
    StringBuilder sb = new StringBuilder();



    sb //
      .append("[ ") //
      .append(error.getErrorId()) //
      .append(" on ")
      .append(error.getEndpoint().getAddress())
      .append(":").append(error.getEndpoint().getUserPort())
      .append(" ]\n");

    boolean cause = false;
    while(ex != null){

      if(ex.hasMessage()){
        finalMessage = ex.getMessage();
      }

      if(verbose){
        sb.append("  ");

        if(cause){
          sb.append("Caused By ");
        }

        sb.append("(");
        sb.append(ex.getExceptionClass());
        sb.append(") ");
        sb.append(ex.getMessage());
        sb.append("\n");
        for(int i = 0; i < ex.getStackTraceCount(); i++){
          StackTraceElementWrapper st = ex.getStackTrace(i);
          sb.append("    ");
          sb.append(st.getClassName());
          sb.append('.');
          sb.append(st.getMethodName());
          sb.append("():");
          sb.append(st.getLineNumber());
          sb.append("\n");
        }
        cause = true;
      }

      ex = ex.hasCause() ? ex.getCause() : null;


    }

    StringBuilder msg = new StringBuilder();

    if (error.hasMessage()){
      msg.append(error.getMessage());
      if(finalMessage != null){
        msg.append(", ");
        msg.append(finalMessage);
        msg.append(' ');
      }
    }else if(finalMessage != null){
      msg.append(finalMessage);
      msg.append(' ');
    }else{
      msg.append("Error ");
    }

    msg.append(sb);

    return msg.toString();
  }

  public static void main(String[] args ){
    DrillPBError e = logAndConvertError(DrillbitEndpoint.newBuilder().setAddress("host1").setControlPort(1234).build(), "RpcFailure", new Exception("Excep 1", new Exception("excep2")), logger);
    System.out.println(getErrorMessage(e, false));
    System.out.println("\n\n\n");
    System.out.println(getErrorMessage(e, true));

  }

}
