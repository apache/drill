package org.apache.drill.plan.ast;

/**
 * Created with IntelliJ IDEA. User: tdunning Date: 10/12/12 Time: 11:25 PM To change this template
 * use File | Settings | File Templates.
 */
public class LogicalPlanParseException extends RuntimeException {
  public LogicalPlanParseException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
