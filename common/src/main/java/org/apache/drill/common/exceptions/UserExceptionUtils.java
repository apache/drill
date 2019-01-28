package org.apache.drill.common.exceptions;

/**
 * Provides utilities (such as retrieving hints) to add more context to UserExceptions.
 */
public class UserExceptionUtils {
  public static final String USER_DOES_NOT_EXIST =
      "Username is absent in connection URL or doesn't exist on Drillbit node." +
          " Please specify a username in connection URL which is present on Drillbit node.";

  private UserExceptionUtils() {
    //Restrict instantiation
  }

  private static String decorateHint(final String text) {
    return String.format("[Hint: %s]", text);
  }
  public static String getUserHint(final Throwable ex) {
    if (ex.getMessage().startsWith("Error getting user info for current user")) {
      //User does not exist hint
      return decorateHint(USER_DOES_NOT_EXIST);
    } else {
      //No hint can be provided
      return "";
    }
  }
}
