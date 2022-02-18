package org.apache.drill.exec.expr.fn.impl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Float.NaN;

public class MathFunctionsVarcharUtils {

  public double validateInput(String input) {

    String regex = "^[-]*[0-9.]*[0-9]*+$";
    Pattern pattern = java.util.regex.Pattern.compile(regex);
    input = input.trim();

    if (input != null) {
      Matcher matcher = pattern.matcher(input);

      if (!input.equals("") && matcher.matches()) {
        return Double.parseDouble(input);
      }
      else {
        return NaN;
      }
    }
    else {
      return NaN;
    }
  }
}
