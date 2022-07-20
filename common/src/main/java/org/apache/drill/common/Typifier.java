/*
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

package org.apache.drill.common;

import java.nio.CharBuffer;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * This class attempts to infer the data type of an unknown data type. It is somewhat
 * configurable.  This was sourced from <a href="https://gist.github.com/awwsmm/56b8164410c89c719ebfca7b3d85870b">this code on github</a>.
 */
public class Typifier {

  private static final Locale defaultLocale = new Locale("en");

  private static final HashSet<DateTimeFormatter> formats = new HashSet<>(
    Arrays.asList(
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", defaultLocale),
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SS", defaultLocale),
      DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a", defaultLocale),
      DateTimeFormatter.ofPattern("M/d/yy H:mm", defaultLocale),
      DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss", defaultLocale)));

  private static final HashSet<DateTimeFormatter> dateFormats = new HashSet<>(
    Arrays.asList(
      DateTimeFormatter.ofPattern("yyyy-MM-dd", defaultLocale),
      DateTimeFormatter.ofPattern("MM/dd/yyyy", defaultLocale),
      DateTimeFormatter.ofPattern("M/d/yy", defaultLocale),
      DateTimeFormatter.ofPattern("dd/MM/yyyy", defaultLocale),
      DateTimeFormatter.ofPattern("yyyy/MM/dd", defaultLocale),
      DateTimeFormatter.ofPattern("M d, yyyy", defaultLocale)
    ));

  // Only Strings contain these characters -- skip all numeric processing
  // arranged roughly by frequency in ~130MB of sample DASGIP files:
  //   $ awk -vFS="" '{for(i=1;i<=NF;i++)w[$i]++}END{for(i in w) print i,w[i]}' file.txt
  private static final char[] StringCharacters = new
    char[] {' ', ':', 'n', 'a', 't', 'r', 'o', 'C', 'i', 'P', 'D', 's', 'c', 'S', 'u', 'A', 'm', '=', 'O', '\\', 'd', 'p', 'T', 'M', 'g', 'I', 'b', 'U', 'h', 'H'};

  // Typify looks for the above characters in an input String before it makes
  // any attempt at parsing that String. If it finds any of the above characters,
  // it immediately skips to the String-processing section, because no numerical
  // type can contain those characters.

  // Adding more characters means that there are more characters to look for in
  // the input String every time a piece of data is parsed, but it also reduces
  // the likelihood that an Exception will be thrown when String data is attempted
  // to be parsed as numerical data (which saves time).

  // The characters below can also be added to the list, but the list above
  // seems to be near-optimal

  //  'J', '+', 'V', 'B', 'G',   'R', 'y', '(', ')', 'v',   '_', ',', '[', ']', '/',
  //  'N', 'k', 'w', '}', '{',   'X', '%', '>', 'x', '\'',  'W', '<', 'K', 'Q', 'q',
  //  'z', 'Y', 'j', 'Z', '!',   '#', '$', '&', '*', ',',   ';', '?', '@', '^', '`',
  //  '|', '~'};

  private static final String[] falseAliases = new String[]{"false", "False", "FALSE"};

  private static final String[] trueAliases = new String[]{"true", "True", "TRUE"};

  // If a String contains any of these, try to evaluate it as an equation
  private static final char[] MathCharacters = new char[]{'+', '-', '/', '*', '='};

  // default is:
  //   > don't interpret "0" and "1" as true and false
  //   > restrict interpretation to common types
  //   > don't allow f/F/l/L postfixes for float/long numbers
  //   > attempt to parse dates

  /**
   * Attempts to classify String input as double, int, char, etc.
   * Common types are: boolean, double, string, timestamp.
   * This is the default constructor for the simplest use case.  The defaults are not
   * to interpet 0,1 as true/false, restrict interpretation to common types,
   * not to allow f/F/l/L postfixes for float and longs and attempts to parse
   * dates.
   * @param data Input string of data
   * @return An Entry of the Class and the original value
   */
  public static Entry<Class, String> typify(String data) {
    return typify(data, false, true, true, true);
  }

  /**
   * Attempts to determine the best data type for an unknown bit of text.  This
   * constructor allows you to set a few
   * @param data The unknown data string
   * @param bool01 True, if you want 0/1 to be marked as boolean, false if not
   * @param commonTypes Limit typifier to boolean, double, string, timestamp
   * @param postfixFL Allow typifier to consider f/F/l/L postfixes for float and longs
   * @param parseDates Attempt to parse timestamps
   * @return An {@link Entry} consisting of the object class and the original value as a String
   */
  public static Entry<Class, String> typify(String data,
                                            boolean bool01,
                                            boolean commonTypes,
                                            boolean postfixFL,
                                            boolean parseDates) {

    // -2. if the input data has 0 length, return as null object
    if (data == null || data.length() == 0) {
      return new SimpleEntry<>(Object.class, null);
    }

    String s = data.trim();
    int slen = s.length();

    // -1. if the input data is only whitespace, return "String" and input as-is
    if (slen == 0) {
      return new SimpleEntry<>(String.class, data);
    }

    // In most data, numerical values are more common than true/false values. So,
    // if we want to speed up data parsing, we can move this block to the end when
    // looking only for common types.

    /// Check if the data is Boolean (true or false)
    if (!commonTypes) {
      if (contains(falseAliases, s)) {
        return new SimpleEntry<>(Boolean.class, "false");
      } else if (contains(trueAliases, s)) {
        return new SimpleEntry<>(Boolean.class, "true");
      }
    }

    // Check for any String-only characters; if we find them, don't bother trying to parse this as a number
    if (!containsAny(s, StringCharacters)) {

      // try again for boolean -- need to make sure it's not parsed as Byte
      if (bool01) {
        if (s.equals("0")) {
          return new SimpleEntry<>(Boolean.class, "false");
        } else if (s.equals("1")) {
          return new SimpleEntry<>(Boolean.class, "true");
        }
      }

      char lastChar = s.charAt(slen - 1);
      boolean lastCharF = (lastChar == 'f' || lastChar == 'F');
      boolean lastCharL = (lastChar == 'l' || lastChar == 'L');

      // If we're not restricted to common types, look for anything
      if (!commonTypes) {
        // 1. Check if data is a Byte (1-byte integer with range [-(2e7) = -128, ((2e7)-1) = 127])
        try {
          byte b = Byte.parseByte(s);
          return new SimpleEntry<>(Byte.class, Byte.toString(b));
        } catch (NumberFormatException ex) {
          // Okay, guess it's not a Byte
        }

        // 2. Check if data is a Short (2-byte integer with range [-(2e15) = -32768, ((2e15)-1) = 32767])
        try {
          short h = Short.parseShort(s);
          return new SimpleEntry<>(Short.class, Short.toString(h));
        } catch (NumberFormatException ex) {
          // Okay, guess it's not a Short
        }

        // 3. Check if data is an Integer (4-byte integer with range [-(2e31), (2e31)-1])
        try {
          int i = Integer.parseInt(s);
          return new SimpleEntry<>(Integer.class, Integer.toString(i));
        } catch (NumberFormatException ex) {
          // okay, guess it's not an Integer
        }
        String s_L_trimmed = s;

        // 4. Check if data is a Long (8-byte integer with range [-(2e63), (2e63)-1])
        //    ...first, see if the last character of the string is "L" or "l"
        //    ... Java parses "3.3F", etc. fine as a float, but throws an error with "3L", etc.
        if (postfixFL && slen > 1 && lastCharL) {
          s_L_trimmed = s.substring(0, slen - 1);
        }

        try {
          long l = Long.parseLong(s_L_trimmed);
          return new SimpleEntry<>(Long.class, Long.toString(l));
        } catch (NumberFormatException ex) {
          // okay, guess it's not a Long
        }

        // 5. Check if data is a Float (32-bit IEEE 754 floating point with approximate extents +/- 3.4028235e38)
        if (postfixFL || !lastCharF) {
          try {
            float f = Float.parseFloat(s);
            // If it's beyond the range of Float, maybe it's not beyond the range of Double
            if (!Float.isInfinite(f)) {
              return new SimpleEntry<>(Float.class, Float.toString(f));
            }
          } catch (NumberFormatException ex) {
            // okay, guess it's not a Float
          }
        }
      }

      // 6. Check if data is a Double (64-bit IEEE 754 floating point with approximate extents +/- 1.797693134862315e308 )
      if (postfixFL || !lastCharF) {
        try {
          double d = Double.parseDouble(s);
          if (!Double.isInfinite(d)) {
            return new SimpleEntry<>(Double.class, Double.toString(d));
          } else {
            return new SimpleEntry<>(String.class, s);
          }
        } catch (NumberFormatException ex) {
          // okay, guess it's not a Double
        }
      }
    }

    // Check for either Boolean or String
    if (commonTypes) {
      if (contains(falseAliases, s)) {
        return new SimpleEntry<>(Boolean.class, "false");
      } else if (contains(trueAliases, s)) {
        return new SimpleEntry<>(Boolean.class, "true");
      }
    }

    // 7. revert to String by default, with caveats...

    // 7a. if string has length 1, it is a single character
    if (!commonTypes && slen == 1) {
      return new SimpleEntry<>(Character.class, s); // end uncommon types 2/2
    }

    // 7b. attempt to parse String as a LocalDateTime
    if (parseDates && stringAsDateTime(s) != null) {
      return new SimpleEntry<>(LocalDateTime.class, s);
    }

    // 7c. Attempt to parse the String as a LocalDate
    if (parseDates && stringAsDate(s) != null) {
      return new SimpleEntry<>(LocalDate.class, s);
    }

    // ...if we've made it all the way to here without returning, give up and return "String" and input as-is
    return new SimpleEntry<>(String.class, data);
  }

  /**
   * Helper function that attempts to parse a String as a LocalDateTime.  If the
   * string cannot be parsed, return null.
   * @param date Input date string
   * @return LocalDateTime representation of the input String.
   */
  private static LocalDateTime stringAsDateTime(String date) {
    for (DateTimeFormatter format : formats) {
      try {
        return LocalDateTime.parse(date, format);
      } catch (DateTimeParseException ex) {
        // can't parse it as this format, but maybe the next one...?
      }
    }
    return null;
  }

  /**
   * Helper function that attempts to parse a String as a LocalDateTime.  If the
   * string cannot be parsed, return null.
   * @param date Input date string
   * @return LocalDateTime representation of the input String.
   */
  private static LocalDate stringAsDate(String date) {
    for (DateTimeFormatter format : dateFormats) {
      try {
        return LocalDate.parse(date, format);
      } catch (DateTimeParseException ex) {
        // can't parse it as this format, but maybe the next one...?
      }
    }
    return null;
  }

  /**
   * Returns true if any of the source characters are found in the target.
   * @param target The target character sequence AKA the haystack.
   * @param source The source characters, AKA the needle
   * @return True if the needle is in the haystack, false if not
   */
  public static boolean containsAny(CharSequence target, CharSequence source) {
    if (target == null || target.length() == 0 || source == null || source.length() == 0) {
      return false;
    }

    for (int aa = 0; aa < target.length(); ++aa) {
      for (int bb = 0; bb < source.length(); ++bb) {
        if (source.charAt(bb) == target.charAt(aa)) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean containsAny(CharSequence target, char[] source) {
    return containsAny(target, CharBuffer.wrap(source));
  }

  /**
   * Checks if a target array contains the source term.
   * @param target The target array
   * @param source The source term
   * @param <T> Unknown class
   * @return True if the target array contains the source term, false if not
   */
  public static <T> boolean contains(T[] target, T source) {
    if (source == null) {
      return false;
    }
    for (T t : target) {
      if (t != null && t.equals(source)) {
        return true;
      }
    }
    return false;
  }
}
