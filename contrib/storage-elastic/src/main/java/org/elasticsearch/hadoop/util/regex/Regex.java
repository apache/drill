/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.util.regex;

import java.util.Collection;
import java.util.List;

// Taken from Elasticsearch core
public class Regex {

    /**
    * Is the str a simple match pattern.
    */
    public static boolean isSimpleMatchPattern(String str) {
        return str.indexOf('*') != -1;
    }

    public static boolean isMatchAllPattern(String str) {
        return str.equals("*");
    }

    /**
    * Match a String against the given pattern, supporting the following simple
    * pattern styles: "xxx*", "*xxx", "*xxx*" and "xxx*yyy" matches (with an
    * arbitrary number of pattern parts), as well as direct equality.
    *
    * @param pattern the pattern to match against
    * @param str     the String to match
    * @return whether the String matches the given pattern
    */
    public static boolean simpleMatch(String pattern, String str) {
        if (pattern == null || str == null) {
            return false;
        }
        int firstIndex = pattern.indexOf('*');
        if (firstIndex == -1) {
            return pattern.equals(str);
        }
        if (firstIndex == 0) {
            if (pattern.length() == 1) {
                return true;
            }
            int nextIndex = pattern.indexOf('*', firstIndex + 1);
            if (nextIndex == -1) {
                return str.endsWith(pattern.substring(1));
            }
            else if (nextIndex == 1) {
                // Double wildcard "**" - skipping the first "*"
                return simpleMatch(pattern.substring(1), str);
            }
            String part = pattern.substring(1, nextIndex);
            int partIndex = str.indexOf(part);
            while (partIndex != -1) {
                if (simpleMatch(pattern.substring(nextIndex), str.substring(partIndex + part.length()))) {
                    return true;
                }
                partIndex = str.indexOf(part, partIndex + 1);
            }
            return false;
        }
        return (str.length() >= firstIndex && pattern.substring(0, firstIndex).equals(str.substring(0, firstIndex)) && simpleMatch(
                pattern.substring(firstIndex), str.substring(firstIndex)));
    }

    /**
    * Match a String against the given patterns, supporting the following simple
    * pattern styles: "xxx*", "*xxx", "*xxx*" and "xxx*yyy" matches (with an
    * arbitrary number of pattern parts), as well as direct equality.
    *
    * @param patterns the patterns to match against
    * @param str      the String to match
    * @return whether the String matches any of the given patterns
    */
    public static boolean simpleMatch(Collection<String> patterns, String str) {
        if (patterns != null) {
            for (String pattern : patterns) {
                if (simpleMatch(pattern, str)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean simpleMatch(List<String> patterns, String[] types) {
        if (patterns != null && types != null) {
            for (String type : types) {
                for (String pattern : patterns) {
                    if (simpleMatch(pattern, type)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}