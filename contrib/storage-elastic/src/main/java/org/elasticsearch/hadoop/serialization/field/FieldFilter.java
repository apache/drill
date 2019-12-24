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
package org.elasticsearch.hadoop.serialization.field;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.hadoop.util.regex.Regex;

public abstract class FieldFilter {

    public static class Result {
        public final boolean matched;
        public final int depth;

        public Result(boolean matched) {
            this(matched, 1);
        }

        public Result(boolean matched, int depth) {
            this.matched = matched;
            this.depth = depth;
        }
    }

    public static final Result INCLUDED = new Result(true);
    public static final Result EXCLUDED = new Result(false);

    public static class NumberedInclude {
        public final String filter;
        public final int depth;

        public NumberedInclude(String filter) {
            this(filter, 1);
        }

        public NumberedInclude(String filter, int depth) {
            this.filter = filter;
            this.depth = depth;
        }
    }

    /**
     * Returns true if the key should be kept or false if it needs to be skipped/dropped.
     *
     * @param path
     * @param includes
     * @param excludes
     * @return
     */
    public static Result filter(String path, Collection<NumberedInclude> includes, Collection<String> excludes, boolean allowPartialMatches) {
        includes = (includes == null ? Collections.<NumberedInclude> emptyList() : includes);
        excludes = (excludes == null ? Collections.<String> emptyList() : excludes);

        if (includes.isEmpty() && excludes.isEmpty()) {
            return INCLUDED;
        }

        if (Regex.simpleMatch(excludes, path)) {
            return EXCLUDED;
        }

        boolean exactIncludeMatch = false; // true if the current position was specifically mentioned
        boolean pathIsPrefixOfAnInclude = false; // true if potentially a sub scope can be included

        NumberedInclude matchedInclude = null;

        if (includes.isEmpty()) {
            // implied match anything
            exactIncludeMatch = true;
        }
        else {
            for (NumberedInclude filter : includes) {
                matchedInclude = filter;
                String include = filter.filter;

                // check for prefix matches as well to see if we need to zero in, something like: obj1.arr1.* or *.field
                // note, this does not work well with middle matches, like obj1.*.obj3
                if (include.charAt(0) == '*') {
                    if (Regex.simpleMatch(include, path)) {
                        exactIncludeMatch = true;
                        break;
                    }
//                    pathIsPrefixOfAnInclude = true;
//                    continue;
                }
                if (include.startsWith(path)) {
                    if (include.length() == path.length()) {
                        exactIncludeMatch = true;
                        break;
                    }
                    else if (include.length() > path.length() && include.charAt(path.length()) == '.') {
                        // include might may match deeper paths. Dive deeper.
                        pathIsPrefixOfAnInclude = true;
                        continue;
                    }
                }
                if (Regex.simpleMatch(include, path)) {
                    exactIncludeMatch = true;
                    break;
                }
            }
        }

        // if match or part of the path (based on the passed param)
        if (exactIncludeMatch || (allowPartialMatches && pathIsPrefixOfAnInclude)) {
            return (matchedInclude != null ? new Result(true, matchedInclude.depth) : INCLUDED);
        }

        return EXCLUDED;
    }

    public static Result filter(String path, Collection<NumberedInclude> includes, Collection<String> excludes) {
        return filter(path, includes, excludes, true);
    }

    public static List<NumberedInclude> toNumberedFilter(Collection<String> includeAsStrings){
        if (includeAsStrings == null || includeAsStrings.isEmpty()) {
            return Collections.<NumberedInclude> emptyList();
        }

        List<NumberedInclude> newFilter = new ArrayList<NumberedInclude>(includeAsStrings.size());

        for (String include : includeAsStrings) {
            newFilter.add(new NumberedInclude(include));
        }

        return newFilter;
    }
}