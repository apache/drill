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
package org.elasticsearch.hadoop.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.hadoop.util.ByteSequence;
import org.elasticsearch.hadoop.util.StringUtils;

class ErrorUtils {

    // find error inside ElasticsearchParseException
    private static final Pattern XCONTENT_PAYLOAD = Pattern.compile("Elastic[s|S]earchParseException.+: \\[(.+)\\]]");
    private static final Pattern OFFSET = Pattern.compile("offset=(\\d+)");
    private static final Pattern LENGTH = Pattern.compile("length=(\\d+)");

    private static final Pattern LINE = Pattern.compile("line: (\\d+)");
    private static final Pattern COLUMN = Pattern.compile("column: (\\d+)");

    private static final int CHARS_TO_THE_LEFT = 15;
    private static final int CHARS_TO_THE_RIGHT = 5;

    static String extractInvalidXContent(String errorMessage) {
        if (!StringUtils.hasText(errorMessage)) {
            return null;
        }

        String group = findMatch(XCONTENT_PAYLOAD.matcher(errorMessage));
        if (!StringUtils.hasText(group)) {
            return null;
        }

        String match = findMatch(OFFSET.matcher(errorMessage));
        int offset = (StringUtils.hasText(match) ? Integer.valueOf(match) : 0);
        match = findMatch(LENGTH.matcher(errorMessage));
        int length = (StringUtils.hasText(match) ? Integer.valueOf(match) : 0);


        List<Byte> bytes = new ArrayList<Byte>();
        // parse the collection into numbers and back to a String
        try {
            for (String byteValue : StringUtils.tokenize(group, ",")) {
                bytes.add(Byte.parseByte(byteValue));
            }
            if (length == 0) {
                length = bytes.size();
            }

            byte[] primitives = new byte[length];
            for (int index = 0; index < length; index++) {
                primitives[index] = bytes.get(index + offset).byteValue();
            }
            return new String(primitives, StringUtils.UTF_8);
        } catch (Exception ex) {
            // can't convert back the byte array - give up
            return null;
        }
    }


    static String extractJsonParse(String errorMessage, ByteSequence body) {
        if (!StringUtils.hasText(errorMessage)) {
            return null;
        }
        if (!errorMessage.startsWith("JsonParseException")) {
            return null;
        }

        String match = findMatch(LINE.matcher(errorMessage));
        int line = (StringUtils.hasText(match) ? Integer.valueOf(match) : 0);
        match = findMatch(COLUMN.matcher(errorMessage));
        int column = (StringUtils.hasText(match) ? Integer.valueOf(match) : 0);

        String payload = body.toString();
        int position = 0;
        int linesRead = 1;
        for (int index = 0; index < payload.length() && linesRead < line; index++) {
            if (payload.charAt(index) == '\n') {
                linesRead++;
            }
            position++;
        }
        position += column;

        // found line, return column +/- some chars
        int from = Math.max(position - CHARS_TO_THE_LEFT, 0);
        int to = Math.min(position + CHARS_TO_THE_RIGHT, payload.length());
        return payload.substring(from, to);
    }

    private static String findMatch(Matcher matcher) {
        return (matcher.find() ? matcher.group(1) : null);
    }
}