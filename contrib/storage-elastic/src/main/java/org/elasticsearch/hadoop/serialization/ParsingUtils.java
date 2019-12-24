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
package org.elasticsearch.hadoop.serialization;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.StringUtils;

public abstract class ParsingUtils {

    public static final String NOT_FOUND = "(not found)";

    /**
     * Seeks the field with the given name in the stream and positions (and returns) the parser to the next available token (value or not).
     * Return null if no token is found.
     *
     * @param path
     * @param parser
     * @return token associated with the given path or null if not found
     */
    public static Token seek(Parser parser, String path) {
        // return current token if no path is given
        if (!StringUtils.hasText(path)) {
            return null;
        }

        List<String> tokens = StringUtils.tokenize(path, ".");
        return seek(parser, tokens.toArray(new String[tokens.size()]));
    }

    public static Token seek(Parser parser, String[] path1) {
        return seek(parser, path1, null);
    }

    public static Token seek(Parser parser, String[] path1, String[] path2) {
        return doSeekToken(parser, path1, 0, path2, 0);
    }

    private static Token doSeekToken(Parser parser, String[] path1, int index1, String[] path2, int index2) {
        Token token = null;

        String currentName;
        token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }

        while ((token = parser.nextToken()) != null) {
            if (token == Token.START_OBJECT) {
                token = parser.nextToken();
            }
            if (token == Token.FIELD_NAME) {
                // found a node, go one level deep
                currentName = parser.currentName();
                if (path1 != null && currentName.equals(path1[index1])) {
                    if (index1 + 1 < path1.length) {
                        return doSeekToken(parser, path1, index1 + 1, null, 0);
                    }
                    else {
                        return parser.nextToken();
                    }
                }
                else if (path2 != null && currentName.equals(path2[index2])) {
                    if (index2 + 1 < path2.length) {
                        return doSeekToken(parser, null, 0, path2, index2 + 1);
                    }
                    else {
                        return parser.nextToken();
                    }
                }
                else {
                    // get field token (can be value, object or array)
                    parser.nextToken();
                    parser.skipChildren();
                }
            }
            else {
                break;
            }
        }

        return null;
    }

    private static class Matcher {
        private final List<String> tokens;
        private final String path;
        private boolean matched = false;
        private Object value;

        Matcher(String path) {
            this.path = path;
            tokens = StringUtils.tokenize(path, ".");
        }

        // number of levels required for the matcher
        int nesting() {
            return tokens.size() - 1;
        }

        boolean matches(String key, int level) {
            if (level < tokens.size()) {
                return tokens.get(level).equals(key);
            }
            return false;
        }

        void value(Object value) {
            matched = true;
            this.value = value;
        }

        @Override
        public String toString() {
            return path;
        }
    }

    public static List<Object> values(Parser parser, String... paths) {
        List<Matcher> matchers = new ArrayList<Matcher>(paths.length);
        int maxNesting = 0;
        for (String path : paths) {
            Matcher matcher = new Matcher(path);
            matchers.add(matcher);
            if (matcher.nesting() > maxNesting) {
                maxNesting = matcher.nesting();
            }
        }

        doFind(parser, matchers, 0, maxNesting);

        List<Object> matches = new ArrayList<Object>();
        for (Matcher matcher : matchers) {
            matches.add(matcher.matched ? matcher.value : NOT_FOUND);
        }

        return matches;
    }

    private static void doFind(Parser parser, List<Matcher> currentMatchers, int level, int maxNesting) {
        Token token = parser.currentToken();
        if (token == null) {
            // advance to the initial START_OBJECT token
            parser.nextToken();
        }

        while ((token = parser.nextToken()) != null && token != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                String currentName = parser.currentName();
                Object value = null;
                boolean valueRead = false;
                List<Matcher> nextLevel = null;

                for (Matcher matcher : currentMatchers) {
                    if (matcher.matches(currentName, level)) {
                        // found a match
                        if (matcher.nesting() == level) {
                            if (!valueRead) {
                                valueRead = true;
                                switch (parser.nextToken()) {
                                case VALUE_NUMBER:
                                    value = parser.numberValue();
                                    break;
                                case VALUE_BOOLEAN:
                                    value = Boolean.valueOf(parser.booleanValue());
                                    break;
                                case VALUE_NULL:
                                    value = null;
                                    break;
                                case VALUE_STRING:
                                    value = parser.text();
                                    break;
                                default:
                                    value = readValueAsString(parser);
                                }
                            }
                            matcher.value(value);
                        }
                        // partial match - keep it for the next level
                        else {
                            if (nextLevel == null) {
                                nextLevel = new ArrayList<Matcher>(currentMatchers.size());
                            }
                            nextLevel.add(matcher);
                        }
                    }
                }

                if (!valueRead) {
                    // must parse or skip the value
                    switch (parser.nextToken()) {
                        case START_OBJECT:
                            if (level < maxNesting && nextLevel != null) {
                                doFind(parser, nextLevel, level + 1, maxNesting);
                            } else {
                                parser.skipChildren();
                            }
                            break;
                        case START_ARRAY:
                            // arrays are not handled; simply ignore
                            parser.skipChildren();
                            break;
                    }
                }
            }
        }
    }

    private static String readValueAsString(Parser parser) {
        FastByteArrayOutputStream out = new FastByteArrayOutputStream(256);
        JacksonJsonGenerator generator = new JacksonJsonGenerator(out);
        traverse(parser, generator);
        generator.close();
        return out.toString();
    }

    private static void traverse(Parser parser, Generator generator) {
        Token t = parser.currentToken();
        switch (t) {
        case START_OBJECT:
            traverseMap(parser, generator);
            break;
        case START_ARRAY:
            traverseArray(parser, generator);
            break;
        case FIELD_NAME:
            generator.writeFieldName(parser.currentName());
            parser.nextToken();
            traverse(parser, generator);
            break;
        case VALUE_STRING:
            generator.writeString(parser.text());
            parser.nextToken();
            break;
        case VALUE_BOOLEAN:
            generator.writeBoolean(parser.booleanValue());
            parser.nextToken();
            break;
        case VALUE_NULL:
            generator.writeNull();
            parser.nextToken();
            break;
        case VALUE_NUMBER:
            switch (parser.numberType()) {
            case INT:
                generator.writeNumber(parser.intValue());
                break;
            case LONG:
                generator.writeNumber(parser.longValue());
                break;
            case DOUBLE:
                generator.writeNumber(parser.doubleValue());
                break;
            case FLOAT:
                generator.writeNumber(parser.floatValue());
                break;
            }
            parser.nextToken();
            break;
        }

    }

    private static void traverseMap(Parser parser, Generator generator) {
        generator.writeBeginObject();
        parser.nextToken();

        for (; parser.currentToken() != Token.END_OBJECT;) {
            traverse(parser, generator);
        }

        generator.writeEndObject();
        parser.nextToken();
    }

    private static void traverseArray(Parser parser, Generator generator) {
        generator.writeBeginArray();
        parser.nextToken();

        for (; parser.currentToken() != Token.END_ARRAY;) {
            traverse(parser, generator);
        }

        generator.writeEndArray();
        parser.nextToken();
    }
}