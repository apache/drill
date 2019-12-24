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
package org.elasticsearch.hadoop.serialization.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonStreamContext;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.impl.JsonParserBase;
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException;
import org.elasticsearch.hadoop.serialization.Parser;

public class JacksonJsonParser implements Parser {

    private static final JsonFactory JSON_FACTORY;
    private final JsonParser parser;
    private final JsonParserBase richerParser;

    static {
        JSON_FACTORY = new JsonFactory();
        //JSON_FACTORY.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        //JSON_FACTORY.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        //JSON_FACTORY.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        //JSON_FACTORY.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
        //JSON_FACTORY.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
    }

    public JacksonJsonParser(InputStream in) {
        try {
            this.parser = JSON_FACTORY.createJsonParser(in);
            richerParser = (parser instanceof JsonParserBase ? (JsonParserBase) parser : null);
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    public JacksonJsonParser(byte[] content) {
        this(content, 0, content.length);
    }

    public JacksonJsonParser(byte[] content, int offset, int length) {
        try {
            this.parser = JSON_FACTORY.createJsonParser(content, offset, length);
            richerParser = (parser instanceof JsonParserBase ? (JsonParserBase) parser : null);
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    public JacksonJsonParser(JsonParser parser) {
        this.parser = parser;
        richerParser = (parser instanceof JsonParserBase ? (JsonParserBase) parser : null);
    }

    @Override
    public Token currentToken() {
        return convertToken(parser.getCurrentToken());
    }

    @Override
    public Object currentValue() {
        try {
            return (parser.getCurrentToken().isNumeric() ? parser.getNumberValue() : parser.getText());
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Token nextToken() {
        try {
            return convertToken(parser.nextToken());
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    private Token convertToken(JsonToken token) {
        if (token == null) {
            return null;
        }
        switch (token) {
        case FIELD_NAME:
            return Token.FIELD_NAME;
        case VALUE_FALSE:
        case VALUE_TRUE:
            return Token.VALUE_BOOLEAN;
        case VALUE_STRING:
            return Token.VALUE_STRING;
        case VALUE_NUMBER_INT:
        case VALUE_NUMBER_FLOAT:
            return Token.VALUE_NUMBER;
        case VALUE_NULL:
            return Token.VALUE_NULL;
        case START_OBJECT:
            return Token.START_OBJECT;
        case END_OBJECT:
            return Token.END_OBJECT;
        case START_ARRAY:
            return Token.START_ARRAY;
        case END_ARRAY:
            return Token.END_ARRAY;
        case VALUE_EMBEDDED_OBJECT:
            return Token.VALUE_EMBEDDED_OBJECT;
        case NOT_AVAILABLE:
            throw new UnsupportedOperationException();
        }
        throw new EsHadoopSerializationException("No matching token for json_token [" + token + "]");
    }

    @Override
    public void skipChildren() {
        try {
            parser.skipChildren();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public String absoluteName() {
        List<String> tree = new ArrayList<String>();
        for (JsonStreamContext ctx = parser.getParsingContext(); ctx != null; ctx = ctx.getParent()) {
            if (ctx.inObject()) {
                tree.add(ctx.getCurrentName());
            }
        }
        StringBuilder sb = new StringBuilder();
        for (int index = tree.size(); index > 0; index--) {
            sb.append(tree.get(index - 1));
            sb.append(".");
        }

        // remove the last .
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    @Override
    public String currentName() {
        try {
            return parser.getCurrentName();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }


    @Override
    public String text() {
        try {
            return parser.getText();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public byte[] bytes() {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Number numberValue() {
        try {
            return parser.getNumberValue();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public NumberType numberType() {
        try {
            return convertNumberType(parser.getNumberType());
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public short shortValue() {
        try {
            return parser.getShortValue();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public int intValue() {
        try {
            return parser.getIntValue();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public long longValue() {
        try {
            return parser.getLongValue();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public float floatValue() {
        try {
            return parser.getFloatValue();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public double doubleValue() {
        try {
            return parser.getDoubleValue();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public boolean booleanValue() {
        try {
            return parser.getBooleanValue();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public byte[] binaryValue() {
        try {
            return parser.getBinaryValue();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public void close() {
        try {
            parser.close();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    private NumberType convertNumberType(JsonParser.NumberType numberType) {
        switch (numberType) {
        case INT:
            return NumberType.INT;
        case LONG:
            return NumberType.LONG;
        case FLOAT:
            return NumberType.FLOAT;
        case DOUBLE:
            return NumberType.DOUBLE;
        case BIG_INTEGER:
            return NumberType.DOUBLE;
        case BIG_DECIMAL:
            return NumberType.DOUBLE;
        }
        throw new EsHadoopSerializationException("No matching token for number_type [" + numberType + "]");
    }

    @Override
    public int tokenCharOffset() {
        return (int) (richerParser != null ? richerParser.getTokenCharacterOffset() : parser.getTokenLocation().getCharOffset());
    }
}