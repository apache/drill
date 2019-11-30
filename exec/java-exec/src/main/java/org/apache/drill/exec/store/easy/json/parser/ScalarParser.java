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
package org.apache.drill.exec.store.easy.json.parser;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonElementParser;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;

import com.fasterxml.jackson.core.JsonToken;

abstract class ScalarParser extends AbstractParser.LeafParser {

  /**
   * Parses true | false | null
   */

  public static class BooleanParser extends ScalarParser {

    public BooleanParser(JsonElementParser parent, String key,
        ScalarWriter writer) {
      super(parent, key, writer);
    }

    @Override
    public boolean parse() {
      JsonToken token = loader.tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        setNull();
        break;
      case VALUE_TRUE:
        try {
          writer.setBoolean(true);
        } catch (UnsupportedConversionError e) {
          throw loader.typeError(e);
        }
        break;
      case VALUE_FALSE:
        try {
          writer.setBoolean(false);
        } catch (UnsupportedConversionError e) {
          throw loader.typeError(e);
        }
        break;
      default:
        throw loader.syntaxError(token, key(), "Boolean");
      }
      return true;
    }
  }

  /**
   * Parses integer | null
   */

  public static class IntParser extends ScalarParser {

    public IntParser(JsonElementParser parent, String key,
        ScalarWriter writer) {
      super(parent, key, writer);
    }

    @Override
    public boolean parse() {
      JsonToken token = loader.tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        setNull();
        break;
      case VALUE_NUMBER_INT:
        writer.setLong(loader.tokenizer.longValue());
        break;
      default:
        throw loader.syntaxError(token, key(), "Integer");
      }
      return true;
    }
  }

  /**
   * Parses float | integer | null
   * <p>
   * The integer value is allowed only after seeing a float value which
   * sets the type.
   */

  public static class FloatParser extends ScalarParser {

    public FloatParser(JsonElementParser parent, String key,
        ScalarWriter writer) {
      super(parent, key, writer);
    }

    @Override
    public boolean parse() {
      JsonToken token = loader.tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        setNull();
        break;
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
        writer.setDouble(loader.tokenizer.doubleValue());
        break;
      default:
        throw loader.syntaxError(token, key(), "Float");
      }
      return true;
    }
  }

  /**
   * Parses "str" | null
   */

  public static class StringParser extends ScalarParser {

    public StringParser(JsonElementParser parent, String key,
        ScalarWriter writer) {
      super(parent, key, writer);
    }

    @Override
    public boolean parse() {
      JsonToken token = loader.tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        setNull();
        break;
      case VALUE_STRING:
        writer.setString(loader.tokenizer.stringValue());
        break;
      default:
        throw loader.syntaxError(token, key(), "String");
      }
      return true;
    }
  }

  /**
   * Parses "str" | true | false | integer | float
   * <p>
   * Returns the result as a string.
   * <p>
   * If the top level value is a scalar:
   * <ul>
   * <li><tt>null</tt> is mapped to a Drill NULL.</li>
   * <li>Strings are stored in Drill unquoted.</li>
   * </ul>
   * If the top level is a map or array, then scalars within
   * that structure:
   * <ul>
   * <li><tt>null</tt> is mapped to the literal "null".</li>
   * <li>Strings are quoted.</li>
   * </ul>
   */

  public static class TextParser extends ScalarParser {

    private final boolean isArray;

    public TextParser(JsonElementParser parent, String key,
        ScalarWriter writer) {
      super(parent, key, writer);
      this.isArray = writer.schema().isArray();
    }

    @Override
    public boolean parse() {
      JsonToken token = loader.tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        if (isArray) {
          try {
            writer.setString("");
          } catch (UnsupportedConversionError e) {
            throw loader.typeError(e);
          }
        } else {
          setNull();
        }
        return true;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE:
      case VALUE_TRUE:
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
      case VALUE_STRING:
        writer.setString(loader.tokenizer.textValue());
        return true;

      default:
        throw loader.syntaxError(token);
      }
    }
  }

  protected final ScalarWriter writer;

  public ScalarParser(JsonElementParser parent, String key, ScalarWriter writer) {
    super(parent, key);
    this.writer = writer;
  }

  public void setNull() {
    try {
      writer.setNull();
    } catch (UnsupportedConversionError e) {
      throw loader.typeError(e, "Null value encountered in JSON input where Drill does not allow nulls.");
    }
  }

  @Override
  public ColumnMetadata schema() { return writer.schema(); }
}
