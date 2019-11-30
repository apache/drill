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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonElementParser;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

import com.fasterxml.jackson.core.JsonToken;

public abstract class ContainerParser extends AbstractParser {

  public ContainerParser(JsonLoaderImpl loader, String key) {
    super(loader, key);
  }

  public ContainerParser(JsonElementParser parent, String key) {
    super(parent, key);
  }

  /**
   * The field type has been determined. Build a writer for that field given
   * the field name, type and mode (optional or repeated). The result set loader
   * that backs this JSON loader will handle field projection, returning a dummy
   * parser if the field is not projected.
   *
   * @param key name of the field
   * @param type Drill data type
   * @param mode cardinality: either Optional (for map fields) or Repeated
   * (for array members). (JSON does not allow Required fields)
   * @return the object writer for the field, which may be a tuple, scalar
   * or array writer, depending on type
   */

  protected abstract ObjectWriter newWriter(String key,
      MinorType type, DataMode mode);

  /**
   * Create a parser for a scalar array implemented as a repeated type.
   *
   * @param parent
   * @param token
   * @param key
   * @return
   */

  protected JsonElementParser detectScalarArrayParser(JsonToken token, String key) {
    MinorType type = typeForToken(token);
    ArrayWriter arrayWriter = newWriter(key, type, DataMode.REPEATED).array();
    JsonElementParser elementState = scalarParserForToken(token, key, arrayWriter.scalar());
    return new ArrayParser.ScalarArrayParser(this, key, arrayWriter, elementState);
  }

  protected ObjectParser objectParser(String key) {
    return new ObjectParser(this, key,
        newWriter(key, MinorType.MAP, DataMode.REQUIRED).tuple());
  }

  protected JsonElementParser typedScalar(JsonToken token, String key) {
    MinorType type = typeForToken(token);
    ScalarWriter scalarWriter = newWriter(key, type, DataMode.OPTIONAL).scalar();
    return scalarParserForToken(token, key, scalarWriter);
  }

  protected ArrayParser objectArrayParser(String key) {
    ArrayWriter arrayWriter = newWriter(key, MinorType.MAP, DataMode.REPEATED).array();
    return new ArrayParser.ObjectArrayParser(this, key, arrayWriter,
        new ObjectParser(this, key, arrayWriter.tuple()));
  }

  protected JsonElementParser listParser(String key) {
    return new ListParser(this, key,
        newWriter(key, MinorType.LIST, DataMode.OPTIONAL).array());
  }

  protected RepeatedListParser repeatedListParser(String key) {
     return new RepeatedListParser(this, key,
         newWriter(key, MinorType.LIST, DataMode.REPEATED).array());
  }

  protected JsonElementParser textArrayParser(String key) {
    ObjectWriter childWriter = newWriter(key(),
        MinorType.VARCHAR, DataMode.REPEATED);
    JsonElementParser elementParser = new ScalarParser.TextParser(
        this, key(), childWriter.array().scalar());
    return new ArrayParser.ScalarArrayParser(this,
        key(), childWriter.array(), elementParser);
  }

  /**
   * Detect the type of an array member by "sniffing" the first element.
   * Creates a simple repeated type if requested and possible. Else, creates
   * an array depending on the array contents. May have to look ahead
   * multiple tokens if the array is multi-dimensional.
   * <p>
   * Note that repeated types can only appear directly inside maps; they
   * cannot be used inside a list.
   *
   * @param parent the object parser that will hold the array element
   * @param key field name
   * @return the parse state for this array
   */

  protected JsonElementParser detectArrayParser(String key) {

    // If using the experimental list support, create a parser
    // for that mode. (Lists are generic and require much less fiddling
    // than the repeated type/repeated list path.)

    if (loader.options.useListType) {
      return listParser(key);
    }

    // Implement JSON lists using the standard repeated types or the
    // repeated list type.
    // Detect the type of that array. Or, detect that the the first value
    // dictates that a list be used because this is a nested list, contains
    // nulls, etc.

    JsonToken token = loader.tokenizer.requireNext();
    JsonElementParser listParser;
    switch (token) {
    case START_ARRAY:

      // Can't use an array, must use a list since this is nested
      // or null.

      listParser = repeatedListParser(key);
      break;

    case VALUE_NULL:
    case END_ARRAY:

      // Don't know what this is. Defer judgment until later.

      listParser = nullArrayParser(key);
      break;

    case START_OBJECT:
      listParser = objectArrayParser(key);
      break;

    default:
      listParser = detectScalarArrayParser(token, key);
      break;
    }
    loader.tokenizer.unget(token);
    return listParser;
  }

  protected abstract JsonElementParser nullArrayParser(String key);

  public MinorType typeForToken(JsonToken token) {
    if (loader.options.allTextMode) {
      return MinorType.VARCHAR;
    }
    switch (token) {
    case VALUE_FALSE:
    case VALUE_TRUE:
      return MinorType.BIT;

    case VALUE_NUMBER_INT:
      if (! loader.options.readNumbersAsDouble) {
        return MinorType.BIGINT;
      } // else fall through

    case VALUE_NUMBER_FLOAT:
      return MinorType.FLOAT8;

    case VALUE_STRING:
      return MinorType.VARCHAR;

    default:
      throw loader.syntaxError(token);
    }
  }

  public AbstractParser scalarParserForToken(JsonToken token, String key, ScalarWriter writer) {
    if (loader.options.allTextMode) {
      return new ScalarParser.TextParser(this, key, writer);
    }
    switch (token) {
    case VALUE_FALSE:
    case VALUE_TRUE:
      return new ScalarParser.BooleanParser(this, key, writer);

    case VALUE_NUMBER_INT:
      if (! loader.options.readNumbersAsDouble) {
        return new ScalarParser.IntParser(this, key, writer);
      } // else fall through

    case VALUE_NUMBER_FLOAT:
      return new ScalarParser.FloatParser(this, key, writer);

    case VALUE_STRING:
      return new ScalarParser.StringParser(this, key, writer);

    default:
      throw loader.syntaxError(token);
    }
  }

  protected ScalarParser scalarParserForType(MinorType type, String key, ScalarWriter scalarWriter) {
    switch (type) {
    case BIGINT:
      return new ScalarParser.IntParser(this, key, scalarWriter);
    case FLOAT8:
      return new ScalarParser.FloatParser(this, key, scalarWriter);
    case TINYINT:
      return new ScalarParser.BooleanParser(this, key, scalarWriter);
    case VARCHAR:
      return new ScalarParser.StringParser(this, key, scalarWriter);
    default:
      throw loader.syntaxError("Unsupported Drill type " + type.toString());
    }
  }

  protected void replaceChild(String key, JsonElementParser newParser) {
    throw new UnsupportedOperationException();
  }
}
