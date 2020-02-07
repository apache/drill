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

import com.fasterxml.jackson.core.JsonToken;

/**
 * Constructs a value parser and its listeners by looking ahead
 * some number of tokens to "sniff" the type of the value. For
 * example:
 * <ul>
 * <li>{@code foo: <value>} - Field value</li>
 * <li>{@code foo: [ <value> ]} - 1D array value</li>
 * <li>{@code foo: [ [<value> ] ]} - 2D array value</li>
 * <li>Etc.</li>
 * </ul>
 * <p>
 * There are two cases in which no type estimation is possible:
 * <ul>
 * <li>The value is {@code null}, indicated by
 * {@link JsonType#NULL}.</code>
 * <li>The value is an array, and the array is empty, indicated
 * by {@link JsonType#EMPTY}.</li>
 * </ul>
 * This class handles syntactic type inference. The associated
 * listener enforces semantic rules. For example, if a schema is
 * available, and we know that field "x" must be an Integer, but
 * this class reports that it is an object, then the listener should
 * raise an exception.
 * <p>
 * Also, the parser cannot enforce type consistency. This class
 * looks only at the first appearance of a value. JSON allows anything.
 * The listener must enforce semantic rules that say whether a different
 * type is allowed for later values.
 */
public class ValueFactory {

  public static class FieldDescrip {
    protected int arrayDims;
    protected JsonType type;

    public boolean isArray() { return arrayDims > 0; }

    public boolean isObject() { return type == JsonType.OBJECT; }
  }

  private ValueFactory() { }

  /**
   * Parse position: <code>{ ... field : ^ ?</code> for a newly-seen field.
   * Look ahead to guess the field type, then declare the field.
   *
   * @param parent the object parser declaring the field
   * @param key the name of the field
   * @param tokenizer the token parser
   * @return the value parser for the element, which may contain additional
   * structure for objects or arrays
   */
  public static ElementParser createFieldParser(ObjectParser parent, String key, TokenIterator tokenizer) {
    FieldDescrip descrip = new FieldDescrip();
    inferFieldType(descrip, tokenizer);
    ObjectListener objListener = parent.listener();
    ValueListener fieldListener;
    if (descrip.isObject()) {
      if (descrip.isArray()) {
        // Object array field
        fieldListener = objListener.addObjectArray(key, descrip.arrayDims);
      } else {
        // Object field
        fieldListener = objListener.addObject(key);
      }
    } else {
      if (descrip.isArray()) {
        // Scalar (or unknown) array field
        fieldListener = objListener.addArray(key, descrip.arrayDims, descrip.type);
      } else {
        // Scalar field
        fieldListener = objListener.addScalar(key, descrip.type);
      }
    }
    ValueParser fp = new ValueParser(parent, key, fieldListener);
    createStructureParser(fp, descrip);
    return fp;
  }

  /**
   * Add the object or array parser, if the structured type is known.
   */
  private static void createStructureParser(ValueParser valueParser,
      FieldDescrip descrip) {
    if (descrip.isArray()) {
      valueParser.bindArrayParser(createArrayParser(valueParser, descrip));
    } else if (descrip.isObject()) {
      valueParser.bindObjectParser(objectParser(valueParser));
    }
  }

  /**
   * Parse position: <code>... [ ?</code> for a field or array element not previously
   * known to be an array. Look ahead to determine if the array is nested and its
   * element types.
   *
   * @param parent the parser for the value that has been found to contain an
   * array
   * @param tokenizer the JSON token parser
   * @return an array parser to bind to the parent value parser to parse the
   * array
   */
  public static ArrayParser createArrayParser(ValueParser parent, TokenIterator tokenizer) {
    FieldDescrip descrip = new FieldDescrip();
    // Already in an array, so add the outer dimension.
    descrip.arrayDims++;
    inferFieldType(descrip, tokenizer);
    return createArrayParser(parent, descrip);
  }

  public static ArrayParser createArrayParser(ValueParser parent, FieldDescrip descrip) {
    ValueListener fieldListener = parent.listener();
    ArrayListener arrayListener;
    if (descrip.isObject()) {
      // Object array elements
      arrayListener = fieldListener.objectArray(descrip.arrayDims);
    } else {
      arrayListener = fieldListener.array(descrip.arrayDims, descrip.type);
    }
    descrip.arrayDims--;
    ValueListener elementListener;
    if (descrip.isObject()) {
      if (descrip.isArray()) {
        // Object array elements
        elementListener = arrayListener.objectArrayElement(descrip.arrayDims);
      } else {
        // Object elements
        elementListener = arrayListener.objectElement();
      }
    } else {
      if (descrip.isArray()) {
        // Scalar (or unknown) array elements
        elementListener = arrayListener.arrayElement(descrip.arrayDims, descrip.type);
      } else {
        // Scalar elements
        elementListener = arrayListener.scalarElement(descrip.type);
      }
    }
    ArrayParser arrayParser = new ArrayParser(parent, arrayListener, elementListener);
    createStructureParser(arrayParser.elementParser(), descrip);
    return arrayParser;
  }

  public static ObjectParser objectParser(ValueParser parent) {
    ValueListener valueListener = parent.listener();
    ObjectListener objListener = valueListener.object();
    return new ObjectParser(parent, objListener);
  }

  protected static void inferFieldType(FieldDescrip descrip, TokenIterator tokenizer) {
    JsonToken token = tokenizer.requireNext();
    switch (token) {
      case START_ARRAY:
        // Position: key: [ ^
        descrip.arrayDims++;
        inferFieldType(descrip, tokenizer);
        break;

      case END_ARRAY:
        if (descrip.arrayDims == 0) {
          throw tokenizer.errorFactory().syntaxError(token);
        }
        descrip.type = JsonType.EMPTY;
        break;

      case START_OBJECT:
        // Position: key: { ^
        descrip.type = JsonType.OBJECT;
        break;

      case VALUE_NULL:

        // Position: key: null ^
        descrip.type = JsonType.NULL;
        break;

      case VALUE_FALSE:
      case VALUE_TRUE:
        descrip.type = JsonType.BOOLEAN;
        break;

      case VALUE_NUMBER_INT:
        descrip.type = JsonType.INTEGER;
        break;

      case VALUE_NUMBER_FLOAT:
        descrip.type = JsonType.FLOAT;
        break;

      case VALUE_STRING:
        descrip.type = JsonType.STRING;
        break;

      default:
        // Won't get here: the Jackson parser catches
        // errors.
        throw tokenizer.errorFactory().syntaxError(token);
    }
    tokenizer.unget(token);
  }
}
