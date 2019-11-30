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
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonElementParser;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Implement a JSON array as a Drill list. The list introduces a new level of container.
 *
 * @param parent
 * @param token
 * @param key
 * @return
 */

public class ListParser extends ArrayParser {

  /**
   * Represents a rather odd state: we have seen a value of one or more nulls,
   * but we have not yet seen a value that would give us a type. This state
   * acts as a placeholder; waiting to see the type, at which point it replaces
   * itself with the actual typed state. If a batch completes with only nulls
   * for this field, then the field becomes a Text field and all values in
   * subsequent batches will be read in "text mode" for that one field in
   * order to avoid a schema change.
   * <p>
   * Note what this state does <i>not</i> do: it does not create a nullable
   * int field per Drill's normal (if less than ideal) semantics. First, JSON
   * <b>never</b> produces an int field, so nullable int is less than ideal.
   * Second, nullable int has no basis in reality and so is a poor choice
   * on that basis.
   */

  protected static class NullElementParser extends AbstractParser.LeafParser
      implements JsonLoaderImpl.NullTypeMarker {

    private final ListParser listParser;

    public NullElementParser(ListParser parentState, String fieldName) {
      super(parentState, fieldName);
      this.listParser = parentState;
      loader.addNullMarker(this);
    }

    @Override
    public boolean parse() {
      JsonToken token = loader.tokenizer.requireNext();

      // If value is the null token, we still don't know the type.

      if (token == JsonToken.VALUE_NULL) {
        return true;
      }

      // Replace this parser with a typed parser.

      JsonLoaderImpl.JsonElementParser newParser = listParser.detectElementParser(token);
      loader.tokenizer.unget(token);
      return resolve(newParser).parse();
    }

    @Override
    public void forceResolution() {
      JsonElementParser newParser = listParser.inferElementFromHint();
      if (newParser != null) {
        JsonLoaderImpl.logger.info("Using hints to determine type of JSON list {}. " +
            " Found type {}",
            fullName(), newParser.schema().type().name());
      } else {
        JsonLoaderImpl.logger.warn("Ambiguous type! JSON list {}" +
            " contains all nulls. Assuming text mode.",
            fullName());
        ObjectWriter elementWriter = listParser.newWriter(MinorType.VARCHAR);
        newParser = new ScalarParser.TextParser(listParser, key(), elementWriter.scalar());
      }
      resolve(newParser);
    }

    private JsonElementParser resolve(JsonElementParser newParser) {
      listParser.replaceChild(newParser);
      loader.removeNullMarker(this);
      return newParser;
    }

    @Override
    public boolean isAnonymous() { return true; }

    @Override
    public ColumnMetadata schema() { return null; }
  }

  private JsonElementParser elementParser;

  public ListParser(ContainerParser parent, String key, ArrayWriter writer) {
    super(parent, key, writer);
    elementParser = new NullElementParser(this, key);
  }

  @Override
  protected void parseContents(JsonToken token) {
    elementParser.parse();
  }

  /**
   * Figure out which element parser is needed for this list based on the
   * first token in the list. If the token is <tt>null</tt>, then use a temporary
   * type-deferral parser.
   *
   * @param token token of first item in the list
   * @return parser for the elements in the list
   */

  protected JsonElementParser detectElementParser(JsonToken token) {
    String childKey = key() + "[]";
    switch (token) {
    case START_ARRAY:
      return new ListParser(this, childKey,
          newWriter(MinorType.LIST).array());

    case START_OBJECT:
      return objectElementParser(childKey);

    case VALUE_NULL:
      JsonElementParser elementParser = inferFromType();
      if (elementParser == null) {
        // Don't know what this is a list of yet.

        elementParser = new NullElementParser(this, childKey);
      }
      return elementParser;

    default:
      return detectScalarElementParser(token, childKey);
    }
  }

  /**
   * Determine which scalar element parser to use based on the JSON token
   * for the first scalar element.
   *
   * @param token token seen in the JSON stream
   * @param key name of the field
   * @return an element parser for the scalar
   */

  private JsonElementParser detectScalarElementParser(JsonToken token, String key) {
    MinorType type = typeForToken(token);
    ScalarWriter childWriter = newWriter(type).scalar();
    return scalarParserForToken(token, key, childWriter);
  }

  protected ObjectParser objectElementParser(String key) {
    TupleWriter tupleWriter = newWriter(MinorType.MAP).tuple();
    return new ObjectParser(this, key, tupleWriter);
  }

  /**
   * Create a writer to be used for array elements.
   *
   * @param type the inferred Drill type for the elements
   * @return a column writer of the requested type
   */

  protected ObjectWriter newWriter(MinorType type) {
    VariantWriter variant = writer.variant();
    if (variant.hasType(type) || variant.variantSchema().size() == 0) {
      return variant.member(type);
    }
    if (! loader.options.unionEnabled) {
      throw loader.syntaxError(
          String.format("JSON array for column %s is of type %s, " +
                        "but a conflicting type of %s found. " +
                        "Consider enabling the union type.",
              key(),
              variant.variantSchema().types().iterator().next(),
              type.name()));
    }
    return variant.member(type);
  }

  /**
   * If the experimental type system is enabled, infer the type of a null column
   * using the type system.
   *
   * @return the parser state, if any, created from the hint
   */

  private JsonElementParser inferFromType() {
    if (! loader.options.detectTypeEarly) {
      return null;
    }
    return inferElementFromHint();
  }

  private JsonElementParser inferElementFromHint() {
    if (loader.options.typeNegotiator == null) {
      return null;
    }
    MajorType type = loader.options.typeNegotiator.typeOf(makePath());
    if (type == null) {
      return null;
    }
    if (type.getMode() != DataMode.REPEATED) {
      JsonLoaderImpl.logger.warn("Cardinality conflict! JSON source {}, list {} " +
          "has a hint of cardinality {}, but JSON requires REPEATED. " +
          "Hint ignored.",
          loader.options.context, fullName(), type.getMode().name());
    }
    String key = key() + "[]";
    ObjectWriter elementWriter = newWriter(type.getMinorType());
    switch (type.getMinorType()) {
    case MAP:
      return new ObjectParser(this, key, elementWriter.tuple());

    case LIST:
      return new ListParser(this, key, elementWriter.array());

    default:
      return scalarParserForType(type.getMinorType(), key, elementWriter.scalar());
    }
  }

  protected void replaceChild(JsonElementParser newParser) {
    elementParser = newParser;
  }
}
