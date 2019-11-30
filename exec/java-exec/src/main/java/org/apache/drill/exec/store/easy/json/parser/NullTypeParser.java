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
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonElementParser;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.NullTypeMarker;

import com.fasterxml.jackson.core.JsonToken;

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

class NullTypeParser extends AbstractParser.LeafParser implements NullTypeMarker {

  private final ObjectParser objectParser;

  public NullTypeParser(ObjectParser parentState, String fieldName) {
    super(parentState, fieldName);
    this.objectParser = parentState;
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

    loader.tokenizer.unget(token);
    return resolve(objectParser.detectValueParser(key())).parse();
  }

  @Override
  public void forceResolution() {
    JsonElementParser newParser = objectParser.inferMemberFromHint(makePath());
    if (newParser != null) {
      JsonLoaderImpl.logger.info("Using hints to determine type of JSON field {}. " +
          " Found type {}",
          fullName(), newParser.schema().type().name());
    } else {
      JsonLoaderImpl.logger.warn("Ambiguous type! JSON field {}" +
          " contains all nulls. Assuming VARCHAR.",
          fullName());
      newParser = new ScalarParser.TextParser(parent(), key(),
          objectParser.newWriter(key(), MinorType.VARCHAR, DataMode.OPTIONAL).scalar());
    }
    resolve(newParser);
  }

  private JsonElementParser resolve(JsonElementParser newParser) {
    objectParser.replaceChild(key(), newParser);
    loader.removeNullMarker(this);
    return newParser;
  }

  @Override
  public ColumnMetadata schema() { return null; }
}
