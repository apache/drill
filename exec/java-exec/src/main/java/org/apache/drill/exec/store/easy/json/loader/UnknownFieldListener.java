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
package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.NullTypeMarker;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a rather odd state: we have seen a value of one or more
 * {@code null}s or empty arrays ({@code []}), but we have not yet seen a
 * value that would give us a type. This listener
 * acts as a placeholder; waiting to see the type, at which point it replaces
 * itself with the actual typed listener. If a batch completes with only nulls
 * for this field, then the field becomes a {@code VARCHAR} field. Drill's "fill
 * empties" logic will back-fill nulls. All values in
 * subsequent batches will be read in "text mode" for that one field in
 * order to avoid a schema change.
 * <p>
 * Note what this listener does <i>not</i> do: it does not create a nullable
 * int field per Drill's normal (if less than ideal) semantics. First, JSON
 * <b>never</b> produces an int field, so nullable int is less than ideal.
 * Second, nullable int has no basis in reality and so is a poor choice
 * on that basis.
 * <p>
 * Note that we <i>cannot</i> use this class for an array that
 * contains nulls: {@code [null]}. The null is a value that must be
 * stored, so we must guess the type as we have no good way to count
 * array entries except via vectors.
 */
public class UnknownFieldListener extends AbstractValueListener implements NullTypeMarker {
  protected static final Logger logger = LoggerFactory.getLogger(UnknownFieldListener.class);

  protected final TupleListener parentTuple;
  protected final String key;
  protected ValueHost host;
  private UnknownArrayListener unknownArray;

  public UnknownFieldListener(TupleListener parentTuple, String key) {
    super(parentTuple.loader());
    this.parentTuple = parentTuple;
    this.key = key;
    loader.addNullMarker(this);
  }

  @Override
  public void bind(ValueHost host) {
    this.host = host;
  }

  @Override
  public void onNull() {
    if (unknownArray != null) {
      // An array, must resolve to some type.
      resolveScalar(JsonType.NULL).onNull();
    }
    // Else ignore: still don't know what this is
  }

  @Override
  public void onBoolean(boolean value) {
    resolveScalar(JsonType.BOOLEAN).onBoolean(value);
  }

  @Override
  public void onInt(long value) {
    resolveScalar(JsonType.INTEGER).onInt(value);
  }

  @Override
  public void onFloat(double value) {
    resolveScalar(JsonType.FLOAT).onFloat(value);
  }

  @Override
  public void onString(String value) {
    resolveScalar(JsonType.STRING).onString(value);
  }

  @Override
  public void onEmbeddedObject(String value) {
    resolveScalar(JsonType.EMBEDDED_OBJECT).onEmbeddedObject(value);
  }

  @Override
  public ObjectListener object() {
    return resolveScalar(JsonType.OBJECT).object();
  }

  /**
   * The column type is now known from context. Create a new, scalar
   * column, writer and listener to replace ourself: this is the last
   * call that this listener will receive.
   */
  protected ValueListener resolveScalar(JsonType type) {
    if (unknownArray == null) {
      return resolveTo(parentTuple.scalarListenerFor(key, type));
    } else {

      // Saw {a: []}, {a: 10}. Since we infer that 10 is a
      // single-element array, resolve to an array, then send
      // the value to the element.
      return unknownArray.element(new ValueDef(type, 0));
    }
  }

  @Override
  protected ColumnMetadata schema() {
    throw new IllegalStateException("Unknown column has no schema");
  }

  @Override
  public ArrayListener array(ValueDef valueDef) {
    if (valueDef.dimensions() > 1) {

      // if 2D+ array, then we know enough to choose a Repeated list
      return resolveToArray(valueDef).array(valueDef);
    }
    if (unknownArray == null) {
      unknownArray = new UnknownArrayListener(this);
    }
    return unknownArray;
  }

  protected ValueListener resolveTo(ValueListener newListener) {
    host.bindListener(newListener);
    loader.removeNullMarker(this);
    return newListener;
  }

  @Override
  public void forceResolution() {
    if (unknownArray == null) {
      logger.warn("Ambiguous type! JSON field {}" +
          " contains all nulls. Assuming VARCHAR.", key);
      resolveTo(parentTuple.scalarListenerFor(key, JsonType.STRING));
    } else {
      logger.warn("Ambiguous type! JSON array field {}" +
          " contains all empty arrays. Assuming repeated VARCHAR.", key);
      resolveTo(parentTuple.arrayListenerFor(key, JsonType.STRING));
    }
  }

  public ValueListener resolveToArray(ValueDef valueDef) {
    if (valueDef.type().isUnknown()) {
      logger.warn("Ambiguous type! JSON array field {}" +
          " starts with null element. Assuming repeated VARCHAR.", key);
      valueDef = new ValueDef(JsonType.STRING, valueDef.dimensions());
    }
    return resolveTo(parentTuple.listenerFor(key, valueDef));
  }

  /**
   * An unknown array within the unknown field. Represents an
   * empty array: {@code []}. Resolves to a specific type upon
   * presentation of the first element. If that element is
   * {@code null}, must still choose a type to record nulls.
   * <p>
   * This array listener holds no element since none has been
   * created yet; we use this only while we see empty arrays.
   */
  public static class UnknownArrayListener implements ArrayListener {

    private final UnknownFieldListener parent;

    public UnknownArrayListener(UnknownFieldListener parent) {
      this.parent = parent;
    }

    @Override
    public void onStart() { }

    @Override
    public void onElementStart() { }

    @Override
    public void onElementEnd() { }

    @Override
    public void onEnd() { }

    /**
     * Saw the first actual element. Swap out the field listener
     * for a real array, then return the new element listener.
     */
    @Override
    public ValueListener element(ValueDef valueDef) {
      ValueDef arrayDef = new ValueDef(valueDef.type(), valueDef.dimensions() + 1);
      return parent.resolveToArray(arrayDef)
          .array(arrayDef)
          .element(valueDef);
    }
  }
}
