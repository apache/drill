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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ObjectArrayListener;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ScalarArrayListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ArrayValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectArrayValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ScalarArrayValueListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Accepts { name : value ... }
 * <p>
 * The structure parser maintains a map of known fields. Each time a
 * field is parsed, looks up the field in the map. If not found, the parser
 * looks ahead to find a value token, if any, and calls this class to add
 * a new column. This class creates a column writer based either on the
 * type provided in a provided schema, or inferred from the JSON token.
 * <p>
 * As it turns out, most of the semantic action occurs at the tuple level:
 * that is where fields are defined, types inferred, and projection is
 * computed.
 *
 * <h4>Nulls</h4>
 *
 * Much code here deals with null types, especially leading nulls, leading
 * empty arrays, and so on. The object parser creates a parser for each
 * value; a parser which "does the right thing" based on the data type.
 * For example, for a Boolean, the parser recognizes {@code true},
 * {@code false} and {@code null}.
 * <p>
 * But what happens if the first value for a field is {@code null}? We
 * don't know what kind of parser to create because we don't have a schema.
 * Instead, we have to create a temporary placeholder parser that will consume
 * nulls, waiting for a real type to show itself. Once that type appears, the
 * null parser can replace itself with the correct form. Each vector's
 * "fill empties" logic will back-fill the newly created vector with nulls
 * for prior rows.
 * <p>
 * Two null parsers are needed: one when we see an empty list, and one for
 * when we only see {@code null}. The one for {@code null{@code  must morph into
 * the one for empty lists if we see:<br>
 * {@code {a: null} {a: [ ]  }}<br>
 * <p>
 * If we get all the way through the batch, but have still not seen a type,
 * then we have to guess. A prototype type system can tell us, otherwise we
 * guess {@code VARCHAR}. ({@code VARCHAR} is the right choice for all-text
 * mode, it is as good a guess as any for other cases.)
 *
 * <h4>Projection List Hints</h4>
 *
 * To help, we consult the projection list, if any, for a column. If the
 * projection is of the form {@code a[0]}, we know the column had better
 * be an array. Similarly, if the projection list has {@code b.c}, then
 * {@code b} had better be an object.
 *
 * <h4>Array Handling</h4>
 *
 * The code here handles arrays in two ways. JSON normally uses the
 * {@code LIST} type. But, that can be expensive if lists are
 * well-behaved. So, the code here also implements arrays using the
 * classic {@code REPEATED} types. The repeated type option is disabled
 * by default. It can be enabled, for efficiency, if Drill ever supports
 * a JSON schema. If an array is well-behaved, mark that column as able
 * to use a repeated type.
 *
 * <h4>Ambiguous Types</h4>
 *
 * JSON nulls are untyped. A run of nulls does not tell us what type will
 * eventually appear. The best solution is to provide a schema. Without a
 * schema, the code is forgiving: defers selection of the column type until
 * the first non-null value (or, forces a type at the end of the batch.)
 * <p>
 * For scalars the pattern is: <code>{a: null} {a: "foo"}</code>. Type
 * selection happens on the value {@code "foo"}.
 * <p>
 * For arrays, the pattern is: <code>{a: []} {a: ["foo"]}</code>. Type
 * selection happens on the first array element. Note that type selection
 * must happen on the first element, even if tha element is null (which,
 * as we just said, ambiguous.)
 * <p>
 * If we are forced to pick a type (because we hit the end of a batch, or
 * we see {@code [null]}, then we pick {@code VARCHAR} as we allow any
 * scalar to be converted to {@code VARCHAR}. This helps for a single-file
 * query, but not if multiple fragments each make their own (inconsistent)
 * decisions. Only a schema provides a consistent answer.
 */
public class TupleListener implements ObjectListener {

  protected final JsonLoaderImpl loader;
  protected final TupleWriter tupleWriter;
  private final TupleMetadata providedSchema;

  public TupleListener(JsonLoaderImpl loader, TupleWriter tupleWriter, TupleMetadata providedSchema) {
    this.loader = loader;
    this.tupleWriter = tupleWriter;
    this.providedSchema = providedSchema;
  }

  public JsonLoaderImpl loader() { return loader; }

  @Override
  public void onStart() { }

  @Override
  public void onEnd() { }

  @Override
  public FieldType fieldType(String key) {
    if (!tupleWriter.isProjected(key)) {
      return FieldType.IGNORE;
    }
    ColumnMetadata providedCol = providedColumn(key);
    if (providedCol == null) {
      return FieldType.TYPED;
    }
    String mode = providedCol.property(JsonLoader.JSON_MODE);
    if (mode == null) {
      return FieldType.TYPED;
    }
    switch (mode) {
      case JsonLoader.JSON_TEXT_MODE:
        return FieldType.TEXT;
      case JsonLoader.JSON_LITERAL_MODE:
        return FieldType.JSON;
      default:
        return FieldType.TYPED;
    }
  }

  /**
   * Add a field not seen before. If a schema is provided, use the provided
   * column schema to define the column. Else, build the column based on the
   * look-ahead hints provided by the structure parser.
   */
  @Override
  public ValueListener addField(String key, ValueDef valueDef) {
    ColumnMetadata providedCol = providedColumn(key);
    if (providedCol != null) {
      return listenerForSchema(providedCol);
    } else {
      return listenerForValue(key, valueDef);
    }
  }

  public ColumnMetadata providedColumn(String key) {
    return providedSchema == null ? null : providedSchema.metadata(key);
  }

  /**
   * Build a column and its listener based a provided schema.
   * The user is responsible to ensure that the provided schema
   * accurately reflects the structure of the JSON being parsed.
   */
  private ValueListener listenerForSchema(ColumnMetadata providedCol) {
    switch (providedCol.structureType()) {

      case PRIMITIVE: {
        ColumnMetadata colSchema = providedCol.copy();
        if (providedCol.isArray()) {
          return scalarArrayListenerFor(colSchema);
        } else {
          return scalarListenerFor(colSchema);
        }
      }

      case TUPLE: {
        // Propagate the provided map schema into the object
        // listener as a provided tuple schema.
        ColumnMetadata colSchema = providedCol.cloneEmpty();
        TupleMetadata providedSchema = providedCol.tupleSchema();
        if (providedCol.isArray()) {
          return objectArrayListenerFor(colSchema, providedSchema);
        } else {
          return objectListenerFor(colSchema, providedSchema);
        }
      }

      case VARIANT: {
        // A variant can contain multiple types. The schema does not
        // declare the types; rather they are discovered by the reader.
        // That is, there is no VARIANT<INT, DOUBLE>, there is just VARIANT.
        ColumnMetadata colSchema = providedCol.cloneEmpty();
        if (providedCol.isArray()) {
          return variantArrayListenerFor(colSchema);
        } else {
          return variantListenerFor(colSchema);
        }
      }

      case MULTI_ARRAY:
        return multiDimArrayListenerForSchema(providedCol);

      default:
        throw loader.unsupportedType(providedCol);
    }
  }

  /**
   * Build a column and its listener based on a look-ahead hint.
   */
  protected ValueListener listenerForValue(String key, ValueDef valueDef) {
    if (!valueDef.isArray()) {
      if (valueDef.type().isUnknown()) {
        return unknownListenerFor(key);
      } else if (valueDef.type().isObject()) {
        return objectListenerForValue(key);
      } else {
        return scalarListenerForValue(key, valueDef.type());
      }
    } else if (valueDef.dimensions() == 1) {
      if (valueDef.type().isUnknown()) {
        return unknownArrayListenerFor(key, valueDef);
      } else if (valueDef.type().isObject()) {
        return objectArrayListenerForValue(key);
      } else {
        return scalarArrayListenerForValue(key, valueDef.type());
      }
    } else {
      if (valueDef.type().isUnknown()) {
        return unknownArrayListenerFor(key, valueDef);
      } else if (valueDef.type().isObject()) {
        return multiDimObjectArrayListenerForValue(key, valueDef);
      } else {
        return multiDimScalarArrayListenerForValue(key, valueDef);
      }
    }
  }

  /**
   * Create a scalar column and listener given the definition of a JSON
   * scalar value.
   */
  public ScalarListener scalarListenerForValue(String key, JsonType jsonType) {
    return scalarListenerFor(MetadataUtils.newScalar(key,
        Types.optional(scalarTypeFor(key, jsonType))));
  }

  /**
   * Create a scalar column and listener given the column schema.
   */
  public ScalarListener scalarListenerFor(ColumnMetadata colSchema) {
    return ScalarListener.listenerFor(loader, addFieldWriter(colSchema));
  }

  /**
   * Create a scalar array column and listener given the definition of a JSON
   * array of scalars.
   */
  public ArrayValueListener scalarArrayListenerForValue(String key, JsonType jsonType) {
    return scalarArrayListenerFor(MetadataUtils.newScalar(key,
        Types.repeated(scalarTypeFor(key, jsonType))));
  }

  /**
   * Create a multi- (2+) dimensional scalar array from a JSON value description.
   */
  private ValueListener multiDimScalarArrayListenerForValue(String key, ValueDef valueDef) {
    return multiDimScalarArrayListenerFor(
        repeatedListSchemaFor(key, valueDef.dimensions(),
            MetadataUtils.newScalar(key, scalarTypeFor(key, valueDef.type()), DataMode.REPEATED)),
        valueDef.dimensions());
  }

  /**
   * Create a multi- (2+) dimensional scalar array from a column schema and dimension
   * count hint.
   */
  private ValueListener multiDimScalarArrayListenerFor(ColumnMetadata colSchema, int dims) {
    return RepeatedListValueListener.multiDimScalarArrayFor(loader,
        addFieldWriter(colSchema), dims);
  }

  /**
   * Create a scalar array column and array listener for the given column
   * schema.
   */
  public ArrayValueListener scalarArrayListenerFor(ColumnMetadata colSchema) {
    return new ScalarArrayValueListener(loader, colSchema,
        new ScalarArrayListener(loader, colSchema,
            scalarListenerFor(colSchema)));
  }

  /**
   * Create a map column and its associated object value listener for the
   * a JSON object value given the value's key.
   */
  public ObjectValueListener objectListenerForValue(String key) {
    ColumnMetadata colSchema = MetadataUtils.newMap(key);
    return objectListenerFor(colSchema, colSchema.tupleSchema());
  }

  /**
   * Create a map column and its associated object value listener for the
   * given key and optional provided schema.
   */
  public ObjectValueListener objectListenerFor(ColumnMetadata colSchema, TupleMetadata providedSchema) {
    return new ObjectValueListener(loader, colSchema,
        new TupleListener(loader, addFieldWriter(colSchema).tuple(),
            providedSchema));
  }

  /**
   * Create a map array column and its associated object array listener
   * for the given key.
   */
  public ArrayValueListener objectArrayListenerForValue(String key) {
    ColumnMetadata colSchema = MetadataUtils.newMapArray(key);
    return objectArrayListenerFor(colSchema, colSchema.tupleSchema());
  }

  /**
   * Create a map array column and its associated object array listener
   * for the given column schema and optional provided schema.
   */
  public ArrayValueListener objectArrayListenerFor(
      ColumnMetadata colSchema, TupleMetadata providedSchema) {
    ArrayWriter arrayWriter = addFieldWriter(colSchema).array();
    return new ObjectArrayValueListener(loader, colSchema,
        new ObjectArrayListener(loader, arrayWriter,
            new ObjectValueListener(loader, colSchema,
                new TupleListener(loader, arrayWriter.tuple(), providedSchema))));
  }

  /**
   * Create a RepeatedList which contains (empty) Map objects using the provided
   * schema. That is, create a multi-dimensional array of maps.
   * The map fields are created on the fly, optionally using the provided schema.
   */
  private ValueListener multiDimObjectArrayListenerForValue(String key, ValueDef valueDef) {
    return multiDimObjectArrayListenerFor(
        repeatedListSchemaFor(key, valueDef.dimensions(),
            MetadataUtils.newMapArray(key)),
        valueDef.dimensions(), null);
  }

  /**
   * Create a multi- (2+) dimensional scalar array from a column schema, dimension
   * count hint, and optional provided schema.
   */
  private ValueListener multiDimObjectArrayListenerFor(ColumnMetadata colSchema,
      int dims, TupleMetadata providedSchema) {
    return RepeatedListValueListener.multiDimObjectArrayFor(loader,
        addFieldWriter(colSchema), dims, providedSchema);
  }

  /**
   * Create a variant (UNION) column and its associated listener given
   * a column schema.
   */
  private ValueListener variantListenerFor(ColumnMetadata colSchema) {
    return new VariantListener(loader, addFieldWriter(colSchema).variant());
  }

  /**
   * Create a variant array (LIST) column and its associated listener given
   * a column schema.
   */
  private ValueListener variantArrayListenerFor(ColumnMetadata colSchema) {
    return new ListListener(loader, addFieldWriter(colSchema));
  }

  /**
   * Create a RepeatedList which contains Unions. (Actually, this is an
   * array of List objects internally.) The variant is variable, it makes no
   * sense to specify a schema for the variant. Also, omitting the schema
   * save a large amount of complexity that will likely never be needed.
   */
  @SuppressWarnings("unused")
  private ValueListener repeatedListOfVariantListenerFor(String key, ValueDef valueDef) {
    return multiDimVariantArrayListenerFor(
        MetadataUtils.newVariant(key, DataMode.REPEATED),
        valueDef.dimensions());
  }

  /**
   * Create a multi- (2+) dimensional variant array from a column schema and dimension
   * count hint. This is actually an (n-1) dimensional array of lists, where a LISt
   * is a repeated UNION.
   */
  private ValueListener multiDimVariantArrayListenerFor(ColumnMetadata colSchema, int dims) {
    return RepeatedListValueListener.repeatedVariantListFor(loader,
        addFieldWriter(colSchema));
  }

  /**
   * Create a repeated list column and its multiple levels of inner structure
   * from a provided schema. Repeated lists can nest to any number of levels to
   * provide any number of dimensions. In general, if an array is <i>n</i>-dimensional,
   * then there are <i>n</i>-1 repeated lists with some array type as the
   * innermost dimension.
   */
  private ValueListener multiDimArrayListenerForSchema(ColumnMetadata providedSchema) {
    // Parse the stack of repeated lists to count the "outer" dimensions and
    // to locate the innermost array (the "list" which is "repeated").
    int dims = 1; // For inner array
    ColumnMetadata elementSchema = providedSchema;
    while (MetadataUtils.isRepeatedList(elementSchema)) {
      dims++;
      elementSchema = elementSchema.childSchema();
      Preconditions.checkArgument(elementSchema != null);
    }

    ColumnMetadata colSchema = repeatedListSchemaFor(providedSchema.name(), dims,
        elementSchema.cloneEmpty());
    switch (elementSchema.structureType()) {

      case PRIMITIVE:
        return multiDimScalarArrayListenerFor(colSchema, dims);

      case TUPLE:
        return multiDimObjectArrayListenerFor(colSchema,
            dims, elementSchema.tupleSchema());

      case VARIANT:
        return multiDimVariantArrayListenerFor(colSchema, dims);

      default:
        throw loader.unsupportedType(providedSchema);
    }
  }

  /**
   * Create a listener when we don't have type information. For the case
   * {@code null} appears before other values.
   */
  private ValueListener unknownListenerFor(String key) {
    return new UnknownFieldListener(this, key);
  }

  /**
   * Create a listener when we don't have type information. For the case
   * {@code []} appears before other values.
   */
  private ValueListener unknownArrayListenerFor(String key, ValueDef valueDef) {
    UnknownFieldListener fieldListener = new UnknownFieldListener(this, key);
    fieldListener.array(valueDef);
    return fieldListener;
  }

  private ObjectWriter addFieldWriter(ColumnMetadata colSchema) {
    int index = tupleWriter.addColumn(colSchema);
    return tupleWriter.column(index);
  }

  /**
   * Convert the JSON type, obtained by looking ahead one token, to a Drill
   * scalar type. Report an error if the JSON type does not map to a Drill
   * type (which can occur in a context where we expect a scalar, but got
   * an object or array.)
   */
  private MinorType scalarTypeFor(String key, JsonType jsonType) {
    MinorType colType = drillTypeFor(jsonType);
    if (colType == null) {
      throw loader.unsupportedJsonTypeException(key, jsonType);
    }
    return colType;
  }

  public MinorType drillTypeFor(JsonType type) {
    if (loader.options().allTextMode) {
      return MinorType.VARCHAR;
    }
    switch (type) {
    case BOOLEAN:
      return MinorType.BIT;
    case FLOAT:
      return MinorType.FLOAT8;
    case INTEGER:
      if (loader.options().readNumbersAsDouble) {
        return MinorType.FLOAT8;
      } else {
        return MinorType.BIGINT;
      }
    case STRING:
      return MinorType.VARCHAR;
    default:
      return null;
    }
  }

  /**
   * Build up a repeated list column definition given a specification of the
   * number of dimensions and the JSON type. Creation of the element type is
   * via a closure that builds the needed schema.
   */
  private ColumnMetadata repeatedListSchemaFor(String key, int dims,
      ColumnMetadata innerArray) {
    ColumnMetadata prev = innerArray;
    for (int i = 1; i < dims; i++) {
      prev = MetadataUtils.newRepeatedList(key, prev);
    }
    return prev;
  }
}
