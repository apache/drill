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
package org.apache.drill.exec.record.metadata.schema.parser;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.RepeatedListBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Visits schema and stores metadata about its columns into {@link TupleMetadata} class.
 */
public class SchemaVisitor extends SchemaParserBaseVisitor<TupleMetadata> {

  @Override
  public TupleMetadata visitSchema(SchemaParser.SchemaContext ctx) {
    TupleMetadata schema = ctx.columns() == null ? new TupleSchema() : visitColumns(ctx.columns());
    if (ctx.property_values() != null) {
      PropertiesVisitor propertiesVisitor = new PropertiesVisitor();
      schema.setProperties(ctx.property_values().accept(propertiesVisitor));
    }
    return schema;
  }

  @Override
  public TupleMetadata visitColumns(SchemaParser.ColumnsContext ctx) {
    TupleMetadata schema = new TupleSchema();
    ColumnDefVisitor columnDefVisitor = new ColumnDefVisitor();
    ctx.column_def().forEach(
      columnDef -> schema.addColumn(columnDef.accept(columnDefVisitor))
    );
    return schema;
  }

  /**
   * Visits column definition, adds column properties to {@link ColumnMetadata} if present.
   */
  public static class ColumnDefVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    @Override
    public ColumnMetadata visitColumn_def(SchemaParser.Column_defContext ctx) {
      ColumnVisitor columnVisitor = new ColumnVisitor();
      ColumnMetadata columnMetadata = ctx.column().accept(columnVisitor);
      if (ctx.property_values() != null) {
        PropertiesVisitor propertiesVisitor = new PropertiesVisitor();
        columnMetadata.setProperties(ctx.property_values().accept(propertiesVisitor));
      }
      return columnMetadata;
    }

  }

  /**
   * Visits various types of columns (primitive, map, array) and stores their metadata
   * into {@link ColumnMetadata} class.
   */
  public static class ColumnVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    @Override
    public ColumnMetadata visitPrimitive_column(SchemaParser.Primitive_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      TypeProtos.DataMode mode = ctx.nullability() == null ? TypeProtos.DataMode.OPTIONAL : TypeProtos.DataMode.REQUIRED;
      ColumnMetadata columnMetadata = ctx.simple_type().accept(new TypeVisitor(name, mode));
      StringValueVisitor stringValueVisitor = new StringValueVisitor();
      if (ctx.format_value() != null) {
        columnMetadata.setFormat(stringValueVisitor.visit(ctx.format_value().string_value()));
      }
      if (ctx.default_value() != null) {
        columnMetadata.setDefaultValue(stringValueVisitor.visit(ctx.default_value().string_value()));
      }
      return columnMetadata;
    }

    @Override
    public ColumnMetadata visitSimple_array_column(SchemaParser.Simple_array_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      return ctx.simple_array_type().accept(new ArrayTypeVisitor(name));
    }

    @Override
    public ColumnMetadata visitStruct_column(SchemaParser.Struct_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      // Drill does not distinguish between nullable and not null structs, by default they are not null
      return ctx.struct_type().accept(new TypeVisitor(name, TypeProtos.DataMode.REQUIRED));
    }

    @Override
    public ColumnMetadata visitComplex_array_column(SchemaParser.Complex_array_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      ColumnMetadata child = ctx.complex_array_type().complex_type().accept(new ArrayTypeVisitor(name));
      RepeatedListBuilder builder = new RepeatedListBuilder(null, name);
      builder.addColumn(child);
      return builder.buildColumn();
    }

  }

  /**
   * Visits quoted string, strips backticks, single quotes or double quotes and returns bare string value.
   */
  private static class StringValueVisitor extends SchemaParserBaseVisitor<String> {

    @Override
    public String visitString_value(SchemaParser.String_valueContext ctx) {
      String text = ctx.getText();
      // first substring first and last symbols (backticks, single quotes, double quotes)
      // then find all chars that are preceding with the backslash and remove the backslash
      return text.substring(1, text.length() -1).replaceAll("\\\\(.)", "$1");
    }
  }

  /**
   * Visits ID and QUOTED_ID, returning their string representation.
   */
  private static class IdVisitor extends SchemaParserBaseVisitor<String> {

    @Override
    public String visitId(SchemaParser.IdContext ctx) {
      return ctx.ID().getText();
    }

    @Override
    public String visitQuoted_id(SchemaParser.Quoted_idContext ctx) {
      String text = ctx.QUOTED_ID().getText();
      // first substring first and last symbols (backticks)
      // then find all chars that are preceding with the backslash and remove the backslash
      return text.substring(1, text.length() -1).replaceAll("\\\\(.)", "$1");
    }
  }

  /**
   * Visits simple and map types, storing their metadata into {@link ColumnMetadata} holder.
   */
  private static class TypeVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    private final String name;
    private final TypeProtos.DataMode mode;

    TypeVisitor(String name, TypeProtos.DataMode mode) {
      this.name = name;
      this.mode = mode;
    }

    @Override
    public ColumnMetadata visitInt(SchemaParser.IntContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.INT, mode));
    }

    @Override
    public ColumnMetadata visitBigint(SchemaParser.BigintContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.BIGINT, mode));
    }

    @Override
    public ColumnMetadata visitFloat(SchemaParser.FloatContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.FLOAT4, mode));
    }

    @Override
    public ColumnMetadata visitDouble(SchemaParser.DoubleContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.FLOAT8, mode));
    }

    @Override
    public ColumnMetadata visitDecimal(SchemaParser.DecimalContext ctx) {
      TypeProtos.MajorType type = Types.withMode(TypeProtos.MinorType.VARDECIMAL, mode);

      List<TerminalNode> numbers = ctx.NUMBER();
      if (!numbers.isEmpty()) {
        int precision = Integer.parseInt(numbers.get(0).getText());
        int scale = numbers.size() == 2 ? Integer.parseInt(numbers.get(1).getText()) : 0;
        type = type.toBuilder().setPrecision(precision).setScale(scale).build();
      }

      return constructColumn(type);
    }

    @Override
    public ColumnMetadata visitBoolean(SchemaParser.BooleanContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.BIT, mode));
    }

    @Override
    public ColumnMetadata visitVarchar(SchemaParser.VarcharContext ctx) {
      TypeProtos.MajorType type = Types.withMode(TypeProtos.MinorType.VARCHAR, mode);

      if (ctx.NUMBER() != null) {
        type = type.toBuilder().setPrecision(Integer.parseInt(ctx.NUMBER().getText())).build();
      }

      return constructColumn(type);
    }

    @Override
    public ColumnMetadata visitBinary(SchemaParser.BinaryContext ctx) {
      TypeProtos.MajorType type = Types.withMode(TypeProtos.MinorType.VARBINARY, mode);

      if (ctx.NUMBER() != null) {
        type = type.toBuilder().setPrecision(Integer.parseInt(ctx.NUMBER().getText())).build();
      }

      return constructColumn(type);
    }

    @Override
    public ColumnMetadata visitTime(SchemaParser.TimeContext ctx) {
      TypeProtos.MajorType type = Types.withMode(TypeProtos.MinorType.TIME, mode);

      if (ctx.NUMBER() != null) {
        type = type.toBuilder().setPrecision(Integer.parseInt(ctx.NUMBER().getText())).build();
      }

      return constructColumn(type);
    }

    @Override
    public ColumnMetadata visitDate(SchemaParser.DateContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.DATE, mode));
    }

    @Override
    public ColumnMetadata visitTimestamp(SchemaParser.TimestampContext ctx) {
      TypeProtos.MajorType type = Types.withMode(TypeProtos.MinorType.TIMESTAMP, mode);

      if (ctx.NUMBER() != null) {
        type = type.toBuilder().setPrecision(Integer.parseInt(ctx.NUMBER().getText())).build();
      }

      return constructColumn(type);
    }

    @Override
    public ColumnMetadata visitInterval_year(SchemaParser.Interval_yearContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.INTERVALYEAR, mode));
    }

    @Override
    public ColumnMetadata visitInterval_day(SchemaParser.Interval_dayContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.INTERVALDAY, mode));
    }

    @Override
    public ColumnMetadata visitInterval(SchemaParser.IntervalContext ctx) {
      return constructColumn(Types.withMode(TypeProtos.MinorType.INTERVAL, mode));
    }

    @Override
    public ColumnMetadata visitStruct_type(SchemaParser.Struct_typeContext ctx) {
      // internally Drill refers to structs as maps and currently does not have true map notion
      // Drill maps will be renamed to structs in future
      MapBuilder builder = new MapBuilder(null, name, mode);
      ColumnDefVisitor visitor = new ColumnDefVisitor();
      ctx.columns().column_def().forEach(
        c -> builder.addColumn(c.accept(visitor))
      );
      return builder.buildColumn();
    }

    private ColumnMetadata constructColumn(TypeProtos.MajorType type) {
      MaterializedField field = MaterializedField.create(name, type);
      return MetadataUtils.fromField(field);
    }

  }

  /**
   * Visits array type: simple (which has only on nested element: array<int>)
   * or complex (which has several nested elements: array<int<int>>).
   */
  private static class ArrayTypeVisitor extends SchemaParserBaseVisitor<ColumnMetadata> {

    private final String name;

    ArrayTypeVisitor(String name) {
      this.name = name;
    }

    @Override
    public ColumnMetadata visitSimple_array_type(SchemaParser.Simple_array_typeContext ctx) {
      TypeVisitor visitor = new TypeVisitor(name, TypeProtos.DataMode.REPEATED);
      return ctx.struct_type() == null ? ctx.simple_type().accept(visitor) : ctx.struct_type().accept(visitor);
    }

    @Override
    public ColumnMetadata visitComplex_array_type(SchemaParser.Complex_array_typeContext ctx) {
      RepeatedListBuilder childBuilder = new RepeatedListBuilder(null, name);
      ColumnMetadata child = ctx.complex_type().accept(new ArrayTypeVisitor(name));
      childBuilder.addColumn(child);
      return childBuilder.buildColumn();
    }
  }

  /**
   * Visits schema or column properties.
   * Properties must be identified as key values pairs separated by equals sign.
   * Properties pairs must be separated by comma.
   * Property name and value must be enclosed into backticks, single quotes or double quotes.
   */
  public static class PropertiesVisitor extends SchemaParserBaseVisitor<Map<String, String>> {

    @Override
    public Map<String, String> visitProperty_values(SchemaParser.Property_valuesContext ctx) {
      StringValueVisitor stringValueVisitor = new StringValueVisitor();
      Map<String, String> properties = new LinkedHashMap<>();
      ctx.property_pair().forEach(
        pair -> {
          List<String> pairValues = pair.string_value().stream()
            .map(stringValueVisitor::visit)
            .collect(Collectors.toList());
          Preconditions.checkState(pairValues.size() == 2);
          properties.put(pairValues.get(0), pairValues.get(1));
        }
      );
      return properties;
    }
  }
}
