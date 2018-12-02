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

import java.util.List;

/**
 * Visits schema and stores metadata about its columns into {@link TupleMetadata} class.
 */
public class SchemaVisitor extends SchemaParserBaseVisitor<TupleMetadata> {

  @Override
  public TupleMetadata visitSchema(SchemaParser.SchemaContext ctx) {
    return visitColumns(ctx.columns());
  }

  @Override
  public TupleMetadata visitColumns(SchemaParser.ColumnsContext ctx) {
    TupleMetadata schema = new TupleSchema();
    ColumnVisitor columnVisitor = new ColumnVisitor();
    ctx.column().forEach(
      c -> schema.addColumn(c.accept(columnVisitor))
    );
    return schema;
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
      return ctx.simple_type().accept(new TypeVisitor(name, mode));
    }

    @Override
    public ColumnMetadata visitSimple_array_column(SchemaParser.Simple_array_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      return ctx.simple_array_type().accept(new ArrayTypeVisitor(name));
    }

    @Override
    public ColumnMetadata visitMap_column(SchemaParser.Map_columnContext ctx) {
      String name = ctx.column_id().accept(new IdVisitor());
      // Drill does not distinguish between nullable and not null map, by default they are not null
      return ctx.map_type().accept(new TypeVisitor(name, TypeProtos.DataMode.REQUIRED));
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
    public ColumnMetadata visitMap_type(SchemaParser.Map_typeContext ctx) {
      MapBuilder builder = new MapBuilder(null, name, mode);
      ColumnVisitor visitor = new ColumnVisitor();
      ctx.columns().column().forEach(
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
      return ctx.map_type() == null ? ctx.simple_type().accept(visitor) : ctx.map_type().accept(visitor);
    }

    @Override
    public ColumnMetadata visitComplex_array_type(SchemaParser.Complex_array_typeContext ctx) {
      RepeatedListBuilder childBuilder = new RepeatedListBuilder(null, name);
      ColumnMetadata child = ctx.complex_type().accept(new ArrayTypeVisitor(name));
      childBuilder.addColumn(child);
      return childBuilder.buildColumn();
    }
  }

}
