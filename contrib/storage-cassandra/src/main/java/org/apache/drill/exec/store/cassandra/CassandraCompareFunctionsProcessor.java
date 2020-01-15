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

package org.apache.drill.exec.store.cassandra;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import java.nio.ByteOrder;


public class CassandraCompareFunctionsProcessor extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {
    private String value;
    private boolean success;
    private boolean isEqualityFn;
    private SchemaPath path;
    private String functionName;

    public static boolean isCompareFunction(String functionName) {
        return COMPARE_FUNCTIONS_TRANSPOSE_MAP.keySet().contains(functionName);
    }

    public static CassandraCompareFunctionsProcessor process(FunctionCall call) {
        String functionName = call.getName();
        LogicalExpression nameArg = call.args.get(0);
        LogicalExpression valueArg = call.args.size() >= 2 ? call.args.get(1) : null;
        CassandraCompareFunctionsProcessor evaluator = new CassandraCompareFunctionsProcessor(functionName);

        if (valueArg != null) { // binary function
            if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
                LogicalExpression swapArg = valueArg;
                valueArg = nameArg;
                nameArg = swapArg;
                evaluator.functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName);
            }
            evaluator.success = nameArg.accept(evaluator, valueArg);
        } else if (call.args.get(0) instanceof SchemaPath) {
            evaluator.success = true;
            evaluator.path = (SchemaPath) nameArg;
        }

        return evaluator;
    }

    public CassandraCompareFunctionsProcessor(String functionName) {
        this.success = false;
        this.functionName = functionName;
        this.isEqualityFn = COMPARE_FUNCTIONS_TRANSPOSE_MAP.containsKey(functionName)
                && COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName).equals(functionName);
    }

    public String getValue() {
        return value;
    }

    public boolean isSuccess() {
        return success;
    }

    public SchemaPath getPath() {
        return path;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public Boolean visitCastExpression(CastExpression e, LogicalExpression valueArg) throws RuntimeException {
        if (e.getInput() instanceof CastExpression || e.getInput() instanceof SchemaPath) {
            return e.getInput().accept(this, valueArg);
        }
        return false;
    }

    @Override
    public Boolean visitConvertExpression(ConvertExpression e, LogicalExpression valueArg) throws RuntimeException {
        if (e.getConvertFunction() == ConvertExpression.CONVERT_FROM && e.getInput() instanceof SchemaPath) {
            ByteBuf bb = null;
            String encodingType = e.getEncodingType();
            switch (encodingType) {
                case "INT_BE":
                case "INT":
                case "UINT_BE":
                case "UINT":
                case "UINT4_BE":
                case "UINT4":
                    if (valueArg instanceof ValueExpressions.IntExpression
                            && (isEqualityFn || encodingType.startsWith("U"))) {
                        bb = Unpooled.wrappedBuffer(new byte[4]).order(encodingType.endsWith("_BE") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
                        bb.writeInt(((ValueExpressions.IntExpression)valueArg).getInt());
                    }
                    break;
                case "BIGINT_BE":
                case "BIGINT":
                case "UINT8_BE":
                case "UINT8":
                    if (valueArg instanceof ValueExpressions.LongExpression
                            && (isEqualityFn || encodingType.startsWith("U"))) {
                        bb = Unpooled.wrappedBuffer(new byte[8]).order(encodingType.endsWith("_BE") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
                        bb.writeLong(((ValueExpressions.LongExpression)valueArg).getLong());
                    }
                    break;
                case "FLOAT":
                    if (valueArg instanceof ValueExpressions.FloatExpression && isEqualityFn) {
                        bb = Unpooled.wrappedBuffer(new byte[4]).order(ByteOrder.BIG_ENDIAN);
                        bb.writeFloat(((ValueExpressions.FloatExpression)valueArg).getFloat());
                    }
                    break;
                case "DOUBLE":
                    if (valueArg instanceof ValueExpressions.DoubleExpression && isEqualityFn) {
                        bb = Unpooled.wrappedBuffer(new byte[8]).order(ByteOrder.BIG_ENDIAN);
                        bb.writeDouble(((ValueExpressions.DoubleExpression)valueArg).getDouble());
                    }
                    break;
                case "TIME_EPOCH":
                case "TIME_EPOCH_BE":
                    if (valueArg instanceof ValueExpressions.TimeExpression) {
                        bb = Unpooled.wrappedBuffer(new byte[8]).order(encodingType.endsWith("_BE") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
                        bb.writeLong(((ValueExpressions.TimeExpression)valueArg).getTime());
                    }
                    break;
                case "DATE_EPOCH":
                case "DATE_EPOCH_BE":
                    if (valueArg instanceof ValueExpressions.DateExpression) {
                        bb = Unpooled.wrappedBuffer(new byte[8]).order(encodingType.endsWith("_BE") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
                        bb.writeLong(((ValueExpressions.DateExpression)valueArg).getDate());
                    }
                    break;
                case "BOOLEAN_BYTE":
                    if (valueArg instanceof ValueExpressions.BooleanExpression) {
                        bb = Unpooled.wrappedBuffer(new byte[1]);
                        bb.writeByte(((ValueExpressions.BooleanExpression)valueArg).getBoolean() ? 1 : 0);
                    }
                    break;
                case "UTF8":
                    // let visitSchemaPath() handle this.
                    return e.getInput().accept(this, valueArg);
            }

            if (bb != null) {
                this.value = bb.toString();
                this.path = (SchemaPath)e.getInput();
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg) throws RuntimeException {
        return false;
    }

    @Override
    public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg) throws RuntimeException {
        if (valueArg instanceof ValueExpressions.QuotedString) {
            this.value = new String(((ValueExpressions.QuotedString) valueArg).value.getBytes(Charsets.UTF_8));
            this.path = path;
            return true;
        }
        return false;
    }

    private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES;
    static {
        ImmutableSet.Builder<Class<? extends LogicalExpression>> builder = ImmutableSet.builder();
        VALUE_EXPRESSION_CLASSES = builder
                .add(ValueExpressions.BooleanExpression.class)
                .add(ValueExpressions.DateExpression.class)
                .add(ValueExpressions.DoubleExpression.class)
                .add(ValueExpressions.FloatExpression.class)
                .add(ValueExpressions.IntExpression.class)
                .add(ValueExpressions.LongExpression.class)
                .add(ValueExpressions.QuotedString.class)
                .add(ValueExpressions.TimeExpression.class)
                .build();
    }

    private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;
    static {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
                // unary functions
                .put("isnotnull", "isnotnull")
                .put("isNotNull", "isNotNull")
                .put("is not null", "is not null")
                .put("isnull", "isnull")
                .put("isNull", "isNull")
                .put("is null", "is null")
                        // binary functions
                .put("like", "like")
                .put("equal", "equal")
                .put("not_equal", "not_equal")
                .put("greater_than_or_equal_to", "less_than_or_equal_to")
                .put("greater_than", "less_than")
                .put("less_than_or_equal_to", "greater_than_or_equal_to")
                .put("less_than", "greater_than")
                .build();
    }

}
