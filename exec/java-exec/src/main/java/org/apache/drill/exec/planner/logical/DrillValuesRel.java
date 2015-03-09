/**
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
package org.apache.drill.exec.planner.logical;

import static org.apache.drill.exec.planner.logical.DrillOptiq.isLiteralNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Values;
import org.apache.drill.exec.vector.complex.fn.ExtendedJsonOutput;
import org.apache.drill.exec.vector.complex.fn.JsonOutput;
import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.type.SqlTypeUtil;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Pair;
import org.joda.time.DateTime;
import org.joda.time.Period;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.base.Functions;

/**
 * Values implemented in Drill.
 */
public class DrillValuesRel extends AbstractRelNode implements DrillRel {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillValuesRel.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final long MILLIS_IN_DAY = 1000*60*60*24;

  private final JSONOptions options;
  private final double rowCount;

  protected DrillValuesRel(RelOptCluster cluster, RelDataType rowType, List<List<RexLiteral>> tuples, RelTraitSet traits) {
    super(cluster, traits);
    assert getConvention() == DRILL_LOGICAL;
    verifyRowType(tuples, rowType);

    this.rowType = rowType;
    this.rowCount = tuples.size();

    try{
      this.options = new JSONOptions(convertToJsonNode(rowType, tuples), JsonLocation.NA);
    }catch(IOException e){
      throw new DrillRuntimeException("Failure while attempting to encode ValuesRel in JSON.", e);
    }

  }

  private DrillValuesRel(RelOptCluster cluster, RelDataType rowType, RelTraitSet traits, JSONOptions options, double rowCount){
    super(cluster, traits);
    this.options = options;
    this.rowCount = rowCount;
    this.rowType = rowType;
  }

  private static void verifyRowType(final List<List<RexLiteral>> tuples, RelDataType rowType){
      for (List<RexLiteral> tuple : tuples) {
        assert (tuple.size() == rowType.getFieldCount());

        for (Pair<RexLiteral, RelDataTypeField> pair : Pair.zip(tuple, rowType.getFieldList())) {
          RexLiteral literal = (RexLiteral) pair.left;
          RelDataType fieldType = ((RelDataTypeField) pair.right).getType();

          if ((!(RexLiteral.isNullLiteral(literal)))
              && (!(SqlTypeUtil.canAssignFrom(fieldType, literal.getType())))) {
            throw new AssertionError("to " + fieldType + " from " + literal);
          }
        }
      }

  }

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return planner.getCostFactory().makeCost(this.rowCount, 1.0d, 0.0d);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new DrillValuesRel(getCluster(), rowType, traitSet, options, rowCount);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
      return Values.builder()
          .content(options.asNode())
          .build();
  }

  public JSONOptions getTuplesAsJsonOptions() throws IOException {
    return options;
  }

  public double getRows() {
    return rowCount;
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("type", this.rowType, pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
        .itemIf("type", this.rowType.getFieldList(), pw.nest())
        .itemIf("tuplesCount", rowCount, pw.getDetailLevel() != SqlExplainLevel.ALL_ATTRIBUTES)
        .itemIf("tuples", options.asNode(), pw.getDetailLevel() == SqlExplainLevel.ALL_ATTRIBUTES);
  }

  private static JsonNode convertToJsonNode(RelDataType rowType, List<List<RexLiteral>> tuples) throws IOException{
    TokenBuffer out = new TokenBuffer(MAPPER.getFactory().getCodec(), false);
    JsonOutput json = new ExtendedJsonOutput(out);
    json.writeStartArray();
    String[] fields = rowType.getFieldNames().toArray(new String[rowType.getFieldCount()]);

    for(List<RexLiteral> row : tuples){
      json.writeStartObject();
      int i =0;
      for(RexLiteral field : row){
        json.writeFieldName(fields[i]);
        writeLiteral(field, json);
        i++;
      }
      json.writeEndObject();
    }
    json.writeEndArray();
    json.flush();
    return out.asParser().readValueAsTree();
  }


  private static void writeLiteral(RexLiteral literal, JsonOutput out) throws IOException{

    switch(literal.getType().getSqlTypeName()){
    case BIGINT:
      if (isLiteralNull(literal)) {
        out.writeBigIntNull();
      }else{
        out.writeBigInt((((BigDecimal) literal.getValue()).setScale(0, BigDecimal.ROUND_HALF_UP)).longValue());
      }
      return;

    case BOOLEAN:
      if (isLiteralNull(literal)) {
        out.writeBooleanNull();
      }else{
        out.writeBoolean((Boolean) literal.getValue());
      }
      return;

    case CHAR:
      if (isLiteralNull(literal)) {
        out.writeVarcharNull();
      }else{
        out.writeVarChar(((NlsString)literal.getValue()).getValue());
      }
      return ;

    case DOUBLE:
      if (isLiteralNull(literal)){
        out.writeDoubleNull();
      }else{
        out.writeDouble(((BigDecimal) literal.getValue()).doubleValue());
      }
      return;

    case FLOAT:
      if (isLiteralNull(literal)) {
        out.writeFloatNull();
      }else{
        out.writeFloat(((BigDecimal) literal.getValue()).floatValue());
      }
      return;

    case INTEGER:
      if (isLiteralNull(literal)) {
        out.writeIntNull();
      }else{
        out.writeInt((((BigDecimal) literal.getValue()).setScale(0, BigDecimal.ROUND_HALF_UP)).intValue());
      }
      return;

    case DECIMAL:
      if (isLiteralNull(literal)) {
        out.writeDoubleNull();
      }else{
        out.writeDouble(((BigDecimal) literal.getValue()).doubleValue());
      }
      logger.warn("Converting exact decimal into approximate decimal.  Should be fixed once decimal is implemented.");
      return;

    case VARCHAR:
      if (isLiteralNull(literal)) {
        out.writeVarcharNull();
      }else{
        out.writeVarChar( ((NlsString)literal.getValue()).getValue());
      }
      return;

    case SYMBOL:
      if (isLiteralNull(literal)) {
        out.writeVarcharNull();
      }else{
        out.writeVarChar(literal.getValue().toString());
      }
      return;

    case DATE:
      if (isLiteralNull(literal)) {
        out.writeDateNull();
      }else{
        out.writeDate(new DateTime((GregorianCalendar)literal.getValue()));
      }
      return;

    case TIME:
      if (isLiteralNull(literal)) {
        out.writeTimeNull();
      }else{
        out.writeTime(new DateTime((GregorianCalendar)literal.getValue()));
      }
      return;

    case TIMESTAMP:
      if (isLiteralNull(literal)) {
        out.writeTimestampNull();
      }else{
        out.writeTimestamp(new DateTime((GregorianCalendar)literal.getValue()));
      }
      return;

    case INTERVAL_YEAR_MONTH:
      if (isLiteralNull(literal)) {
        out.writeIntervalNull();
      }else{
        int months = ((BigDecimal) (literal.getValue())).intValue();
        out.writeInterval(new Period().plusMonths(months));
      }
      return;

    case INTERVAL_DAY_TIME:
      if (isLiteralNull(literal)) {
        out.writeIntervalNull();
      }else{
        long millis = ((BigDecimal) (literal.getValue())).longValue();
        int days = (int) (millis/MILLIS_IN_DAY);
        millis = millis - (days * MILLIS_IN_DAY);
        out.writeInterval(new Period().plusDays(days).plusMillis( (int) millis));
      }
      return;

    case NULL:
      out.writeUntypedNull();
      return;

    case ANY:
    default:
      throw new UnsupportedOperationException(String.format("Unable to convert the value of %s and type %s to a Drill constant expression.", literal, literal.getType().getSqlTypeName()));
    }
  }
}
