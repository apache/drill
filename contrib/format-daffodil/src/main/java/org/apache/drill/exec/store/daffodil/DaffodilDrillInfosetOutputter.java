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
package org.apache.drill.exec.store.daffodil;

import com.ibm.icu.util.Calendar;
import com.ibm.icu.util.TimeZone;
import org.apache.daffodil.runtime1.api.ComplexElementMetadata;
import org.apache.daffodil.runtime1.api.ElementMetadata;
import org.apache.daffodil.runtime1.api.InfosetArray;
import org.apache.daffodil.runtime1.api.InfosetComplexElement;
import org.apache.daffodil.japi.infoset.InfosetOutputter;
import org.apache.daffodil.runtime1.api.InfosetSimpleElement;
import org.apache.daffodil.runtime1.api.PrimitiveType;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.daffodil.schema.DrillDaffodilSchemaUtils;
import org.apache.drill.exec.store.daffodil.schema.DrillDaffodilSchemaVisitor;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Stack;

/**
 * Adapts Daffodil parser infoset event calls to Drill writer calls
 * to fill in Drill data rows.
 */
public class DaffodilDrillInfosetOutputter
    extends InfosetOutputter {

  private boolean isOriginalRoot() {
    boolean result = currentTupleWriter() == rowSetWriter;
    if (result)
      assert(tupleWriterStack.size() == 1);
    return result;
  }

  /**
   * True if the next startComplex call will be for the
   * DFDL infoset root element whose children are the columns of
   * the row set.
   */
  private boolean isRootElement = true;

  /**
   * Stack that is used only if we have sub-structures that are not
   * simple-type fields of the row.
   */
  private final Stack<TupleWriter> tupleWriterStack = new Stack<>();

  private final Stack<ArrayWriter> arrayWriterStack = new Stack<>();

  private TupleWriter currentTupleWriter() {
    return tupleWriterStack.peek();
  }

  private ArrayWriter currentArrayWriter() {
    return arrayWriterStack.peek();
  }


  private static final Logger logger = LoggerFactory.getLogger(DaffodilDrillInfosetOutputter.class);

  private DaffodilDrillInfosetOutputter() {} // no default constructor

  private RowSetLoader rowSetWriter;

  public DaffodilDrillInfosetOutputter(RowSetLoader writer) {
    this.rowSetWriter = writer;
    this.tupleWriterStack.push(writer);
  }

  @Override
  public void reset() {
    tupleWriterStack.clear();
    tupleWriterStack.push(rowSetWriter);
    arrayWriterStack.clear();
    this.isRootElement = true;
    checkCleanState();
  }

  private void checkCleanState() {
    assert(isOriginalRoot());
    assert(arrayWriterStack.isEmpty());
    assert(isRootElement);
  }

  @Override
  public void startDocument() {
    checkCleanState();
  }

  @Override
  public void endDocument() {
    checkCleanState();
  }

  private String colName(ElementMetadata md) {
    return DrillDaffodilSchemaVisitor.makeColumnName(md);
  }

  @Override
  public void startSimple(InfosetSimpleElement ise) {
    assert (!isRootElement);
    ElementMetadata md = ise.metadata();
    String colName = colName(md);
    ScalarWriter cw;
    if (md.isArray()) {
      // A simple type array
      assert(!arrayWriterStack.isEmpty());
      cw = currentArrayWriter().scalar();
    } else {
      // A simple element within a map
      // Note the map itself might be an array
      // but we don't care about that here.
      cw = currentTupleWriter().scalar(colName);
    }
    ColumnMetadata cm = cw.schema();
    assert(cm.isScalar());
    if (md.isNillable() && ise.isNilled()) {
      assert cm.isNullable();
      cw.setNull();
    } else {
      convertDaffodilValueToDrillValue(ise, cm, cw);
    }
  }

  @Override
  public void endSimple(InfosetSimpleElement diSimple) {
    assert (!isRootElement);
    // do nothing
  }

  @Override
  public void startComplex(InfosetComplexElement ce) {
    ComplexElementMetadata md = ce.metadata();
    String colName = colName(ce.metadata());
    if (isRootElement) {
      assert(isOriginalRoot());
      // This complex element's corresponds to the root element of the
      // DFDL schema. We don't treat this as a column of the row set.
      // Rather, it's children are the columns of the row set.
      //
      // If we do nothing at all here, then we'll start getting
      // even calls for the children.
      isRootElement = false;
      return;
    }
    if (md.isArray()) {
      assert(!arrayWriterStack.isEmpty());
      // FIXME: is this the way to add a complex array child item (i.e., each array item is a map)
      tupleWriterStack.push(currentArrayWriter().tuple());
    } else {
      tupleWriterStack.push(currentTupleWriter().tuple(colName));
    }
  }

  @Override
  public void endComplex(InfosetComplexElement ce) {
    ComplexElementMetadata md = ce.metadata();
    if (isOriginalRoot()) {
      isRootElement = true;
      // do nothing else. The row gets closed-out in the DaffodilBatchReader.next() method.
    } else {
      // it's a map.
      // We seem to not need to do anything to end the map. No action taken here works.
      if (md.isArray()) {
        assert (!arrayWriterStack.isEmpty());
        currentArrayWriter().save(); // required for map array entries.
      }
      tupleWriterStack.pop();
    }
  }

  @Override
  public void startArray(InfosetArray diArray) {
    ElementMetadata md = diArray.metadata();
    assert (md.isArray());
    // DFDL has no notion of an array directly within another array. A named field (map) is necessary
    // before you can have another array.
    assert (currentTupleWriter().type() == ObjectType.TUPLE); // parent is a map, or the top level row.
    String colName = colName(md);
    TupleWriter enclosingParentTupleWriter = currentTupleWriter();
    ArrayWriter aw = enclosingParentTupleWriter.array(colName);
    arrayWriterStack.push(aw);
  }

  @Override
  public void endArray(InfosetArray ia) {
    ElementMetadata md = ia.metadata();
    assert (md.isArray());
    assert (!arrayWriterStack.empty());
    // FIXME: How do we end/close-out an array?
    // note that each array instance, when the instance is a map, must have
    // save called after it is written to the array but that happens
    // in endComplex events since it must be called not once per array, but
    // once per array item.
    arrayWriterStack.pop();
  }

  private void invariantFailed(String dafTypeName, ColumnMetadata cm) {
    String msg = String.format("Daffodil to Drill Conversion Invariant Failed: dafType %s, drill type %s.", dafTypeName, cm.typeString());
    logger.error(msg);
    fatalError(msg);
  }

  private void convertDaffodilValueToDrillValue(InfosetSimpleElement ise, ColumnMetadata cm, ScalarWriter cw) {
    PrimitiveType dafType = ise.metadata().primitiveType();
    String dafTypeName = dafType.name();
    TypeProtos.MinorType drillType = DrillDaffodilSchemaUtils.getDrillDataType(dafType);
    assert(drillType == cm.type());
    switch (drillType) {
    case BIGINT: { // not a bignum, BIGINT is a signed 8-byte long in Drill.
      switch (dafTypeName) {
      case "unsignedInt": {
        cw.setLong(ise.getUnsignedInt());
        break;
      }
      case "long": {
        cw.setLong(ise.getLong());
        break;
      }
      default: invariantFailed(dafTypeName, cm);
      }
      break;
    }
    case INT: {
      cw.setInt(ise.getInt());
      break;
    }
    case SMALLINT: {
      cw.setInt(ise.getShort()); // there is no setShort
      break;
    }
    case TINYINT: {
      cw.setInt(ise.getByte()); // there is no setByte
      break;
    }
    case UINT4: {
      // daffodil represents unsigned int as long.
      // drill represents unsigned int as int.
      cw.setInt(ise.getUnsignedInt().intValue());
      break;
    }
    case UINT2: {
      cw.setInt(ise.getUnsignedShort());
      break;
    }
    case UINT1: {
      cw.setInt(ise.getUnsignedByte());
      break;
    }
    case VARDECIMAL: {
      switch (dafTypeName) {
      case "unsignedLong": {
        cw.setDecimal(new BigDecimal(ise.getUnsignedLong()));
        break;
      }
      case "integer": {
        cw.setDecimal(new BigDecimal(ise.getInteger()));
        break;
      }
      case "nonNegativeInteger": {
        cw.setDecimal(new BigDecimal(ise.getNonNegativeInteger()));
        break;
      }
      default: invariantFailed(dafTypeName, cm);
      }
      break;
    }
    case BIT: {
      cw.setBoolean(ise.getBoolean());
      break;
    }
    case FLOAT8: {
      switch (dafTypeName) {
      case "double": {
        cw.setDouble(ise.getDouble());
        break;
      }
      case "float": {
        // converting a float to a double by doubleValue() fails here
        // Float.MaxValue converted to a double via doubleValue()
        // then placed in a FLOAT8 column displays as
        // 3.4028234663852886E38 not 3.4028235E38.
        // But converting to string first, then to double works properly.
        cw.setDouble(Double.valueOf(ise.getFloat().toString()));
        break;
      }
      default:
        invariantFailed(dafTypeName, cm);
      }
      break;
    }
    case FLOAT4: {
      // we don't use float4, we always use float8.
      invariantFailed(dafTypeName, cm);
      // cw.setFloat(ise.getFloat());
      break;
    }
    case VARBINARY: {
      byte[] hexBinary = ise.getHexBinary();
      cw.setBytes(hexBinary, hexBinary.length);
      break;
    }
    case VARCHAR: {
      switch (dafTypeName) {
      case "decimal": {
        BigDecimal decimal = ise.getDecimal();
        cw.setString(decimal.toString());
        break;
      }
      case "string": {
        String s = ise.getString();
        cw.setString(s);
        break;
      }
      default:
        invariantFailed(dafTypeName, cm);
      }
      break;
    }
    case TIME: {
      Calendar icuCal = ise.getTime();
      Instant instant = Instant.ofEpochMilli(icuCal.getTimeInMillis());
      TimeZone icuZone = icuCal.getTimeZone();
      String zoneString = icuZone.getID();
      ZoneId zoneId = ZoneId.of(zoneString);
      LocalTime localTime = instant.atZone(zoneId).toLocalTime();
      cw.setTime(localTime);
      break;
    }
    case DATE: {
      Calendar icuCalendar = ise.getDate();
      // Extract year, month, and day from ICU Calendar
      int year = icuCalendar.get(Calendar.YEAR);
      // Note: ICU Calendar months are zero-based, similar to java.util.Calendar
      int month = icuCalendar.get(Calendar.MONTH) + 1;
      int day = icuCalendar.get(Calendar.DAY_OF_MONTH);
      // Create a LocalDate
      LocalDate localDate = LocalDate.of(year, month, day);
      cw.setDate(localDate);
      break;
    }
    case TIMESTAMP: {
      Calendar icuCalendar = ise.getDateTime();
      // Get time in milliseconds from the epoch
      long millis = icuCalendar.getTimeInMillis();
      // Create an Instant from milliseconds
      Instant instant = Instant.ofEpochMilli(millis);
      cw.setTimestamp(instant);
      break;
    }
    default: invariantFailed(dafTypeName, cm);
    }
  }

  private void DFDLParseError(String s) {
    throw new RuntimeException(s);
  }

  private static void nyi() {
    throw new IllegalStateException("not yet implemented.");
  }

  private static void fatalError(String s) {
    throw new IllegalStateException(s);
  }
}

