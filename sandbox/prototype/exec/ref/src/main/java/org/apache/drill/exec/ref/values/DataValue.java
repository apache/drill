package org.apache.drill.exec.ref.values;

import java.io.IOException;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.exec.ref.rops.DataWriter;


public interface DataValue {
  
  public static final DataValue NULL_VALUE = new ScalarValues.NullValue();

  public DataValue getValue(PathSegment segment);
  public void addValue(PathSegment segment, DataValue v);
  public void write(DataWriter writer) throws IOException;
  public DataType getDataType();
  public NumericValue getAsNumeric();
  public ContainerValue getAsContainer();
  public StringValue getAsStringValue();
  public BooleanValue getAsBooleanValue();
  public BytesValue getAsBytesValue();
  public boolean equals(DataValue v);
  public int hashCode();
}
