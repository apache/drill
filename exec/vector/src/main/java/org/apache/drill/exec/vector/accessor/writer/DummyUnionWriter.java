package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

public class DummyUnionWriter extends UnionWriter {

  @Override
  public ObjectWriter member(MinorType type) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ScalarWriter scalar(MinorType type) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TupleWriter tuple() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ArrayWriter array() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isProjected() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void copy(ColumnReader from) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setObject(Object value) {
    // TODO Auto-generated method stub

  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    // TODO Auto-generated method stub

  }

  @Override
  public void startWrite() {
    // TODO Auto-generated method stub

  }

  @Override
  public void startRow() {
    // TODO Auto-generated method stub

  }

  @Override
  public void endArrayValue() {
    // TODO Auto-generated method stub

  }

  @Override
  public void restartRow() {
    // TODO Auto-generated method stub

  }

  @Override
  public void saveRow() {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWrite() {
    // TODO Auto-generated method stub

  }

  @Override
  public void preRollover() {
    // TODO Auto-generated method stub

  }

  @Override
  public void postRollover() {
    // TODO Auto-generated method stub

  }

  @Override
  public void dump(HierarchicalFormatter format) {
    // TODO Auto-generated method stub

  }

  @Override
  public int rowStartIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int lastWriteIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int writeIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void bindShim(UnionShim shim) {
    // TODO Auto-generated method stub

  }

}
