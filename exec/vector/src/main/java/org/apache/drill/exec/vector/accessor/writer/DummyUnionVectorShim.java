package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

/**
 * Shim for a non-existent (unprojected) union vector.
 */
public class DummyUnionVectorShim implements UnionShim {

  @Override
  public void bindIndex(ColumnWriterIndex index) { }

  @Override
  public void bindListener(ColumnWriterListener listener) { }

  @Override
  public void startWrite() { }

  @Override
  public void startRow() { }

  @Override
  public void endArrayValue() { }

  @Override
  public void restartRow() { }

  @Override
  public void saveRow() { }

  @Override
  public void endWrite() { }

  @Override
  public void preRollover() { }

  @Override
  public void postRollover() { }

  @Override
  public void dump(HierarchicalFormatter format) { }

  @Override
  public int writeIndex() { return 0; }

  @Override
  public void bindWriter(UnionWriterImpl writer) { }

  @Override
  public void setNull() { }

  @Override
  public boolean hasType(MinorType type) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ObjectWriter member(MinorType type) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setType(MinorType type) {
    // TODO Auto-generated method stub
  }

  @Override
  public int lastWriteIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int rowStartIndex() { return 0; }

  @Override
  public AbstractObjectWriter addMember(ColumnMetadata colSchema) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AbstractObjectWriter addMember(MinorType type) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addMember(AbstractObjectWriter colWriter) {
    // TODO Auto-generated method stub
  }
}
