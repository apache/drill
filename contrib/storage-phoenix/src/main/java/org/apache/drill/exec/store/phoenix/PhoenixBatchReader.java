package org.apache.drill.exec.store.phoenix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoenixBatchReader implements ManagedReader<SchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(PhoenixBatchReader.class);

  private final PhoenixSubScan subScan;
  private PhoenixResultSet resultSet;

  private ColumnDefn[] columns;
  private int count = 1;

  public PhoenixBatchReader(PhoenixSubScan subScan) {
    this.subScan = subScan;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    negotiator.tableSchema(defineMetadata(), true);
//    negotiator.batchSize(3);
    resultSet = new PhoenixResultSet(negotiator.build());
    bindColumns(resultSet.getWriter());
    return true;
  }

  String abc = "Currently, the Apache Drill build process is known to work on Linux, Windows and OSX.";

  @Override
  public boolean next() {
    if (count > 3) {
      return false;
    }
//    byte[] value = new byte[512];
//    Arrays.fill(value, (byte) String.valueOf(count).charAt(0));
    while(!resultSet.getWriter().isFull()) {
      resultSet.getWriter().start();
      for (int i = 0; i < columns.length; i++) {
        columns[i].load(count + "\t" + abc);
      }
      resultSet.getWriter().save();
    }
    count++;
    return true;
  }

  @Override
  public void close() {
    int count = resultSet.getWriter().loader().batchCount();
    logger.info("phoenix fetch batch size: {}", count);
  }

  private TupleMetadata defineMetadata() {
    List<String> cols = new ArrayList<String>(Arrays.asList("a", "b", "c"));
    columns = new ColumnDefn[cols.size()];
    SchemaBuilder builder = new SchemaBuilder();
    for (int i = 0; i < cols.size(); i++) {
      columns[i] = makeColumn(cols.get(i), i);
      columns[i].define(builder);
    }
    return builder.buildSchema();
  }

  private ColumnDefn makeColumn(String name, int index) {
    return new VarCharDefn(name, index);
  }

  private void bindColumns(RowSetLoader loader) {
    for (int i = 0; i < columns.length; i++) {
      columns[i].bind(loader);
    }
  }

  public abstract static class ColumnDefn {

    final String name;
    int index;
    ScalarWriter writer;

    public String getName() {
      return name;
    }

    public int getIndex() {
      return index;
    }

    public ColumnDefn(String name, int index) {
      this.name = name;
      this.index = index;
    }

    public void bind(RowSetLoader loader) {
      writer = loader.scalar(getName());
    }

    public abstract void define(SchemaBuilder builder);

    public abstract void load(String value);

    public abstract void load(byte[] value);

    public abstract void load(int index, String value);
  }

  public static class VarCharDefn extends ColumnDefn {

    public VarCharDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(SchemaBuilder builder) {
      builder.addNullable(getName(), MinorType.VARCHAR);
    }

    @Override
    public void load(String value) {
      writer.setString(value);
    }

    @Override
    public void load(byte[] value) {
      writer.setBytes(value, value.length);
    }

    @Deprecated
    @Override
    public void load(int index, String value) {  }
  }
}
