package org.apache.drill.exec.store.jdbc;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.AbstractRecordWriter;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class JdbcRecordWriter extends AbstractRecordWriter {

  private static final Logger logger = LoggerFactory.getLogger(JdbcRecordWriter.class);
  private static final ImmutableMap<MinorType, Integer> JDBC_TYPE_MAPPINGS;
  private final DataSource source;

  /*
   * This map maps JDBC data types to their Drill equivalents.  The basic strategy is that if there
   * is a Drill equivalent, then do the mapping as expected.  All flavors of INT (SMALLINT, TINYINT etc)
   * are mapped to INT in Drill, with the exception of BIGINT.
   *
   * All flavors of character fields are mapped to VARCHAR in Drill. All versions of binary fields are
   * mapped to VARBINARY.
   *
   */
  static {
    JDBC_TYPE_MAPPINGS = ImmutableMap.<MinorType, Integer>builder()
      .put(MinorType.FLOAT8, java.sql.Types.DOUBLE)
      .put(MinorType.FLOAT4 ,java.sql.Types.FLOAT)
      .put(MinorType.TINYINT, java.sql.Types.TINYINT)
      .put(MinorType.SMALLINT, java.sql.Types.SMALLINT)
      .put(MinorType.INT, java.sql.Types.INTEGER)
      .put(MinorType.BIGINT, java.sql.Types.BIGINT)

      .put(MinorType.VARCHAR, java.sql.Types.VARCHAR)
      .put(MinorType.VARBINARY, java.sql.Types.VARBINARY)

      .put( MinorType.VARDECIMAL, java.sql.Types.DECIMAL)

      .put(MinorType.DATE, java.sql.Types.DATE)
      .put(MinorType.TIME, java.sql.Types.TIME)
      .put(MinorType.TIMESTAMP, java.sql.Types.TIMESTAMP)

      .put(MinorType.BIT, java.sql.Types.BOOLEAN)
      .build();
  }

  public JdbcRecordWriter(DataSource source) {
    this.source = source;
  }

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {

  }

  @Override
  public void updateSchema(VectorAccessible batch) throws IOException {
    BatchSchema schema = batch.getSchema();
    String columnName;
    MinorType type;

    for (Iterator<MaterializedField> it = schema.iterator(); it.hasNext(); ) {
      MaterializedField field = it.next();
      columnName = field.getName();
      type = field.getType().getMinorType();
      logger.debug("Adding column {} of type {}.", columnName, type);

    }
  }

  @Override
  public void startRecord() throws IOException {

  }

  @Override
  public void endRecord() throws IOException {

  }

  @Override
  public void abort() throws IOException {

  }

  @Override
  public void cleanup() throws IOException {

  }
}
