package org.apache.drill;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchemaBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Covers selecting completely missing columns from a parquet table. Should create Nullable Int
 * ValueVector in that case since there is no chance to guess the correct data type here.
 */
public class TestParquetMissingColumns extends ClusterTest {

  private static final TypeProtos.MajorType NULLABLE_INT_TYPE = Types.optional(TypeProtos.MinorType.INT);

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testCoalesceOnNotExistentColumns() throws Exception {
    String query = "select coalesce(cunk1, unk2) as coal from cp.`tpch/nation.parquet` limit 5";
    SchemaBuilder schemaBuilder = new SchemaBuilder()
        .add("coal", NULLABLE_INT_TYPE);
    BatchSchema expectedSchema = new BatchSchemaBuilder()
        .withSchemaBuilder(schemaBuilder)
        .build();

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .go();

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("coal")
        .baselineValuesForSingleColumn(null, null, null, null, null)
        .go();
  }

  @Test
  public void testCoalesceOnNotExistentColumnsWithGroupBy() throws Exception {
    String query = "select coalesce(unk1, unk2) as coal from cp.`tpch/nation.parquet` group by 1";
    SchemaBuilder schemaBuilder = new SchemaBuilder()
        .add("coal", NULLABLE_INT_TYPE);
    BatchSchema expectedSchema = new BatchSchemaBuilder()
        .withSchemaBuilder(schemaBuilder)
        .build();

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .go();

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("coal")
        .baselineValuesForSingleColumn(new Object[] {null})
        .go();
  }

  @Test
  public void testCoalesceOnNotExistentColumnsWithOrderBy() throws Exception {
    String query = "select coalesce(unk1, unk2) as coal from cp.`tpch/nation.parquet` order by 1 limit 5";
    SchemaBuilder schemaBuilder = new SchemaBuilder()
        .add("coal", NULLABLE_INT_TYPE);
    BatchSchema expectedSchema = new BatchSchemaBuilder()
        .withSchemaBuilder(schemaBuilder)
        .build();

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .go();

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("coal")
        .baselineValuesForSingleColumn(null, null, null, null, null)
        .go();
  }
}
