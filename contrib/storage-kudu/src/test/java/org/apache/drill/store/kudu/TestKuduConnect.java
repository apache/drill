package org.apache.drill.store.kudu;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.Insert;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.ListTablesResponse;
import org.kududb.client.PartialRow;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;


public class TestKuduConnect {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestKuduConnect.class);

  public static final String KUDU_MASTER = "172.31.1.99";
  public static final String KUDU_TABLE = "demo";

  @Test
  public void abc() throws Exception {

    try (KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()) {

      ListTablesResponse tables = client.getTablesList(KUDU_TABLE);
      if (!tables.getTablesList().isEmpty()) {
        client.deleteTable(KUDU_TABLE);
      }
      ;

      List<ColumnSchema> columns = new ArrayList<>(5);
      columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("binary", Type.BINARY).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("boolean", Type.BOOL).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("float", Type.FLOAT).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("string", Type.STRING).build());

      Schema schema = new Schema(columns);
      client.createTable(KUDU_TABLE, schema);

      KuduTable table = client.openTable(KUDU_TABLE);

      KuduSession session = client.newSession();
      for (int i = 0; i < 3; i++) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addInt(0, i);
        row.addBinary(1, ("Row " + i).getBytes());
        row.addBoolean(2, i % 2 == 0);
        row.addFloat(3, i + 0.01f);
        row.addString(4, ("Row " + i));
        session.apply(insert);
      }

      List<String> projectColumns = new ArrayList<>(1);
      projectColumns.add("float");
      KuduScanner scanner = client.newScannerBuilder(table)
          .setProjectedColumnNames(projectColumns)
          .build();
      while (scanner.hasMoreRows()) {
        RowResultIterator results = scanner.nextRows();
        while (results.hasNext()) {
          RowResult result = results.next();
          System.out.println(result.toStringLongFormat());
        }
      }
    }
  }
}
