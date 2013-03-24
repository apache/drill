package org.apache.drill.hbase.table;

import org.apache.drill.exec.ref.rse.ReferenceStorageEngine;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * @author David Alves
 */
public class HBaseTableScanner implements ReferenceStorageEngine.ReadEntry {

  public final HTable hTable;
  public final Scan scan;

  public HBaseTableScanner(HTable hTable, Scan scan) {
    this.hTable = hTable;
    this.scan = scan;
  }

  public ResultScanner newScanner() throws IOException {
    return this.hTable.getScanner(scan);
  }
}