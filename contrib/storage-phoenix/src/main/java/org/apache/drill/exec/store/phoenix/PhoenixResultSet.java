package org.apache.drill.exec.store.phoenix;

import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;

public class PhoenixResultSet {

  private ResultSetLoader loader;
  private RowSetLoader writer;

  public PhoenixResultSet(ResultSetLoader loader) {
    this.loader = loader;
    this.writer = loader.writer();
  }

  public RowSetLoader getWriter() {
    return writer;
  }
}
