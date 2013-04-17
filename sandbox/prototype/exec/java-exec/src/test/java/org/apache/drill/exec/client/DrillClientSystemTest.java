package org.apache.drill.exec.client;

import org.apache.drill.exec.DrillSystemTestBase;
import org.apache.drill.exec.server.StartupOptions;
import org.junit.Test;

/**
 * @author David Alves
 */
public class DrillClientSystemTest extends DrillSystemTestBase {

  StartupOptions options = new StartupOptions();

  @Test
  public void testSubmitQuery() {
    startCluster(options, 1);


  }
}
