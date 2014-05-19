package org.apache.drill.exec.server;

import org.apache.drill.exec.client.DrillClient;
import org.glassfish.hk2.api.Factory;

public class DrillClientFactory implements Factory<DrillClient>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillClientFactory.class);



  @Override
  public void dispose(DrillClient arg0) {
  }

  @Override
  public DrillClient provide() {
    return new DrillClient();
  }


}
