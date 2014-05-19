package org.apache.drill.exec.server;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.apache.drill.exec.client.DrillClient;

@Path("hello")
public class HelloResource {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HelloResource.class);

  @Inject DrillClient client;

  @GET
  @Produces("text/plain")
  public String getHello() {
    return "hello world" + client;
  }

}
