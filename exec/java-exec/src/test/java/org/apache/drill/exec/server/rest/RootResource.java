package org.apache.drill.exec.server.rest;

import javax.ws.rs.Path;

@Path("/")
public class RootResource {
  public int hi = 5;
  public String blue = "yo";
}
