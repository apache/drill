/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server.rest;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.apache.drill.exec.work.WorkManager;

@Path("/www")
public class WebResourceServer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WebResourceServer.class);

  @Inject WorkManager work;


  @GET
  @Path("/{path}")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getResource(@PathParam("path") String path) throws IOException {
    try {
      String s = "rest/www/" + path;
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      InputStream is = new BufferedInputStream(cl.getResource(s).openStream());
      
      String mime = "text/plain";
      if (s.endsWith(".js")) {
        mime = "text/javascript";
      } else if (s.endsWith(".css")) {
        mime = "text/css";
      } else {
        mime = URLConnection.guessContentTypeFromStream(is);
      }
      
      byte[] d = IOUtils.toByteArray(is);
      return Response.ok(d).type(mime).build(); 
    } catch (Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
    }
    
    return Response.noContent().status(Status.NOT_FOUND).build();
  }
}
