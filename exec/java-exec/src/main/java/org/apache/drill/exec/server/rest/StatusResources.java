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

import java.util.LinkedList;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;

@Path("/")
public class StatusResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatusResources.class);

  @Inject
  WorkManager work;

  @GET
  @Path("/status")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getStatus() {
    String status = "Running!";
    return new Viewable("/rest/status.ftl", status);
  }

  @GET
  @Path("/options.json")
  @Produces(MediaType.APPLICATION_JSON)
  public List getSystemOptionsJSON() {
    List<OptionWrapper> options = new LinkedList<>();
    for (OptionValue option : work.getContext().getOptionManager()) {
      options.add(new OptionWrapper(option.name, option.getValue(), option.type, option.kind));
    }
    return options;
  }

  @GET
  @Path("/options")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getSystemOptions() {
    return new Viewable("/rest/options.ftl", getSystemOptionsJSON());
  }

  @POST
  @Path("/option/{optionName}")
  @Consumes("application/x-www-form-urlencoded")
  @Produces(MediaType.TEXT_HTML)
  public Viewable updateSystemOption(@FormParam("name") String name, @FormParam("value") String value,
                                   @FormParam("kind") String kind) {
    try {
      work.getContext()
        .getOptionManager()
        .setOption(OptionValue.createOption(
          OptionValue.Kind.valueOf(kind),
          OptionValue.OptionType.SYSTEM,
          name,
          value));
    } catch (Exception e) {
      logger.debug("Could not update.", e);
    }
    return getSystemOptions();
  }

  @XmlRootElement
  public class OptionWrapper {

    private String name;
    private Object value;
    private OptionValue.OptionType type;
    private String kind;

    @JsonCreator
    public OptionWrapper(String name, Object value, OptionValue.OptionType type, Kind kind) {
      this.name = name;
      this.value = value;
      this.type = type;
      this.kind = kind.name();
    }

    public String getName() {
      return name;
    }

    @JsonIgnore
    public String getValueAsString() {
      return value.toString();
    }

    public Object getValue() {
      return value;
    }

    public OptionValue.OptionType getType() {
      return type;
    }

    public String getKind() {
      return kind;
    }
  }

}
