/*
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

import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import jakarta.annotation.security.PermitAll;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.work.WorkManager;
import org.apache.http.client.methods.HttpGet;
import org.glassfish.jersey.server.mvc.Viewable;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;

@Path("/")
@PermitAll
public class StatusResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatusResources.class);

  public static final String REST_API_SUFFIX = ".json";
  public static final String PATH_STATUS_JSON = "/status" + REST_API_SUFFIX;
  public static final String PATH_STATUS = "/status";
  public static final String PATH_METRICS = PATH_STATUS + "/metrics";
  public static final String PATH_OPTIONS_JSON = "/options" + REST_API_SUFFIX;
  public static final String PATH_INTERNAL_OPTIONS_JSON = "/internal_options" + REST_API_SUFFIX;
  public static final String PATH_OPTIONS = "/options";
  public static final String PATH_INTERNAL_OPTIONS = "/internal_options";
  @Inject
  UserAuthEnabled authEnabled;

  @Inject
  WorkManager work;

  @Inject
  SecurityContext sc;

  @Inject
  HttpServletRequest request;

  @GET
  @Path(StatusResources.PATH_STATUS_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(externalDocs = @ExternalDocumentation(description = "Apache Drill REST API documentation:", url = "https://drill.apache.org/docs/rest-api-introduction/"))
  public Pair<String, String> getStatusJSON() {
    return new ImmutablePair<>("status", "Running!");
  }

  @GET
  @Path(StatusResources.PATH_STATUS)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getStatus() {
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/status.ftl", sc, getStatusJSON());
  }

  /**
   * Returns the local Drillbit's metrics as JSON.
   * Replaces the old Codahale MetricsServlet that was removed during
   * the javax.servlet to jakarta.servlet migration.
   */
  @GET
  @Path(StatusResources.PATH_METRICS)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(externalDocs = @ExternalDocumentation(description = "Apache Drill REST API documentation:", url = "https://drill.apache.org/docs/rest-api-introduction/"))
  public Map<String, Object> getLocalMetrics() {
    MetricRegistry metrics = work.getContext().getMetrics();
    Map<String, Object> result = new LinkedHashMap<>();

    // Gauges
    Map<String, Object> gauges = new LinkedHashMap<>();
    for (Map.Entry<String, Gauge> entry : metrics.getGauges().entrySet()) {
      Map<String, Object> g = new LinkedHashMap<>();
      g.put("value", entry.getValue().getValue());
      gauges.put(entry.getKey(), g);
    }
    result.put("gauges", gauges);

    // Counters
    Map<String, Object> counters = new LinkedHashMap<>();
    for (Map.Entry<String, Counter> entry : metrics.getCounters().entrySet()) {
      Map<String, Object> c = new LinkedHashMap<>();
      c.put("count", entry.getValue().getCount());
      counters.put(entry.getKey(), c);
    }
    result.put("counters", counters);

    // Histograms
    Map<String, Object> histograms = new LinkedHashMap<>();
    for (Map.Entry<String, Histogram> entry : metrics.getHistograms().entrySet()) {
      Snapshot snap = entry.getValue().getSnapshot();
      Map<String, Object> h = new LinkedHashMap<>();
      h.put("count", entry.getValue().getCount());
      h.put("max", snap.getMax());
      h.put("mean", snap.getMean());
      h.put("min", snap.getMin());
      h.put("p50", snap.getMedian());
      h.put("p75", snap.get75thPercentile());
      h.put("p95", snap.get95thPercentile());
      h.put("p98", snap.get98thPercentile());
      h.put("p99", snap.get99thPercentile());
      h.put("p999", snap.get999thPercentile());
      h.put("stddev", snap.getStdDev());
      histograms.put(entry.getKey(), h);
    }
    result.put("histograms", histograms);

    // Meters
    Map<String, Object> meters = new LinkedHashMap<>();
    for (Map.Entry<String, Meter> entry : metrics.getMeters().entrySet()) {
      Meter m = entry.getValue();
      Map<String, Object> mData = new LinkedHashMap<>();
      mData.put("count", m.getCount());
      mData.put("m1_rate", m.getOneMinuteRate());
      mData.put("m5_rate", m.getFiveMinuteRate());
      mData.put("m15_rate", m.getFifteenMinuteRate());
      mData.put("mean_rate", m.getMeanRate());
      mData.put("units", "events/second");
      meters.put(entry.getKey(), mData);
    }
    result.put("meters", meters);

    // Timers
    Map<String, Object> timers = new LinkedHashMap<>();
    for (Map.Entry<String, Timer> entry : metrics.getTimers().entrySet()) {
      Timer t = entry.getValue();
      Snapshot snap = t.getSnapshot();
      Map<String, Object> tData = new LinkedHashMap<>();
      tData.put("count", t.getCount());
      tData.put("max", snap.getMax());
      tData.put("mean", snap.getMean());
      tData.put("min", snap.getMin());
      tData.put("p50", snap.getMedian());
      tData.put("p75", snap.get75thPercentile());
      tData.put("p95", snap.get95thPercentile());
      tData.put("p98", snap.get98thPercentile());
      tData.put("p99", snap.get99thPercentile());
      tData.put("p999", snap.get999thPercentile());
      tData.put("stddev", snap.getStdDev());
      tData.put("m1_rate", t.getOneMinuteRate());
      tData.put("m5_rate", t.getFiveMinuteRate());
      tData.put("m15_rate", t.getFifteenMinuteRate());
      tData.put("mean_rate", t.getMeanRate());
      tData.put("duration_units", "seconds");
      tData.put("rate_units", "calls/second");
      timers.put(entry.getKey(), tData);
    }
    result.put("timers", timers);

    return result;
  }

  @GET
  @Path(StatusResources.PATH_METRICS + "/{hostname}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(externalDocs = @ExternalDocumentation(description = "Apache Drill REST API documentation:", url = "https://drill.apache.org/docs/rest-api-introduction/"))
  public String getMetrics(@PathParam("hostname") String hostname) throws Exception {
    URL metricsURL = WebUtils.getDrillbitURL(work, request, hostname, StatusResources.PATH_METRICS);
    return WebUtils.doHTTPRequest(new HttpGet(metricsURL.toURI()), work.getContext().getConfig());
  }

  private List<OptionWrapper> getSystemOptionsJSONHelper(boolean internal)
  {
    List<OptionWrapper> options = new LinkedList<>();
    OptionManager optionManager = work.getContext().getOptionManager();
    OptionList optionList = internal ? optionManager.getInternalOptionList(): optionManager.getPublicOptionList();

    for (OptionValue option : optionList) {
      options.add(new OptionWrapper(option.name, option.getValue(), optionManager.getDefault(option.name).getValue().toString(), option.accessibleScopes, option.kind, option.scope));
    }

    Collections.sort(options, new Comparator<OptionWrapper>() {
      @Override
      public int compare(OptionWrapper o1, OptionWrapper o2) {
        return o1.name.compareTo(o2.name);
      }
    });
    return options;
  }

  @GET
  @Path(StatusResources.PATH_OPTIONS_JSON)
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(externalDocs = @ExternalDocumentation(description = "Apache Drill REST API documentation:", url = "https://drill.apache.org/docs/rest-api-introduction/"))
  public List<OptionWrapper> getSystemPublicOptionsJSON() {
    return getSystemOptionsJSONHelper(false);
  }

  @GET
  @Path(StatusResources.PATH_INTERNAL_OPTIONS_JSON)
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.APPLICATION_JSON)
  public List<OptionWrapper> getSystemInternalOptionsJSON() {
    return getSystemOptionsJSONHelper(true);
  }

  @GET
  @Path(StatusResources.PATH_OPTIONS)
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.TEXT_HTML)
  public Response getSystemPublicOptions() {
    return Response.seeOther(java.net.URI.create("/sqllab#/options")).build();
  }

  @GET
  @Path(StatusResources.PATH_INTERNAL_OPTIONS)
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.TEXT_HTML)
  public Response getSystemInternalOptions() {
    return Response.seeOther(java.net.URI.create("/sqllab#/options")).build();
  }

  @POST
  @Path("option/{optionName}")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  @Consumes("application/x-www-form-urlencoded")
  @Produces(MediaType.TEXT_HTML)
  public Response updateSystemOption(@FormParam("name") String name,
                                     @FormParam("value") String value,
                                     @FormParam("kind") String kind) {
    SystemOptionManager optionManager = work.getContext()
      .getOptionManager();

    try {
      optionManager.setLocalOption(OptionValue.Kind.valueOf(kind), name, value);
    } catch (Exception e) {
      logger.debug("Could not update.", e);
    }

    return Response.seeOther(java.net.URI.create("/sqllab#/options")).build();
  }

  @XmlRootElement
  public static class OptionWrapper {

    private String name;
    private Object value;
    private String defaultValue;
    private OptionValue.AccessibleScopes accessibleScopes;
    private String kind;
    private String optionScope;

    @JsonCreator
    public OptionWrapper(@JsonProperty("name") String name,
                         @JsonProperty("value") Object value,
                         @JsonProperty("defaultValue") String defaultValue,
                         @JsonProperty("accessibleScopes") OptionValue.AccessibleScopes type,
                         @JsonProperty("kind") Kind kind,
                         @JsonProperty("optionScope") OptionValue.OptionScope scope) {
      this.name = name;
      this.value = value;
      this.defaultValue = defaultValue;
      this.accessibleScopes = type;
      this.kind = kind.name();
      this.optionScope = scope.name();
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

    public String getDefaultValue() {
      return defaultValue;
    }

    public OptionValue.AccessibleScopes getAccessibleScopes() {
      return accessibleScopes;
    }

    public String getKind() {
      return kind;
    }

    public String getOptionScope() {
      return optionScope;
    }

    @Override
    public String toString() {
      return "OptionWrapper{" + "name='" + name + '\'' + ", value=" + value + ", default=" + defaultValue + ", accessibleScopes=" + accessibleScopes + ", kind='" + kind + '\'' + ", scope='" + optionScope + '\'' +'}';
    }
  }
}
