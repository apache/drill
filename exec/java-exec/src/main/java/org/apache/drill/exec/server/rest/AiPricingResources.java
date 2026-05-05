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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.rest.ai.AiPricing;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Admin-only REST resource for managing per-model AI pricing used by the
 * AI analytics dashboard to estimate cost.
 */
@Path("/api/v1/ai/pricing")
@Tag(name = "AI Pricing", description = "Admin configuration of LLM pricing per model")
@RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
public class AiPricingResources {

  private static final Logger logger = LoggerFactory.getLogger(AiPricingResources.class);
  private static final String PRICING_STORE_NAME = "drill.sqllab.ai_pricing";

  @Inject
  WorkManager workManager;

  @Inject
  PersistentStoreProvider storeProvider;

  private static volatile PersistentStore<AiPricing> cachedStore;

  public static class PricingListResponse {
    @JsonProperty
    public List<AiPricing> entries;

    public PricingListResponse(List<AiPricing> entries) {
      this.entries = entries;
    }
  }

  public static class MessageResponse {
    @JsonProperty public String message;

    public MessageResponse(String message) {
      this.message = message;
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List pricing entries")
  public Response list() {
    try {
      List<AiPricing> entries = new ArrayList<>();
      Iterator<Map.Entry<String, AiPricing>> it = getStore().getAll();
      while (it.hasNext()) {
        entries.add(it.next().getValue());
      }
      return Response.ok(new PricingListResponse(entries)).build();
    } catch (Exception e) {
      logger.error("Error listing AI pricing", e);
      throw new DrillRuntimeException("Failed to list pricing: " + e.getMessage(), e);
    }
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create or update a pricing entry")
  public Response upsert(AiPricing entry) {
    if (entry == null || entry.getProvider() == null || entry.getModel() == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("provider and model are required"))
          .build();
    }
    if (entry.getCurrency() == null || entry.getCurrency().isEmpty()) {
      entry.setCurrency("USD");
    }
    entry.setUpdatedAt(Instant.now().toEpochMilli());
    try {
      getStore().put(AiPricing.key(entry.getProvider(), entry.getModel()), entry);
      return Response.ok(entry).build();
    } catch (Exception e) {
      logger.error("Error saving AI pricing", e);
      throw new DrillRuntimeException("Failed to save pricing: " + e.getMessage(), e);
    }
  }

  @PUT
  @Path("/{provider}/{model}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Update pricing for a provider+model pair")
  public Response update(@PathParam("provider") String provider,
      @PathParam("model") String model, AiPricing entry) {
    if (entry == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new MessageResponse("body required"))
          .build();
    }
    entry.setProvider(provider);
    entry.setModel(model);
    return upsert(entry);
  }

  @DELETE
  @Path("/{provider}/{model}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete a pricing entry")
  public Response delete(@PathParam("provider") String provider,
      @PathParam("model") String model) {
    try {
      getStore().delete(AiPricing.key(provider, model));
      return Response.ok(new MessageResponse("deleted")).build();
    } catch (Exception e) {
      logger.error("Error deleting AI pricing", e);
      throw new DrillRuntimeException("Failed to delete pricing: " + e.getMessage(), e);
    }
  }

  private PersistentStore<AiPricing> getStore() {
    if (cachedStore == null) {
      synchronized (AiPricingResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = storeProvider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    AiPricing.class)
                .name(PRICING_STORE_NAME)
                .build()
            );
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access AI pricing store", e);
          }
        }
      }
    }
    return cachedStore;
  }

  /** Read-only access for analytics aggregation. Returns pricing keyed by "provider:model". */
  public static java.util.Map<String, AiPricing> snapshot(WorkManager workManager,
      PersistentStoreProvider storeProvider) {
    java.util.Map<String, AiPricing> map = new java.util.HashMap<>();
    try {
      PersistentStore<AiPricing> store;
      if (cachedStore != null) {
        store = cachedStore;
      } else {
        store = storeProvider.getOrCreateStore(
            PersistentStoreConfig.newJacksonBuilder(
                workManager.getContext().getLpPersistence().getMapper(),
                AiPricing.class)
            .name(PRICING_STORE_NAME)
            .build());
        cachedStore = store;
      }
      Iterator<Map.Entry<String, AiPricing>> it = store.getAll();
      while (it.hasNext()) {
        Map.Entry<String, AiPricing> e = it.next();
        map.put(e.getKey(), e.getValue());
      }
    } catch (Exception e) {
      logger.warn("Failed to read AI pricing snapshot", e);
    }
    return map;
  }
}
