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
package org.apache.drill.exec.store.druid.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.druid.druid.DruidSelectResponse;
import org.apache.drill.exec.store.druid.druid.PagingIdentifier;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class DruidQueryClient {

  private static final Logger logger = LoggerFactory.getLogger(DruidQueryClient.class);

  private static final String QUERY_BASE_URI = "/druid/v2";
  private static final ObjectMapper mapper = new ObjectMapper();

  private final RestClient restClient;
  private final String queryUrl;

  public DruidQueryClient(String brokerURI, RestClient restClient) {
    queryUrl = brokerURI + QUERY_BASE_URI;
    this.restClient = restClient;
    logger.debug("Initialized DruidQueryClient with druidURL - {}", this.queryUrl);
  }

  public DruidSelectResponse executeQuery(String query) throws Exception {
    logger.debug("Executing Query - {}", query);
    HttpResponse response = restClient.post(queryUrl, query);

    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      throw UserException
          .dataReadError()
          .message("Error executing druid query. HTTP request failed")
          .addContext("Response code", response.getStatusLine().getStatusCode())
          .addContext("Response message", response.getStatusLine().getReasonPhrase())
          .build(logger);
    }

    String data = EntityUtils.toString(response.getEntity());
    ArrayNode responses = mapper.readValue(data, ArrayNode.class);
    return parseResponse(responses);
  }

  private DruidSelectResponse parseResponse(ArrayNode responses) {
    ArrayList<ObjectNode> events = new ArrayList<>();
    ObjectNode pagingIdentifiersNode = null;

    if (responses.size() > 0) {
      ObjectNode firstNode = (ObjectNode) responses.get(0);
      ObjectNode resultNode = (ObjectNode) firstNode.get("result");
      pagingIdentifiersNode = (ObjectNode) resultNode.get("pagingIdentifiers");
      ArrayNode eventsNode = (ArrayNode) resultNode.get("events");
      for(int i=0;i < eventsNode.size(); i++) {
        ObjectNode eventNode = (ObjectNode) eventsNode.get(i).get("event");
        events.add(eventNode);
      }
    }

    ArrayList<PagingIdentifier> pagingIdentifierList = new ArrayList<>();
    if (pagingIdentifiersNode != null) {
      for (Iterator<Map.Entry<String, JsonNode>> iterator = pagingIdentifiersNode.fields(); iterator.hasNext();) {
        Map.Entry<String, JsonNode> currentNode = iterator.next();
        if (currentNode != null) {
          String segmentName = currentNode.getKey();
          int segmentOffset = currentNode.getValue().asInt();
          PagingIdentifier pagingIdentifier = new PagingIdentifier(segmentName, segmentOffset);
          pagingIdentifierList.add(pagingIdentifier);
        }
      }
    }

    DruidSelectResponse druidSelectResponse = new DruidSelectResponse();
    if (CollectionUtils.isNotEmpty(pagingIdentifierList)) {
      druidSelectResponse.setPagingIdentifiers(pagingIdentifierList);
    }

    druidSelectResponse.setEvents(events);
    return druidSelectResponse;
  }
}
