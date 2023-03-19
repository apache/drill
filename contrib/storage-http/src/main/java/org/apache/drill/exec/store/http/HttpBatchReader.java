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
package org.apache.drill.exec.store.http;

import com.typesafe.config.Config;
import okhttp3.HttpUrl;
import okhttp3.HttpUrl.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.FixedReceiver;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderOptions;
import org.apache.drill.exec.store.http.HttpApiConfig.PostLocation;
import org.apache.drill.exec.store.http.HttpPaginatorConfig.PaginatorMethod;
import org.apache.drill.exec.store.http.paginator.IndexPaginator;
import org.apache.drill.exec.store.http.paginator.Paginator;
import org.apache.drill.exec.store.http.util.HttpProxyConfig;
import org.apache.drill.exec.store.http.util.HttpProxyConfig.ProxyBuilder;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.apache.drill.exec.store.ImplicitColumnUtils.ImplicitColumns;
import org.apache.drill.exec.store.security.UsernamePasswordWithProxyCredentials;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HttpBatchReader implements ManagedReader<SchemaNegotiator> {

  private static final String[] STRING_METADATA_FIELDS = {"_response_message", "_response_protocol", "_response_url"};
  private static final String RESPONSE_CODE_FIELD = "_response_code";
  private static final Logger logger = LoggerFactory.getLogger(HttpBatchReader.class);

  private final HttpSubScan subScan;
  private final int maxRecords;
  protected final Paginator paginator;
  protected String baseUrl;
  private JsonLoader jsonLoader;
  private ResultSetLoader resultSetLoader;
  private final List<SchemaPath> projectedColumns;
  protected ImplicitColumns implicitColumns;
  private final Map<String, Object> paginationFields;

  public HttpBatchReader(HttpSubScan subScan) {
    this.subScan = subScan;
    this.maxRecords = subScan.maxRecords();
    this.baseUrl = subScan.tableSpec().connectionConfig().url();
    this.paginator = null;
    this.projectedColumns = subScan.columns();
    this.paginationFields = generatePaginationFieldMap();
  }

  public HttpBatchReader(HttpSubScan subScan, Paginator paginator) {
    this.subScan = subScan;
    this.maxRecords = subScan.maxRecords();
    this.paginator = paginator;
    this.baseUrl = paginator.next();
    this.projectedColumns = subScan.columns();
    this.paginationFields = generatePaginationFieldMap();
    logger.debug("Batch reader with URL: {}", this.baseUrl);
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {

    // Result set loader setup
    String tempDirPath = negotiator
        .drillConfig()
        .getString(ExecConstants.DRILL_TMP_DIR);

    HttpUrl url = buildUrl();
    logger.debug("Final URL: {}", url);

    CustomErrorContext errorContext = new ChildErrorContext(negotiator.parentErrorContext()) {
      @Override
      public void addContext(UserException.Builder builder) {
        super.addContext(builder);
        builder.addContext("URL", url.toString());
      }
    };
    negotiator.setErrorContext(errorContext);

    logger.debug("Executing request with url: {}", url);

    // Http client setup
    SimpleHttp http = SimpleHttp.builder()
      .scanDefn(subScan)
      .url(url)
      .tempDir(new File(tempDirPath))
      .proxyConfig(proxySettings(negotiator.drillConfig(), url))
      .errorContext(errorContext)
      .build();

    // JSON loader setup
    resultSetLoader = negotiator.build();
    if (implicitColumnsAreProjected()) {
      implicitColumns = new ImplicitColumns(resultSetLoader.writer());
      buildImplicitColumns();
    }

    // inStream is expected to be closed by the JsonLoader.
    InputStream inStream = http.getInputStream();
    populateImplicitFieldMap(http);

    try {
      JsonLoaderBuilder jsonBuilder = new JsonLoaderBuilder()
          .implicitFields(implicitColumns)
          .resultSetLoader(resultSetLoader)
          .standardOptions(negotiator.queryOptions())
          .maxRows(maxRecords)
          .dataPath(subScan.tableSpec().connectionConfig().dataPath())
          .errorContext(errorContext)
          .listenerColumnMap(paginationFields)
          .fromStream(inStream);

      if (subScan.tableSpec().connectionConfig().jsonOptions() != null) {
        JsonLoaderOptions jsonOptions = subScan
          .tableSpec()
          .connectionConfig()
          .jsonOptions()
          .getJsonOptions(negotiator.queryOptions());
        jsonBuilder.options(jsonOptions);
      } else {
        jsonBuilder.standardOptions(negotiator.queryOptions());
      }

      if (getSchema(negotiator) != null) {
        jsonBuilder.providedSchema(getSchema(negotiator));
      }

      jsonLoader = jsonBuilder.build();
    } catch (Throwable t) {

      // Paranoia: ensure stream is closed if anything goes wrong.
      // After this, the JSON loader will close the stream.
      AutoCloseables.closeSilently(inStream);
      AutoCloseables.closeSilently(http);
      throw t;
    }

    // Close the http client
    http.close();
    return true;
  }

  /**
   * This function obtains the correct schema for the {@link JsonLoader}.  There are four possibilities:
   * 1.  The schema is provided in the configuration only.  In this case, that schema will be returned.
   * 2.  The schema is provided in both the configuration and inline.  These two schemas will be merged together.
   * 3.  The schema is provided inline in a query.  In this case, that schema will be returned.
   * 4.  No schema is provided.  Function returns null.
   * @param negotiator {@link SchemaNegotiator} The schema negotiator with all the connection information
   * @return The built {@link TupleMetadata} of the provided schema, null if none provided.
   */
  private TupleMetadata getSchema(SchemaNegotiator negotiator) {
    if (subScan.tableSpec().connectionConfig().jsonOptions() != null &&
      subScan.tableSpec().connectionConfig().jsonOptions().schema() != null) {
      TupleMetadata configuredSchema = subScan.tableSpec().connectionConfig().jsonOptions().schema();

      // If it has a provided schema both inline and in the config, merge the two, otherwise, return the config schema
      if (negotiator.hasProvidedSchema()) {
        TupleMetadata inlineSchema = negotiator.providedSchema();
        return FixedReceiver.Builder.mergeSchemas(configuredSchema, inlineSchema);
      } else {
        return configuredSchema;
      }
    } else {
       if (negotiator.hasProvidedSchema()) {
         return negotiator.providedSchema();
       }
    }
    return null;
  }

  protected void buildImplicitColumns() {
    // Add String fields
    for (String fieldName : STRING_METADATA_FIELDS) {
      implicitColumns.addImplicitColumn(fieldName, MinorType.VARCHAR);
    }
    implicitColumns.addImplicitColumn(RESPONSE_CODE_FIELD, MinorType.INT);
  }

  protected void populateImplicitFieldMap(SimpleHttp http) {
    // If the implicit fields are not projected, skip this step
    if (! implicitColumnsAreProjected()) {
      return;
    }

    implicitColumns.getColumn(STRING_METADATA_FIELDS[0]).setValue(http.getResponseMessage());
    implicitColumns.getColumn(STRING_METADATA_FIELDS[1]).setValue(http.getResponseProtocol());
    implicitColumns.getColumn(STRING_METADATA_FIELDS[2]).setValue(http.getResponseURL());
    implicitColumns.getColumn(RESPONSE_CODE_FIELD).setValue(http.getResponseCode());
  }

  protected Map<String, Object> generatePaginationFieldMap() {
    if (paginator == null || paginator.getMode() != PaginatorMethod.INDEX) {
      return null;
    }

    Map<String, Object> fieldMap = new HashMap<>();
    IndexPaginator indexPaginator = (IndexPaginator) paginator;

    // Initialize the pagination field map
    if (StringUtils.isNotEmpty(indexPaginator.getIndexParam())) {
      fieldMap.put(indexPaginator.getIndexParam(), null);
    }

    if (StringUtils.isNotEmpty(indexPaginator.getHasMoreParam())) {
      fieldMap.put(indexPaginator.getHasMoreParam(), null);
    }

    if (StringUtils.isNotEmpty(indexPaginator.getNextPageParam())) {
      fieldMap.put(indexPaginator.getNextPageParam(), null);
    }
    return fieldMap;
  }

  protected boolean implicitColumnsAreProjected() {
    List<SchemaPath> columns = subScan.columns();
    for (SchemaPath path : columns) {
      if (path.nameEquals(STRING_METADATA_FIELDS[0]) ||
        path.nameEquals(STRING_METADATA_FIELDS[1]) ||
        path.nameEquals(STRING_METADATA_FIELDS[2]) ||
        path.nameEquals(RESPONSE_CODE_FIELD)) {
        return true;
      }
    }
    return false;
  }

  protected HttpUrl buildUrl() {
    logger.debug("Building URL from {}", baseUrl);
    HttpApiConfig apiConfig = subScan.tableSpec().connectionConfig();

    // Append table name, if present. When pagination is used, the paginator adds this.
    if (subScan.tableSpec().tableName() != null && paginator == null) {
      baseUrl += subScan.tableSpec().tableName();
    }

    // Add URL Parameters to baseURL
    HttpUrl parsedURL = HttpUrl.parse(baseUrl);
    baseUrl = SimpleHttp.mapURLParameters(parsedURL, subScan.filters());

    HttpUrl.Builder urlBuilder = HttpUrl.parse(baseUrl).newBuilder();
    if (apiConfig.params() != null &&
      !apiConfig.params().isEmpty() &&
        subScan.filters() != null) {
      addFilters(urlBuilder, apiConfig.params(), subScan.filters());
    }

    // Add limit parameter if defined and limit set
    if (! Strings.isNullOrEmpty(apiConfig.limitQueryParam()) && maxRecords > 0) {
      urlBuilder.addQueryParameter(apiConfig.limitQueryParam(), String.valueOf(maxRecords));
    }

    return urlBuilder.build();
  }

  /**
   * Convert equality filter conditions into HTTP query parameters
   * Parameters must appear in the order defined in the config.
   */
  protected void addFilters(Builder urlBuilder, List<String> params,
      Map<String, String> filters) {

    // If the request is a POST query and the user selected to push the filters to either JSON body
    // or the post body, do not add to the query string.
    if (subScan.tableSpec().connectionConfig().getMethodType() == HttpApiConfig.HttpMethod.POST &&
      (subScan.tableSpec().connectionConfig().getPostLocation() == PostLocation.POST_BODY ||
        subScan.tableSpec().connectionConfig().getPostLocation() == PostLocation.JSON_BODY)
    ) {
      return;
    }
    for (String param : params) {
      String value = filters.get(param);
      if (value != null) {
        urlBuilder.addQueryParameter(param, value);
      }
    }
  }

  protected HttpProxyConfig proxySettings(Config drillConfig, HttpUrl url) {
    final HttpStoragePluginConfig config = subScan.tableSpec().config();
    final ProxyBuilder builder = HttpProxyConfig.builder()
        .fromConfigForURL(drillConfig, url.toString());
    final String proxyType = config.proxyType();
    if (proxyType != null && !"direct".equals(proxyType)) {
      builder
        .type(config.proxyType())
        .host(config.proxyHost())
        .port(config.proxyPort());

      Optional<UsernamePasswordWithProxyCredentials> credentials = config.getUsernamePasswordCredentials();
      if (credentials.isPresent()) {
        builder.username(credentials.get().getUsername()).password(credentials.get().getPassword());
      }
    }
    return builder.build();
  }

  protected void populateIndexPaginator() {
    IndexPaginator indexPaginator = (IndexPaginator) paginator;
    // There are two cases, however in both, there is a boolean parameter which indicates
    // whether there are more pages to follow.  The first case is when the API provides
    // a URL for the next page and the other is when the API provides an offset which is added
    // to the next page.
    //
    // In ether case, we first need the boolean "has more" parameter so let's grab that.
    if (paginationFields.get(indexPaginator.getHasMoreParam()) != null) {
      Object hasMore = paginationFields.get(indexPaginator.getHasMoreParam());
      boolean hasMoreValues;

      if (hasMore instanceof Boolean) {
        hasMoreValues = (Boolean) hasMore;
      } else {
        String hasMoreString = hasMore.toString();
        // Attempt to convert to boolean
        hasMoreValues = Boolean.parseBoolean(hasMoreString);
      }
      indexPaginator.setHasMoreValue(hasMoreValues);

      // If there are no more values, notify the paginator
      if (!hasMoreValues) {
        paginator.notifyPartialPage();
      }

      // At this point we know that there are more pages to come.  Send the data to the paginator and
      // use that to generate the next page.
      if (StringUtils.isNotEmpty(indexPaginator.getIndexParam()) && paginationFields.containsKey(indexPaginator.getIndexParam())) {
        indexPaginator.setIndexValue(paginationFields.get(indexPaginator.getIndexParam()).toString());
      }

      if (StringUtils.isNotEmpty(indexPaginator.getNextPageParam()) && paginationFields.containsKey(indexPaginator.getNextPageParam())) {
        Object nextPageValue = paginationFields.get(indexPaginator.getNextPageParam());
        if (nextPageValue != null) {
          indexPaginator.setNextPageValue(nextPageValue.toString());
        } else {
          // If the next URL is null, notify the paginator and stop paginating
          paginator.notifyPartialPage();
          indexPaginator.setNextPageValue(null);
        }
      }
    } else {
      // This covers the case of keyset/index pagination where there isn't a boolean parameter to indicate whether there are more results.
      // In this case, we will interpret the absence of the pagination field, receiving the same value, or a null value as the marker to stop pagination.

      // This represents the case when the index field is being used.
      if (StringUtils.isNotEmpty(indexPaginator.getIndexParam())) {
        if ((!paginationFields.containsKey(indexPaginator.getIndexParam())) || paginationFields.get(indexPaginator.getIndexParam()) == null) {
          // End pagination
          paginator.notifyPartialPage();
        } else {
          // Otherwise, check to see if the field is present but empty, or contains the value from the last page.
          // This will prevent runaway pagination calls.
          String indexParameter = paginationFields.get(indexPaginator.getIndexParam()).toString();
          // Empty value or the last value is the same as the current one.
          if (StringUtils.isEmpty(indexParameter) || (StringUtils.equals(indexParameter, indexPaginator.getLastIndexValue()))) {
            paginator.notifyPartialPage();
          } else {
            // Whew!  We made it... get the next page.
            indexPaginator.setIndexValue(indexParameter);
          }
        }
      } else {
        // Case when the next page field is used.
        if ((!paginationFields.containsKey(indexPaginator.getNextPageParam()))
            || paginationFields.get(indexPaginator.getNextPageParam()) == null
            || paginationFields.get(indexPaginator.getNextPageParam()) == "true") {
          // End pagination
          paginator.notifyPartialPage();
        } else {
          // Get the next page
          String nextURL = paginationFields.get(indexPaginator.getNextPageParam()).toString();
          // Empty value or the last value is the same as the current one.
          if (StringUtils.isEmpty(nextURL) || (StringUtils.equals(nextURL, indexPaginator.getLastNextPageValue()))) {
            paginator.notifyPartialPage();
          } else {
            // Whew!  We made it... get the next page.
            indexPaginator.setNextPageValue(nextURL);
          }
        }
      }
    }
  }

  @Override
  public boolean next() {
    boolean result = jsonLoader.readBatch();
    // This code implements the index/keyset pagination.  This pagination method
    // uses a value returned in the current result set as the starting point for the
    // next page.  Some APIs will have a boolean parameter to indicate that there are
    // additional pages.  Some APIs will also return a URL for the next page. In any event,
    // it is necessary to grab these values from the returned data.
    if (paginator != null && paginator.getMode() == PaginatorMethod.INDEX) {
      // First check to see if the limit has been reached.  If so, mark the end of pagination.
      long totalRowCount = resultSetLoader.totalRowCount();
      if (maxRecords > 0 && totalRowCount >= maxRecords) {
        // End Pagination
        paginator.notifyPartialPage();
        // Returning false here because if there is a partial page, we want the reader to stop and no further batches to be created.
        return false;
      } else {
        populateIndexPaginator();
      }
    } else if (paginator != null &&
      maxRecords < 0 && (resultSetLoader.totalRowCount()) < paginator.getPageSize()) {
    // Allows limitless pagination. Does not apply for index pagination
      logger.debug("Partially filled page received, ending pagination");
      paginator.notifyPartialPage();
    }
    return result;
  }

  @Override
  public void close() {
    if (jsonLoader != null) {
      jsonLoader.close();
      jsonLoader = null;
    }
  }
}
