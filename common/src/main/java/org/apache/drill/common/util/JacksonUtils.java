package org.apache.drill.common.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

public final class JacksonUtils {

  /**
   * Creates a new instance of the Jackson {@link ObjectMapper}.
   * @return an {@link ObjectMapper} instance
   */
  public static ObjectMapper createObjectMapper() {
    return createJsonMapperBuilder().build();
  }

  /**
   * Creates a new instance of the Jackson {@link ObjectMapper}.
   * @param factory a {@link JsonFactory} instance
   * @return an {@link ObjectMapper} instance
   */
  public static ObjectMapper createObjectMapper(final JsonFactory factory) {
    return createJsonMapperBuilder(factory).build();
  }

  /**
   * Creates a new instance of the Jackson {@link JsonMapper.Builder}.
   * @return an {@link JsonMapper.Builder} instance
   */
  public static JsonMapper.Builder createJsonMapperBuilder() {
    return JsonMapper.builder();
  }

  /**
   * Creates a new instance of the Jackson {@link JsonMapper.Builder}.
   * @param factory a {@link JsonFactory} instance
   * @return an {@link JsonMapper.Builder} instance
   */
  public static JsonMapper.Builder createJsonMapperBuilder(final JsonFactory factory) {
    return JsonMapper.builder(factory);
  }
}
