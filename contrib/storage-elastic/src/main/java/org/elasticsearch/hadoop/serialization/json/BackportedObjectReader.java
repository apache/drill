/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.serialization.json;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.DeserializerProvider;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.deser.StdDeserializationContext;
import org.codehaus.jackson.map.introspect.VisibilityChecker;
import org.codehaus.jackson.map.jsontype.TypeResolverBuilder;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ReflectionUtils;

/**
 * Backported class from Jackson 1.8.8 for Jackson 1.5.2.
 * Used only when dealing with Jackson 1.5 otherwise the proper Jackson class is used which
 * saves us the hassle of keeping up with the breaking changes in Jackson library.
 *
 * Builder object that can be used for per-serialization configuration of
 * deserialization parameters, such as root type to use or object
 * to update (instead of constructing new instance).
 * Uses "fluid" (aka builder) pattern so that instances are immutable
 * (and thus fully thread-safe with no external synchronization);
 * new instances are constructed for different configurations.
 * Instances are initially constructed by {@link ObjectMapper} and can be
 * reused.
 *
 * @author tatu
 * @since 1.6
 */

public class BackportedObjectReader implements ObjectReader {

    final static Field ROOT_DESERIALIZERS;

    static {
        Field fl = ReflectionUtils.findField(ObjectMapper.class, "_rootDeserializers");
        Assert.notNull(fl, "Cannot find root deserializers");
        ROOT_DESERIALIZERS = fl;
        ReflectionUtils.makeAccessible(fl);
    }

    /*
    /**********************************************************
    /* Immutable configuration from ObjectMapper
    /**********************************************************
     */

    /**
     * Root-level cached deserializers
     */
    final protected ConcurrentHashMap<JavaType, JsonDeserializer<Object>> _rootDeserializers;

    /**
     * General serialization configuration settings
     */
    protected final DeserializationConfig _config;

    protected final DeserializerProvider _provider;

    /**
     * Factory used for constructing {@link JsonGenerator}s
     */
    protected final JsonFactory _jsonFactory;

    // Support for polymorphic types:
    protected TypeResolverBuilder<?> _defaultTyper;

    // Configurable visibility limits
    protected VisibilityChecker<?> _visibilityChecker;

    /*
    /**********************************************************
    /* Configuration that can be changed during building
    /**********************************************************
     */

    /**
     * Declared type of value to instantiate during deserialization.
     * Defines which deserializer to use; as well as base type of instance
     * to construct if an updatable value is not configured to be used
     * (subject to changes by embedded type information, for polymorphic
     * types). If {@link #_valueToUpdate} is non-null, only used for
     * locating deserializer.
     */
    protected final JavaType _valueType;

    /**
     * Instance to update with data binding; if any. If null,
     * a new instance is created, if non-null, properties of
     * this value object will be updated instead.
     * Note that value can be of almost any type, except not
     * {@link org.codehaus.jackson.map.type.ArrayType}; array
     * types can not be modified because array size is immutable.
     */
    protected final Object _valueToUpdate;


    public static BackportedObjectReader create(ObjectMapper mapper, Class<?> type) {
        return new BackportedObjectReader(mapper, TypeFactory.type(type), null);
    }

    /**
     * Constructor used by {@link ObjectMapper} for initial instantiation
     */
    protected BackportedObjectReader(ObjectMapper mapper, JavaType valueType, Object valueToUpdate) {
        _rootDeserializers = ReflectionUtils.getField(ROOT_DESERIALIZERS, mapper);
        _provider = mapper.getDeserializerProvider();
        _jsonFactory = mapper.getJsonFactory();

        // must make a copy at this point, to prevent further changes from trickling down
        _config = mapper.copyDeserializationConfig();

        _valueType = valueType;
        _valueToUpdate = valueToUpdate;
        if (valueToUpdate != null && valueType.isArrayType()) {
            throw new IllegalArgumentException("Can not update an array value");
        }
    }


    public <T> BackportedJacksonMappingIterator<T> readValues(JsonParser jp) throws IOException,
            JsonProcessingException {
        DeserializationContext ctxt = _createDeserializationContext(jp, _config);
        return new BackportedJacksonMappingIterator<T>(_valueType, jp, ctxt, _findRootDeserializer(_config, _valueType));
    }


    /**
     * Method called to locate deserializer for the passed root-level value.
     */
    protected JsonDeserializer<Object> _findRootDeserializer(DeserializationConfig cfg, JavaType valueType)
            throws JsonMappingException {

        // Sanity check: must have actual type...
        if (valueType == null) {
            throw new JsonMappingException("No value type configured for ObjectReader");
        }

        // First: have we already seen it?
        JsonDeserializer<Object> deser = _rootDeserializers.get(valueType);
        if (deser != null) {
            return deser;
        }

        // es-hadoop: findType with 2 args have been removed since 1.9 so this code compiles on 1.8 (which has the fallback method)
        // es-hadoop: on 1.5 only the 2 args method exists, since 1.9 only the one with 3 args hence the if

        // Nope: need to ask provider to resolve it
        //modify by zhoupeiyuan
        deser = _provider.findTypedValueDeserializer(cfg, valueType ,null);
        if (deser == null) { // can this happen?
            throw new JsonMappingException("Can not find a deserializer for type " + valueType);
        }
        _rootDeserializers.put(valueType, deser);
        return deser;
    }

    protected DeserializationContext _createDeserializationContext(JsonParser jp, DeserializationConfig cfg) {
        // 04-Jan-2010, tatu: we do actually need the provider too... (for polymorphic deser)
        //modify by zhoupeiyuan
        return new StdDeserializationContext(cfg, jp, _provider ,null);
    }
}