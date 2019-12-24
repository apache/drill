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
package org.elasticsearch.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.SettingsAware;

public abstract class ObjectUtils {

    @SuppressWarnings("unchecked")
    public static <T> T instantiate(String className, ClassLoader loader) {
        Assert.hasText(className, "No class name given");
        ClassLoader cl = (loader != null ? loader : ObjectUtils.class.getClassLoader());
        Class<?> clz = null;
        try {
            clz = cl.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new EsHadoopIllegalStateException(String.format("Cannot load class [%s]", className), e);
        }
        try {
            return (T) clz.newInstance();
        } catch (Exception ex) {
            throw new EsHadoopIllegalStateException(String.format("Cannot instantiate class [%s]", className), ex);
        }
    }

    public static <T> T instantiate(String className, Settings settings) {
        return instantiate(className, null, settings);
    }

    public static <T> T instantiate(String className, ClassLoader loader, Settings settings) {
        T obj = instantiate(className, loader);

        if (obj instanceof SettingsAware) {
            ((SettingsAware) obj).setSettings(settings);
        }

        return obj;
    }

    public static boolean isClassPresent(String className, ClassLoader cl) {
        Class<?> clz = null;
        try {
            clz = Class.forName(className, false, cl);
        } catch (Exception ex) {
            // ignore
        }
        return (clz != null);
    }

    public static Class<?> loadClass(String className, ClassLoader cl) {
        try {
            return Class.forName(className, true, cl);
        } catch (ClassNotFoundException ex) {
            throw new EsHadoopIllegalArgumentException(String.format("Cannot load class [%s]", className), ex);
        }
    }

    public static boolean isEmpty(byte[] array) {
        return (array == null || array.length == 0);
    }

    public static boolean isEmpty(Object[] array) {
        return (array == null || array.length == 0);
    }

    public static Object[] toObjectArray(Object source) {
        if (source instanceof Object[]) {
            return (Object[]) source;
        }
        if (source == null) {
            return new Object[0];
        }
        if (!source.getClass().isArray()) {
            throw new IllegalArgumentException("Source is not an array: " + source);
        }
        int length = Array.getLength(source);
        if (length == 0) {
            return new Object[0];
        }
        Class<?> wrapperType = Array.get(source, 0).getClass();
        Object[] newArray = (Object[]) Array.newInstance(wrapperType, length);
        for (int i = 0; i < length; i++) {
            newArray[i] = Array.get(source, i);
        }
        return newArray;
    }
    
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }
    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }
}