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

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;

public abstract class ReflectionUtils {

    public static Field findField(Class<?> clazz, String name) {
        return findField(clazz, name, null);
    }

    public static Field findField(Class<?> clazz, String name, Class<?> type) {
        Assert.notNull(clazz, "Class must not be null");
        Assert.isTrue(name != null || type != null, "Either name or type of the field must be specified");
        Class<?> searchType = clazz;
        while (!Object.class.equals(searchType) && searchType != null) {
            Field[] fields = searchType.getDeclaredFields();
            for (Field field : fields) {
                if ((name == null || name.equals(field.getName())) && (type == null || type.equals(field.getType()))) {
                    return field;
                }
            }
            searchType = searchType.getSuperclass();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getField(Field field, Object target) {
        try {
            return (T) field.get(target);
        } catch (IllegalAccessException ex) {
            throw new EsHadoopIllegalStateException("Unexpected reflection exception - " + ex.getClass().getName() + ": "+ ex.getMessage());
        }
    }

    public static void setField(Field field, Object target, Object value) {
        try {
            field.set(target, value);
        } catch (IllegalAccessException ex) {
            throw new EsHadoopIllegalStateException("Unexpected reflection exception - " + ex.getClass().getName() + ": "+ ex.getMessage());
        }
    }

    public static void makeAccessible(AccessibleObject accessible) {
        if (!accessible.isAccessible()) {
            accessible.setAccessible(true);
        }
    }

    public static Method findMethod(Class<?> targetClass, String name, Class<?>... paramTypes) {
        while (targetClass != null) {
            Method[] methods = (targetClass.isInterface() ? targetClass.getMethods() : targetClass.getDeclaredMethods());
            for (Method method : methods) {
                if (name.equals(method.getName())
                        && (paramTypes == null || Arrays.equals(paramTypes, method.getParameterTypes()))) {
                    return method;
                }
            }
            targetClass = targetClass.getSuperclass();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <T> T invoke(Method method, Object target, Object...args) {
        try {
            return (T) method.invoke(target, args);
        } catch (Exception ex) {
            throw new EsHadoopIllegalArgumentException("Cannot invoke method " + method, ex);
        }
    }
}
