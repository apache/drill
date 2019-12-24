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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException;

/**
 * Utility class used internally for the Pig support.
 */
public abstract class IOUtils {

    private final static Field BYTE_ARRAY_BUFFER;

    static {
        BYTE_ARRAY_BUFFER = ReflectionUtils.findField(ByteArrayInputStream.class, "buf");
        ReflectionUtils.makeAccessible(BYTE_ARRAY_BUFFER);
    }

    public static String serializeToBase64(Serializable object) {
        if (object == null) {
            return StringUtils.EMPTY;
        }
        FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
        } catch (IOException ex) {
            throw new EsHadoopSerializationException("Cannot serialize object " + object, ex);
        } finally {
            close(oos);
        }
        return DatatypeConverter.printBase64Binary(baos.bytes().bytes());
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T deserializeFromBase64(String data) {
        if (!StringUtils.hasLength(data)) {
            return null;
        }

        byte[] rawData = DatatypeConverter.parseBase64Binary(data);
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(new FastByteArrayInputStream(rawData));
            Object o = ois.readObject();
            return (T) o;
        } catch (ClassNotFoundException ex) {
            throw new EsHadoopIllegalStateException("cannot deserialize object", ex);
        } catch (IOException ex) {
            throw new EsHadoopSerializationException("cannot deserialize object", ex);
        } finally {
            close(ois);
        }
    }

    public static String propsToString(Properties props) {
        StringWriter sw = new StringWriter();
        if (props != null) {
            try {
                props.store(sw, "");
            } catch (IOException ex) {
                throw new EsHadoopIllegalArgumentException(ex);
            }
        }
        return sw.toString();
    }

    public static Properties propsFromString(String source) {
        Properties copy = new Properties();
        if (source != null) {
            try {
                copy.load(new StringReader(source));
            } catch (IOException ex) {
                throw new EsHadoopIllegalArgumentException(ex);
            }
        }
        return copy;
    }

    public static BytesArray asBytes(InputStream in) throws IOException {
        BytesArray ba = unwrapStreamBuffer(in);
        if (ba != null) {
            return ba;
        }
        return asBytes(new BytesArray(in.available()), in);
    }

    public static BytesArray asBytes(BytesArray ba, InputStream in) throws IOException {
        BytesArray buf = unwrapStreamBuffer(in);
        if (buf != null) {
            ba.bytes(buf);
            return ba;
        }

        FastByteArrayOutputStream bos = new FastByteArrayOutputStream(ba);
        byte[] buffer = new byte[1024];
        int read = 0;
        try {
            while ((read = in.read(buffer)) != -1) {
                bos.write(buffer, 0, read);
            }

        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                // ignore
            }
            // non needed but used to avoid the warnings
            bos.close();
        }
        return bos.bytes();
    }

    public static String asString(InputStream in) throws IOException {
        return asBytes(in).toString();
    }

    public static String asStringAlways(InputStream in) {
        if (in == null) {
            return StringUtils.EMPTY;
        }
        try {
            return asBytes(in).toString();
        } catch (IOException ex) {
            return StringUtils.EMPTY;
        }
    }

    public static InputStream open(String resource, ClassLoader loader) {
        if (loader == null) {
            loader = Thread.currentThread().getContextClassLoader();
        }

        if (loader == null) {
            loader = IOUtils.class.getClassLoader();
        }

        try {
            // no prefix means classpath
            if (!resource.contains(":")) {
                return loader.getResourceAsStream(resource);
            }
            return new URL(resource).openStream();
        } catch (IOException ex) {
            throw new EsHadoopIllegalArgumentException(String.format("Cannot open stream (inlined queries need to be marked as such through `?` and `{}`) for resource %s", resource));
        }
    }

    public static InputStream open(String location) {
        return open(location, null);
    }

    public static void close(Closeable closable) {
        if (closable != null) {
            try {
                closable.close();
            } catch (IOException e) {
                // silently ignore
            }
        }
    }

    private static byte[] byteArrayInputStreamInternalBuffer(ByteArrayInputStream bais) {
        return ReflectionUtils.getField(BYTE_ARRAY_BUFFER, bais);
    }

    private static BytesArray unwrapStreamBuffer(InputStream in) {
        if (in instanceof FastByteArrayInputStream) {
            return ((FastByteArrayInputStream) in).data;
        }

        if (in instanceof ByteArrayInputStream) {
            return new BytesArray(byteArrayInputStreamInternalBuffer((ByteArrayInputStream) in));
        }
        return null;
    }
}