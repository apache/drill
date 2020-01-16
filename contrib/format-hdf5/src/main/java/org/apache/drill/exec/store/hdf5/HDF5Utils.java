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

package org.apache.drill.exec.store.hdf5;

import ch.systemsx.cisd.hdf5.HDF5DataClass;
import ch.systemsx.cisd.hdf5.HDF5EnumerationValue;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation;
import ch.systemsx.cisd.hdf5.HDF5DataTypeInformation;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HDF5Utils {
  private static final Logger logger = LoggerFactory.getLogger(HDF5Utils.class);

  private static final boolean CASE_SENSITIVE = false;

  /*
   * This regex is used to extract the final part of an HDF5 path, which is the name of the data field or column.
   * While these look like file paths, they are fully contained within HDF5. This regex would extract part3 from:
   * /part1/part2/part3
   */
  private static final Pattern PATH_PATTERN = Pattern.compile("/*.*/(.+?)$");

  /**
   * This function returns and HDF5Attribute object for use when Drill maps the attributes.
   *
   * @param pathName The path to retrieve attributes from
   * @param key The key for the specific attribute you are retrieving
   * @param reader The IHDF5 reader object for the file you are querying
   * @return HDF5Attribute The attribute from the path with the key that was requested.
   */
  public static HDF5Attribute getAttribute(String pathName, String key, IHDF5Reader reader) {
    if (pathName.equals("")) {
      pathName = "/";
    }

    if (!reader.exists(pathName)) {
      return null;
    }

    if (key.equals("dimensions")) {
      HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
      long[] dimensions = datasetInfo.getDimensions();
      ArrayUtils.reverse(dimensions);
      return new HDF5Attribute(MinorType.LIST, "dimensions", dimensions);
    }

    if (key.equals("dataType")) {
      HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
      return new HDF5Attribute(getDataType(datasetInfo), "DataType", datasetInfo.getTypeInformation().getDataClass());
    }

    if (!reader.object().hasAttribute(pathName, key)) {
      return null;
    }

    HDF5DataTypeInformation attributeInfo = reader.object().getAttributeInformation(pathName, key);
    Class<?> type = attributeInfo.tryGetJavaType();
    if (type.isAssignableFrom(long[].class)) {
      if (attributeInfo.isSigned()) {
        return new HDF5Attribute(MinorType.BIGINT, key, reader.int64().getAttr(pathName, key));
      } else {
        return new HDF5Attribute(MinorType.BIGINT, key, reader.uint64().getAttr(pathName, key));
      }
    } else if (type.isAssignableFrom(int[].class)) {
      if (attributeInfo.isSigned()) {
        return new HDF5Attribute(MinorType.INT, key, reader.int32().getAttr(pathName, key));
      } else {
        return new HDF5Attribute(MinorType.INT, key, reader.uint32().getAttr(pathName, key));
      }
    } else if (type.isAssignableFrom(short[].class)) {
      if (attributeInfo.isSigned()) {
        return new HDF5Attribute(MinorType.INT, key, reader.int16().getAttr(pathName, key));
      } else {
        return new HDF5Attribute(MinorType.INT, key, reader.uint16().getAttr(pathName, key));
      }
    } else if (type.isAssignableFrom(byte[].class)) {
      if (attributeInfo.isSigned()) {
        return new HDF5Attribute(MinorType.INT, key, reader.int8().getAttr(pathName, key));
      } else {
        return new HDF5Attribute(MinorType.INT, key, reader.uint8().getAttr(pathName, key));
      }
    } else if (type.isAssignableFrom(double[].class)) {
      return new HDF5Attribute(MinorType.FLOAT8, key, reader.float64().getAttr(pathName, key));
    } else if (type.isAssignableFrom(float[].class)) {
      return new HDF5Attribute(MinorType.FLOAT8, key, reader.float32().getAttr(pathName, key));
    } else if (type.isAssignableFrom(String[].class)) {
      return new HDF5Attribute(MinorType.VARCHAR, key, reader.string().getAttr(pathName, key));
    } else if (type.isAssignableFrom(long.class)) {
      if (attributeInfo.isSigned()) {
        return new HDF5Attribute(MinorType.BIGINT, key, reader.int64().getAttr(pathName, key));
      } else {
        return new HDF5Attribute(MinorType.BIGINT, key, reader.uint64().getAttr(pathName, key));
      }
    } else if (type.isAssignableFrom(int.class)) {
      if (attributeInfo.isSigned()) {
        return new HDF5Attribute(MinorType.INT, key, reader.int32().getAttr(pathName, key));
      } else {
        return new HDF5Attribute(MinorType.INT, key, reader.uint32().getAttr(pathName, key));
      }
    } else if (type.isAssignableFrom(short.class)) {
      if (attributeInfo.isSigned()) {
        return new HDF5Attribute(MinorType.INT, key, reader.int16().getAttr(pathName, key));
      } else {
        return new HDF5Attribute(MinorType.INT, key, reader.uint16().getAttr(pathName, key));
      }
    } else if (type.isAssignableFrom(byte.class)) {
      if (attributeInfo.isSigned()) {
        return new HDF5Attribute(MinorType.INT, key, reader.int8().getAttr(pathName, key));
      } else {
        return new HDF5Attribute(MinorType.INT, key, reader.uint8().getAttr(pathName, key));
      }
    } else if (type.isAssignableFrom(double.class)) {
      return new HDF5Attribute(MinorType.FLOAT8, key, reader.float64().getAttr(pathName, key));
    } else if (type.isAssignableFrom(float.class)) {
      return new HDF5Attribute(MinorType.FLOAT4, key, reader.float32().getAttr(pathName, key));
    } else if (type.isAssignableFrom(String.class)) {
      return new HDF5Attribute(MinorType.VARCHAR, key, reader.string().getAttr(pathName, key));
    } else if (type.isAssignableFrom(boolean.class)) {
      return new HDF5Attribute(MinorType.BIT, key, reader.bool().getAttr(pathName, key));
    } else if (type.isAssignableFrom(HDF5EnumerationValue.class)) {
      // Convert HDF5 Enum to String
      return new HDF5Attribute(MinorType.GENERIC_OBJECT, key, reader.enumeration().getAttr(pathName, key));
    } else if (type.isAssignableFrom(BitSet.class)) {
      return new HDF5Attribute(MinorType.BIT, key, reader.bool().getAttr(pathName, key));
    }

    logger.warn("Reading attributes of type {} not yet implemented.", attributeInfo);
    return null;
  }

  /**
   * This function returns the Drill data type of a given HDF5 dataset.
   * @param datasetInfo The input data set.
   * @return MinorType The Drill data type of the dataset in question
   */
  public static MinorType getDataType(HDF5DataSetInformation datasetInfo) {

    HDF5DataTypeInformation typeInfo = datasetInfo.getTypeInformation();
    Class<?> type = typeInfo.tryGetJavaType();
    String name = typeInfo.getDataClass().name();
    if (type == null) {
      logger.warn("Datasets of type {} not implemented.", typeInfo);
      //Fall back to string
      if(name.equalsIgnoreCase("OTHER")) {
        return MinorType.GENERIC_OBJECT;
      } else {
        return MinorType.VARCHAR;
      }
    } else if (type.isAssignableFrom(long.class)) {
      return MinorType.BIGINT;
    } else if (type.isAssignableFrom(short.class)) {
      return MinorType.SMALLINT;
    } else if (type.isAssignableFrom(byte.class)) {
      return MinorType.TINYINT;
    } else if (type.isAssignableFrom(int.class)) {
      return MinorType.INT;
    } else if (type.isAssignableFrom(double.class) || type.isAssignableFrom(float.class)) {
      return MinorType.FLOAT8;
    } else if (type.isAssignableFrom(String.class)) {
      return MinorType.VARCHAR;
    } else if (type.isAssignableFrom(java.util.Date.class)) {
      return MinorType.TIMESTAMP;
    } else if (type.isAssignableFrom(boolean.class) || type.isAssignableFrom(BitSet.class)) {
      return MinorType.BIT;
    } else if (type.isAssignableFrom(Map.class)) {
      return MinorType.MAP;
    } else if (type.isAssignableFrom(Enum.class)) {
      return MinorType.GENERIC_OBJECT;
    }
    return MinorType.GENERIC_OBJECT;
  }

  /**
   * This function gets the type of dataset
   * @param path The path of the dataset
   * @param reader The HDF5 reader
   * @return The data type
   */
  public static Class<?> getDatasetClass(String path, IHDF5Reader reader) {
    HDF5DataSetInformation info = reader.getDataSetInformation(resolvePath(path, reader));
    return info.getTypeInformation().tryGetJavaType();
  }

  /**
   * This function resolves path references
   * @param path The path for the possible reference
   * @param reader The HDF5 reader object for the file
   * @return the string for the relative path
   */
  public static String resolvePath(String path, IHDF5Reader reader) {
    if (reader.exists(path)) {
      // Resolve references, if any.
      if (reader.object().isDataSet(path)) {
        HDF5DataClass dataClass = reader.getDataSetInformation(path).getTypeInformation().getDataClass();
        if (dataClass.toString().equals("REFERENCE")) {
          return reader.reference().read(path);
        }
      }
      return path;
    } else if (!CASE_SENSITIVE) {
      // Look for a match with a different case.
      String[] pathParts = path.split("/");
      String resolvedPath = "/";
      for (int i = 1; i < pathParts.length; i++) {
        String testPath = (resolvedPath.endsWith("/")) ? resolvedPath + pathParts[i] : resolvedPath + "/" + pathParts[i];
        if (reader.exists(testPath)) {
          resolvedPath = testPath;
        } else if (reader.isGroup(resolvedPath)) {
          List<String> children = reader.getGroupMembers(resolvedPath);
          for (String child : children) {
            if (child.equalsIgnoreCase(pathParts[i])) {
              resolvedPath = resolvedPath + "/" + child;
            }
          }
        }
      }
      if (path.equalsIgnoreCase(resolvedPath)) {
        return resolvedPath;
      }
    }
    return null;
  }

  /**
   * This helper function returns the name of a HDF5 record from a data path
   *
   * @param path Path to HDF5 data
   * @return String name of data
   */
  public static String getNameFromPath(String path) {
    if( path == null) {
      return null;
    }
    // Now create matcher object.
    Matcher m = PATH_PATTERN.matcher(path);
    if (m.find()) {
      return m.group(1);
    } else {
      return "";
    }
  }
}
