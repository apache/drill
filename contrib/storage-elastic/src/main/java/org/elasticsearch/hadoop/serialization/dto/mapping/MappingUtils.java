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
package org.elasticsearch.hadoop.serialization.dto.mapping;

import java.util.*;

import org.apache.commons.logging.Log;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.FieldPresenceValidation;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoField.GeoType;
import org.elasticsearch.hadoop.serialization.field.FieldFilter;
import org.elasticsearch.hadoop.serialization.field.FieldFilter.NumberedInclude;
import org.elasticsearch.hadoop.util.StringUtils;

@SuppressWarnings("rawtypes")
public abstract class MappingUtils {

    private static final Set<String> BUILT_IN_FIELDS = new HashSet<String>();

    static {
        BUILT_IN_FIELDS.addAll(Arrays.asList("_uid", "_id", "_type", "_source", "_all", "_analyzer", "_boost",
                "_parent", "_routing", "_index", "_size", "_timestamp", "_ttl"));
    }

    public static void validateMapping(String fields, Field mapping, FieldPresenceValidation validation, Log log) {
        if (StringUtils.hasText(fields)) {
            validateMapping(StringUtils.tokenizeAndUriDecode(fields, ","), mapping, validation, log);
        }
    }

    public static void validateMapping(Collection<String> fields, Field mapping, FieldPresenceValidation validation, Log log) {
        if (mapping == null || fields == null || fields.isEmpty() || validation == null || FieldPresenceValidation.IGNORE == validation) {
            return;
        }

        List[] results = findTypos(fields, mapping);

        if (results == null) {
            return;
        }

        String message = String.format("Field(s) [%s] not found in the Elasticsearch mapping specified; did you mean [%s]?",
                removeDoubleBrackets(results[0]), removeDoubleBrackets(results[1]));
        if (validation == FieldPresenceValidation.WARN) {
            log.warn(message);
        }
        else {
            throw new EsHadoopIllegalArgumentException(message);
        }
    }

    // return a tuple for proper messages
    static List[] findTypos(Collection<String> fields, Field mapping) {
        Set<String> keys = Field.toLookupMap(mapping).keySet();

        // find missing
        List<String> missing = new ArrayList<String>(fields.size());

        for (String field : fields) {
            if (!keys.contains(field) && !isBuiltIn(field)) {
                missing.add(field);
            }
        }
        if (missing.isEmpty()) {
            return null;
        }
        Map<String, String> unwrapped = new LinkedHashMap<String, String>();
        // find similar
        for (String key : keys) {
            int match = key.lastIndexOf(".");
            if (match > 0) {
                String leafField = key.substring(match + 1);
                // leaf fields are secondary to top-level ones (in case of overwrite, the top level ones win)
                if (!unwrapped.containsKey(leafField)) {
                    unwrapped.put(leafField, key);
                }
            }
            unwrapped.put(key, key);
        }

        List<String> typos = new ArrayList<String>();

        Set<String> similar = unwrapped.keySet();
        for (String string : missing) {
            List<String> matches = StringUtils.findSimiliar(string, similar);
            for (String match : matches) {
                // get actual field
                typos.add(unwrapped.get(match));
            }
        }

        return new List[] { missing, typos };
    }

    private static boolean isBuiltIn(String field) {
        return BUILT_IN_FIELDS.contains(field);
    }

    private static String removeDoubleBrackets(List col) {
        if (col.isEmpty()) {
            return "<no-similar-match-found>";
        }
        if (col.size() == 1) {
            return col.get(0).toString();
        }
        return col.toString();
    }

    public static Field filter(Field field, Collection<String> includes, Collection<String> excludes) {
        if (field == null) {
            return field;
        }
        if (includes.isEmpty() && excludes.isEmpty()) {
          return field;
        }

        List<Field> filtered = new ArrayList<Field>();
        List<NumberedInclude> convertedIncludes = FieldFilter.toNumberedFilter(includes);

        boolean intact = true;
        for (Field fl : field.properties()) {
            intact &= processField(fl, null, filtered, convertedIncludes, excludes);
        }

        return (intact ? field : new Field(field.name(), field.type(), filtered));
    }

    private static boolean processField(Field field, String parentName, List<Field> filtered, Collection<NumberedInclude> includes, Collection<String> excludes) {
        String fieldName = (parentName != null ? parentName + "." + field.name() : field.name());

        boolean intact = true;
        
        if (FieldFilter.filter(fieldName, includes, excludes).matched) {
            if (FieldType.isCompound(field.type())) {
                List<Field> nested = new ArrayList<Field>();
                for (Field nestedField : field.properties()) {
                    intact &= processField(nestedField, field.name(), nested, includes, excludes);
                }
                filtered.add(new Field(field.name(), field.type(), nested));
            }
            else {
                filtered.add(field);
            }
        }
        else {
            intact = false;
        }
        return intact;
    }

    public static Field filterMapping(Field mapping, Settings cfg) {
        String readIncludeCfg = cfg.getReadFieldInclude();
        String readExcludeCfg = cfg.getReadFieldExclude();

        return filter(mapping, StringUtils.tokenize(readIncludeCfg), StringUtils.tokenize(readExcludeCfg));
    }

    private static void findGeo(Field field, String parentName, Map<String, GeoType> geoFields) {
        String fieldName = (parentName != null ? parentName + "." + field.name() : field.name());

        if (FieldType.GEO_POINT == field.type()) {
            geoFields.put(fieldName, GeoType.GEO_POINT);
        }
        else if (FieldType.GEO_SHAPE == field.type()) {
            geoFields.put(fieldName, GeoType.GEO_SHAPE);
        }
        else if (FieldType.isCompound(field.type())) {
            for (Field nestedField : field.properties()) {
                findGeo(nestedField, fieldName, geoFields);
            }
        }
    }

    public static Map<String, GeoType> geoFields(Field rootMapping) {
        if (rootMapping == null) {
            return Collections.emptyMap();
        }

        Map<String, GeoType> geoFields = new LinkedHashMap<String, GeoType>();
        // ignore the root field
        for (Field nestedField : rootMapping.properties()) {
            findGeo(nestedField, null, geoFields);
        }
        return geoFields;
    }

    public static GeoField parseGeoInfo(GeoType geoType, Object parsedContent) {
        if (geoType == GeoType.GEO_POINT) {
            return doParseGeoPointInfo(parsedContent);
        }
        if (geoType == GeoType.GEO_SHAPE) {
            return doParseGeoShapeInfo(parsedContent);
        }
        
        throw new EsHadoopIllegalArgumentException(String.format(Locale.ROOT, "Unknown GeoType %s", geoType));
    }

    private static GeoField doParseGeoPointInfo(Object parsedContent) {
        if (parsedContent instanceof List) {
            Object content = ((List) parsedContent).get(0);
            if (content instanceof Double) {
                return GeoPointType.LAT_LON_ARRAY;
            }
        }
        if (parsedContent instanceof String) {
            // check whether it's lat/lon or geohash
            return ((String) parsedContent).contains(",") ? GeoPointType.LAT_LON_STRING : GeoPointType.GEOHASH;  
        }

        return GeoPointType.LON_LAT_OBJECT;
    }

    private static GeoField doParseGeoShapeInfo(Object parsedContent) {
        if (parsedContent instanceof Map) {
            Map<String, Object> geoShape = (Map<String, Object>) parsedContent;
            Object typ = geoShape.get("type");
            if (typ == null) {
                throw new EsHadoopIllegalArgumentException(String.format(
                        Locale.ROOT, "Invalid GeoShape [%s]", parsedContent));
            }
            String type = typ.toString();
            GeoShapeType found = GeoShapeType.parse(type);
            if (found != null) {
                return found;
            }
        }
        throw new EsHadoopIllegalArgumentException(String.format(Locale.ROOT, "Unknown GeoShape [%s]", parsedContent));
    }
}