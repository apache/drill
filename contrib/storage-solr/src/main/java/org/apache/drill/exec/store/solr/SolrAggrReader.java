/**
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
package org.apache.drill.exec.store.solr;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.solr.schema.SolrSchemaField;
import org.apache.drill.exec.store.solr.schema.SolrSchemaPojo;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SolrAggrReader extends AbstractRecordReader {
  static final Logger logger = LoggerFactory.getLogger(SolrAggrReader.class);

  private FragmentContext fc;
  protected Map<String, ValueVector> vectors = Maps.newHashMap();
  protected String solrServerUrl;
  protected SolrClient solrClient;
  protected SolrSubScan solrSubScan;
  protected List<SolrScanSpec> scanList;
  protected SolrClientAPIExec solrClientApiExec;
  protected OutputMutator outputMutator;
  protected List<String> fields;
  private MajorType.Builder t;
  Map<String, FieldStatsInfo> fieldStatsInfoMap;
  private List<SolrAggrParam> solrAggrParams;
  private Map<String, SolrSchemaField> schemaFieldMap;

  public SolrAggrReader(FragmentContext context, SolrSubScan config) {
    fc = context;
    solrSubScan = config;

    solrServerUrl = solrSubScan.getSolrPlugin().getSolrStorageConfig()
        .getSolrServer();
    scanList = solrSubScan.getScanList();
    solrClientApiExec = solrSubScan.getSolrPlugin().getSolrClientApiExec();
    solrClient = solrSubScan.getSolrPlugin().getSolrClient();

    String solrCoreName = scanList.get(0).getSolrCoreName();
    List<SchemaPath> colums = config.getColumns();
    SolrFilterParam filters = config.getSolrScanSpec().getFilter();
    solrAggrParams = config.getSolrScanSpec().getAggrParams();
    SolrSchemaPojo oCVSchema = config.getSolrScanSpec().getCvSchema();

    if (oCVSchema.getSchemaFields() != null) {
      schemaFieldMap = new HashMap<String, SolrSchemaField>(oCVSchema
          .getSchemaFields().size());

      for (SolrSchemaField cvSchemaField : oCVSchema.getSchemaFields()) {
        if (!cvSchemaField.isSkipdelete()) {
          schemaFieldMap.put(cvSchemaField.getFieldName(), cvSchemaField);
        }
      }
    }
    StringBuilder sb = new StringBuilder();

    if (filters != null) {
      for (String filter : filters) {
        sb.append(filter);
      }
    }

    setColumns(colums);
    // Query Response
    if (!solrAggrParams.isEmpty()) {
      QueryResponse queryRsp = solrClientApiExec.getSolrFieldStats(
          solrServerUrl, solrCoreName, oCVSchema.getUniqueKey(), this.fields,
          sb);
      if (queryRsp != null) {
        fieldStatsInfoMap = queryRsp.getFieldStatsInfo();
      }
    }

  }

  @Override
  protected Collection<SchemaPath> transformColumns(
      Collection<SchemaPath> projectedColumns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      logger
          .debug(" This is not a star query, restricting response to projected columns only "
              + projectedColumns);
      fields = Lists.newArrayListWithExpectedSize(projectedColumns.size());
      for (SchemaPath column : projectedColumns) {
        String fieldName = column.getRootSegment().getPath();
        if (schemaFieldMap.containsKey(fieldName)) {
          transformed.add(SchemaPath.getSimplePath(fieldName));
          this.fields.add(fieldName);
        }

      }
    } else {
      fields = Lists.newArrayListWithExpectedSize(schemaFieldMap.size());
      for (String fieldName : schemaFieldMap.keySet()) {
        this.fields.add(fieldName);
      }
      transformed.add(AbstractRecordReader.STAR_COLUMN);
    }
    return transformed;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output)
      throws ExecutionSetupException {
    logger.debug("SolrAggrReader :: setup");
    int counter = 0;
    if (fieldStatsInfoMap != null) {
      for (SolrAggrParam solrAggrParam : solrAggrParams) {
        if (fieldStatsInfoMap.containsKey(solrAggrParam.getFieldName())) {
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.BIGINT);
          MaterializedField m_field = MaterializedField.create(
              solrAggrParam.getFieldName(), t.build());
          try {
            String key = solrAggrParam.getFunctionName() + "_" + counter;
            vectors.put(key,
                output.addField(m_field, NullableBigIntVector.class));
            counter++;
          } catch (SchemaChangeException e) {

          }
        }

      }
    }
  }

  @Override
  public int next() {
    logger.debug("SolrAggrReader :: next");
    int counter = 0;
    if (!vectors.isEmpty()) {
      for (SolrAggrParam solrAggrParam : solrAggrParams) {
        String functionName = solrAggrParam.getFunctionName();
        String key = functionName + "_" + counter;
        ValueVector vv = vectors.get(functionName + "_" + counter);
        String solrField = solrAggrParam.getFieldName();
        FieldStatsInfo fieldStats = fieldStatsInfoMap.get(solrField);

        if (vv.getClass().equals(NullableBigIntVector.class)) {
          NullableBigIntVector v = (NullableBigIntVector) vv;

          Object value = null;
          if (functionName.equalsIgnoreCase("sum")) {
            value = fieldStats.getSum();
          } else if (functionName.equalsIgnoreCase("count")) {
            value = fieldStats.getCount();
          } else if (functionName.equalsIgnoreCase("min")) {
            value = fieldStats.getMin();
          } else if (functionName.equalsIgnoreCase("max")) {
            value = fieldStats.getMax();
          } else {
            logger.debug("yet to implement function type [ " + functionName
                + " ]");
          }
          Long l = 0l;
          if (value != null) {
            BigDecimal bd = new BigDecimal(value.toString());
            l = bd.longValue();
          }
          logger.debug("functionName [ " + functionName + " ] value is " + l
              + " index " + counter);
          v.getMutator().setSafe(counter, l);

        }
        counter++;
      }
      for (String functionName : vectors.keySet()) {
        ValueVector vv = vectors.get(functionName);
        vv.getMutator().setValueCount(counter > 0 ? counter : 0);
      }
      vectors.clear();
    }
    return counter;
  }

  @Override
  public void close() {
  }
}
