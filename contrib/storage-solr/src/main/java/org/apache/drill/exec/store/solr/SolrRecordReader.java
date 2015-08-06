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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.Charsets;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SolrRecordReader extends AbstractRecordReader {
  static final Logger logger = LoggerFactory.getLogger(SolrRecordReader.class);

  private FragmentContext fc;
  protected List<ValueVector> vectors = Lists.newArrayList();
  protected String solrServerUrl;
  protected SolrClient solrClient;
  protected List<SolrScanSpec> scanList;
  protected SolrClientAPIExec solrClientApiExec;
  protected OutputMutator outputMutator;
  // protected List<Tuple> resultTuple;
  protected SolrDocumentList solrDocList;
  protected Iterator<SolrDocument> resultIter;
  protected List<String> fields;
  private MajorType.Builder t;

  public SolrRecordReader(FragmentContext context, SolrSubScan config) {
    fc = context;
    solrServerUrl = config.getSolrPlugin().getSolrStorageConfig()
        .getSolrServer();
    scanList = config.getScanList();
    solrClientApiExec = config.getSolrPlugin().getSolrClientApiExec();
    solrClient = config.getSolrPlugin().getSolrClient();
    String solrCoreName = scanList.get(0).getSolrCoreName();
    List<SchemaPath> colums=config.getColumns();
    setColumns(colums);
    Map<String, String> solrParams = new HashMap<String, String>();
    solrParams.put("q", "*:*");
    solrParams.put("rows", String.valueOf(Integer.MAX_VALUE));
    solrParams.put("qt", "/select");
    
    solrDocList = solrClientApiExec.getSolrDocs(solrServerUrl, solrCoreName,this.fields);
    //solrClientApiExec.getSchemaForCore(solrCoreName);
    resultIter=solrDocList.iterator();
    logger.info("SolrRecordReader:: solrDocList:: " + solrDocList.size());
    
  }
  @Override
  protected Collection<SchemaPath> transformColumns(
      Collection<SchemaPath> projectedColumns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      logger.debug(" This is not a start query, restring response to ");
      fields = Lists.newArrayListWithExpectedSize(projectedColumns.size());
      for (SchemaPath column : projectedColumns) {
        String fieldName = column.getRootSegment().getPath();
        transformed.add(SchemaPath.getSimplePath(fieldName));
        this.fields.add(fieldName);
      }
    } else {
      transformed.add(AbstractRecordReader.STAR_COLUMN);
    }
    return transformed;
  }
  @Override
  public void setup(OperatorContext context, OutputMutator output)
      throws ExecutionSetupException {
    logger.info("SolrRecordReader :: setup");
    if (solrDocList != null) {
      SolrDocument solrDocument = solrDocList.get(0);

      Collection<String> fieldNames = solrDocument.getFieldNames();
      for (String field : fieldNames) {
        MaterializedField m_field = null;
        logger.debug("solr column is " + field);
        t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.VARCHAR);
        m_field = MaterializedField.create(field, t.build());
        try {
          vectors.add(output.addField(m_field, NullableVarCharVector.class));
        } catch (SchemaChangeException e) {
          logger.debug("error while creating result vector "
              + e.getLocalizedMessage());
        }
      }
      this.outputMutator = output;
    }
  }

  // @Override
  // public void allocate(Map<Key, ValueVector> vectorMap)
  // throws OutOfMemoryException {
  // logger.info("SolrRecordReader :: allocate");
  // int prec = 0;
  // try {
  // for (ValueVector vv : vectorMap.values()) {
  // if (vv.getClass().equals(NullableVarCharVector.class)) {
  // NullableVarCharVector v = (NullableVarCharVector) vv;
  // if(prec > 0) {
  // AllocationHelper.allocate(v, 65536, prec);
  // } else {
  // AllocationHelper.allocate(v, 65536, 2000);
  // }
  // }
  // }
  // } catch (NullPointerException e) {
  // throw new OutOfMemoryException();
  // }
  // }

  @Override
  public int next() {
    int counter = 0;
    logger.info("SolrRecordReader :: next");
    try {
      while(counter <= solrDocList.getNumFound() && resultIter.hasNext()){        
        SolrDocument solrDocument =resultIter.next();
        for (ValueVector vv : vectors) {
          String solrField=vv.getField().getPath().toString().replaceAll("`", ""); //re-think ?? 
          Object fieldValue = solrDocument.get(solrField);
          String fieldValueStr = "NULL";
          if (fieldValue != null) {
            fieldValueStr = fieldValue.toString();
          }
          byte[] record = fieldValueStr.getBytes(Charsets.UTF_8);
          if (vv.getClass().equals(NullableVarCharVector.class)) {
            NullableVarCharVector v = (NullableVarCharVector) vv;
            v.getMutator().setSafe(counter, record, 0, record.length);
            v.getMutator().setValueLengthSafe(counter, record.length);
          } else {
            NullableVarCharVector v = (NullableVarCharVector) vv;
            v.getMutator().setSafe(counter, record, 0, record.length);
            v.getMutator().setValueLengthSafe(counter, record.length);
          }
        }
        counter++;
      }

    } catch (Exception e) {

      throw new DrillRuntimeException(e);
    }

    for (ValueVector vv : vectors) {
      vv.getMutator().setValueCount(counter > 0 ? counter : 0);
    }
    return counter > 0 ? counter : 0;
  }

  @Override
  public void cleanup() {
    // TODO Auto-generated method stub

  }

}
