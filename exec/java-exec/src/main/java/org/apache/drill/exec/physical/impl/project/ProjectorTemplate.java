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
package org.apache.drill.exec.physical.impl.project;

import java.io.IOException;
import java.util.List;

import javax.inject.Named;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.SkippingRecordLogger;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import com.google.common.collect.ImmutableList;

public abstract class ProjectorTemplate implements Projector {
  // static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectorTemplate.class);

  private ImmutableList<TransferPair> transfers;
  private SelectionVector2 vector2;
  private SelectionVector4 vector4;
  private SelectionVectorMode svMode;

  // Skip Records
  private SelectionVector2 vector2Out;
  private boolean skipRecord;
  private SkippingRecordLogger skipRecordLogging;

  public ProjectorTemplate() throws SchemaChangeException {
  }

  @Override
  public final int projectRecords(int startIndex, final int recordCount, int firstOutputIndex) {
    if(skipRecord) {
      if (!vector2Out.allocateNewSafe(recordCount)) {
        throw new OutOfMemoryException("Unable to allocate project batch");
      }
    }

    switch (svMode) {
    case FOUR_BYTE:
      throw new UnsupportedOperationException();

    case TWO_BYTE: {
      final int count = recordCount;
      int svIndex = 0;
      for (int i = 0; i < count; ++i, ++firstOutputIndex) {
        if(skipRecord) {
          if(doEvalSkip(i, vector2.getIndex(i), firstOutputIndex, svIndex)) {
            ++svIndex;
          }
        } else {
          doEval(vector2.getIndex(i), firstOutputIndex);
        }
      }
      if(skipRecord) {
        vector2Out.setRecordCount(svIndex);
      }
      return recordCount;
    }

    case NONE: {
      final int countN = recordCount;
      int i;
      int svIndex = 0;
      for (i = startIndex; i < startIndex + countN; i++, firstOutputIndex++) {
        if(skipRecord) {
          if(doEvalSkip(i, i, firstOutputIndex, svIndex)) {
            ++svIndex;
          }
        } else {
          doEval(i, firstOutputIndex);
        }
      }
      if(skipRecord) {
        vector2Out.setRecordCount(svIndex);
      }

      if (i < startIndex + recordCount || startIndex > 0) {
        for (TransferPair t : transfers) {
          t.splitAndTransfer(startIndex, i - startIndex);
        }
        return i - startIndex;
      }
      for (TransferPair t : transfers) {
          t.transfer();
      }
      return recordCount;
    }

    default:
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public final void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, List<TransferPair> transfers, SkippingRecordLogger skipRecordLogging)  throws SchemaChangeException{
    this.svMode = incoming.getSchema().getSelectionVectorMode();
    switch (svMode) {
    case FOUR_BYTE:
      this.vector4 = incoming.getSelectionVector4();
      break;
    case TWO_BYTE:
      this.vector2 = incoming.getSelectionVector2();
      break;
    }
    this.transfers = ImmutableList.copyOf(transfers);
    doSetup(context, incoming, outgoing);

    this.skipRecord = (skipRecordLogging != null);
    this.skipRecordLogging = skipRecordLogging;

    if(this.skipRecord) {
      vector2Out = outgoing.getSelectionVector2();
    }
  }

  private boolean doEvalSkip(int inputline, int inIndex, int outIndex, int svIndex) {
    final List<Integer> errorCodeList = (List<Integer>) doEval(inIndex, outIndex);

    boolean noError = true;
    for(int i = 0; i < errorCodeList.size(); ++i) {
      int errorCode = errorCodeList.get(i);
      if(errorCode != 0) {
        skipRecordLogging.append("Error_Type", "Parsing Error");
        skipRecordLogging.append("Expression", skipRecordLogging.getExprList().get(i).getRight());
        skipRecordLogging.appendContext(inputline);

        skipRecordLogging.incrementNumSkippedRecord();
        try {
          skipRecordLogging.write();
        } catch (IOException ioe) {
          throw new RuntimeException();
        }
        errorCodeList.set(i, 0);
        noError = false;
      }
    }

    if(noError) {
      vector2Out.setIndex(svIndex, (char) outIndex);
    }
    return noError;
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract List doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
}