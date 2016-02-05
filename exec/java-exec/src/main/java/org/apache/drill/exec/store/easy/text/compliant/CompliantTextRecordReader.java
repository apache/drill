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
package org.apache.drill.exec.store.easy.text.compliant;

import com.google.common.collect.Maps;
import com.univocity.parsers.common.TextParsingException;
import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.mapred.FileSplit;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.drill.exec.expr.TypeHelper;

// New text reader, complies with the RFC 4180 standard for text/csv files
public class CompliantTextRecordReader extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompliantTextRecordReader.class);

  private static final int MAX_RECORDS_PER_BATCH = 8096;
  static final int READ_BUFFER = 1024*1024;
  private static final int WHITE_SPACE_BUFFER = 64*1024;

  // settings to be used while parsing
  private TextParsingSettings settings;
  // Chunk of the file to be read by this reader
  private FileSplit split;
  // text reader implementation
  private TextReader reader;
  // input buffer
  private DrillBuf readBuffer;
  // working buffer to handle whitespaces
  private DrillBuf whitespaceBuffer;
  private DrillFileSystem dfs;
  // operator context for OutputMutator
  private OperatorContext oContext;

  public CompliantTextRecordReader(FileSplit split, DrillFileSystem dfs, FragmentContext context, TextParsingSettings settings, List<SchemaPath> columns) {
    this.split = split;
    this.settings = settings;
    this.dfs = dfs;
    setColumns(columns);
  }

  // checks to see if we are querying all columns(star) or individual columns
  @Override
  public boolean isStarQuery() {
    if(settings.isUseRepeatedVarChar()) {
      return super.isStarQuery() || Iterables.tryFind(getColumns(), new Predicate<SchemaPath>() {
        @Override
        public boolean apply(@Nullable SchemaPath path) {
          return path.equals(RepeatedVarCharOutput.COLUMNS);
        }
      }).isPresent();
    }
    return super.isStarQuery();
  }

  @Override
  protected List<SchemaPath> getDefaultColumnsToRead() {
    return DEFAULT_TEXT_COLS_TO_READ;
  }

  /**
   * Performs the initial setup required for the record reader.
   * Initializes the input stream, handling of the output record batch
   * and the actual reader to be used.
   * @param context  operator context from which buffer's will be allocated and managed
   * @param outputMutator  Used to create the schema in the output record batch
   * @throws ExecutionSetupException
   */
  @Override
  public void setup(OperatorContext context, OutputMutator outputMutator) throws ExecutionSetupException {

    oContext = context;
    readBuffer = context.getManagedBuffer(READ_BUFFER);
    whitespaceBuffer = context.getManagedBuffer(WHITE_SPACE_BUFFER);

    // setup Output, Input, and Reader
    try {
      TextOutput output = null;
      TextInput input = null;
      InputStream stream = null;

      // setup Output using OutputMutator
      if (settings.isHeaderExtractionEnabled()){
        //extract header and use that to setup a set of VarCharVectors
        String [] fieldNames = extractHeader();
        output = new FieldVarCharOutput(outputMutator, fieldNames, getColumns(), isStarQuery());
      } else {
        //simply use RepeatedVarCharVector
        output = new RepeatedVarCharOutput(outputMutator, getColumns(), isStarQuery());
      }

      // setup Input using InputStream
      stream = dfs.openPossiblyCompressedStream(split.getPath());
      input = new TextInput(settings,  stream, readBuffer, split.getStart(), split.getStart() + split.getLength());

      // setup Reader using Input and Output
      reader = new TextReader(settings, input, output, whitespaceBuffer);
      reader.start();

    } catch (SchemaChangeException | IOException e) {
      throw new ExecutionSetupException(String.format("Failure while setting up text reader for file %s", split.getPath()), e);
    } catch (IllegalArgumentException e) {
      throw UserException.dataReadError(e).addContext("File Path", split.getPath().toString()).build(logger);
    }
  }

  /**
   * This method is responsible to implement logic for extracting header from text file
   * Currently it is assumed to be first line if headerExtractionEnabled is set to true
   * TODO: enhance to support more common header patterns
   * @return field name strings
   */
  private String [] extractHeader() throws SchemaChangeException, IOException, ExecutionSetupException{
    assert (settings.isHeaderExtractionEnabled());
    assert (oContext != null);

    // don't skip header in case skipFirstLine is set true
    settings.setSkipFirstLine(false);

    // setup Output using OutputMutator
    // we should use a separate output mutator to avoid reshaping query output with header data
    HeaderOutputMutator hOutputMutator = new HeaderOutputMutator();
    TextOutput hOutput = new RepeatedVarCharOutput(hOutputMutator, getColumns(), true);
    this.allocate(hOutputMutator.fieldVectorMap);

    // setup Input using InputStream
    // we should read file header irrespective of split given given to this reader
    InputStream hStream = dfs.openPossiblyCompressedStream(split.getPath());
    TextInput hInput = new TextInput(settings,  hStream, oContext.getManagedBuffer(READ_BUFFER), 0, split.getLength());

    // setup Reader using Input and Output
    this.reader = new TextReader(settings, hInput, hOutput, oContext.getManagedBuffer(WHITE_SPACE_BUFFER));
    reader.start();

    // extract first row only
    reader.parseNext();

    // grab the field names from output
    String [] fieldNames = ((RepeatedVarCharOutput)hOutput).getTextOutput();

    // cleanup and set to skip the first line next time we read input
    reader.close();
    hOutputMutator.close();
    settings.setSkipFirstLine(true);

    return fieldNames;
  }

  /**
   * Generates the next record batch
   * @return  number of records in the batch
   *
   */
  @Override
  public int next() {
    reader.resetForNextBatch();
    int cnt = 0;

    try{
      while(cnt < MAX_RECORDS_PER_BATCH && reader.parseNext()){
        cnt++;
      }
      reader.finishBatch();
      return cnt;
    } catch (IOException | TextParsingException e) {
      throw UserException.dataReadError(e)
          .addContext("Failure while reading file %s. Happened at or shortly before byte position %d.",
            split.getPath(), reader.getPos())
          .build(logger);
    }
  }

  /**
   * Cleanup state once we are finished processing all the records.
   * This would internally close the input stream we are reading from.
   */
  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    } catch (IOException e) {
      logger.warn("Exception while closing stream.", e);
    }
  }

  /**
   * TextRecordReader during its first phase read to extract header should pass its own
   * OutputMutator to avoid reshaping query output.
   * This class provides OutputMutator for header extraction.
   */
  private class HeaderOutputMutator implements OutputMutator {
    private final Map<String, ValueVector> fieldVectorMap = Maps.newHashMap();

    @Override
    public <T extends ValueVector> T addField(MaterializedField field, Class<T> clazz) throws SchemaChangeException {
      ValueVector v = fieldVectorMap.get(field);
      if (v == null || v.getClass() != clazz) {
        // Field does not exist add it to the map
        v = TypeHelper.getNewVector(field, oContext.getAllocator());
        if (!clazz.isAssignableFrom(v.getClass())) {
          throw new SchemaChangeException(String.format(
              "Class %s was provided, expected %s.", clazz.getSimpleName(), v.getClass().getSimpleName()));
        }
        fieldVectorMap.put(field.getPath(), v);
      }
      return clazz.cast(v);
    }

    @Override
    public void allocate(int recordCount) {
      //do nothing for now
    }

    @Override
    public boolean isNewSchema() {
      return false;
    }

    @Override
    public DrillBuf getManagedBuffer() {
      return null;
    }

    @Override
    public CallBack getCallBack() {
      return null;
    }

    /**
     * Since this OutputMutator is passed by TextRecordReader to get the header out
     * the mutator might not get cleaned up elsewhere. TextRecordReader will call
     * this method to clear any allocations
     */
    public void close() {
      for (final ValueVector v : fieldVectorMap.values()) {
        v.clear();
      }
      fieldVectorMap.clear();
    }

  }

}
