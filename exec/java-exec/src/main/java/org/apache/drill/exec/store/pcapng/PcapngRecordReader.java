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
package org.apache.drill.exec.store.pcapng;

import fr.bmartel.pcapdecoder.PcapDecoder;
import fr.bmartel.pcapdecoder.structure.types.IPcapngType;
import fr.bmartel.pcapdecoder.structure.types.inter.IEnhancedPacketBLock;
import org.apache.commons.io.IOUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.pcapng.schema.Column;
import org.apache.drill.exec.store.pcapng.schema.DummyArrayImpl;
import org.apache.drill.exec.store.pcapng.schema.DummyImpl;
import org.apache.drill.exec.store.pcapng.schema.Schema;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

public class PcapngRecordReader extends AbstractRecordReader {
  private static final Logger logger = LoggerFactory.getLogger(PcapngRecordReader.class);

  // batch size should not exceed max allowed record count
  private static final int BATCH_SIZE = 40_000;

  private final Path pathToFile;
  private OutputMutator output;
  private List<ProjectedColumnInfo> projectedCols;
  private FileSystem fs;
  private FSDataInputStream in;
  private List<SchemaPath> columns;

  private Iterator<IPcapngType> it;

  public PcapngRecordReader(final String pathToFile,
                            final FileSystem fileSystem,
                            final List<SchemaPath> columns) {
    this.fs = fileSystem;
    this.pathToFile = fs.makeQualified(new Path(pathToFile));
    this.columns = columns;
    setColumns(columns);
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    try {

      this.output = output;
      this.in = fs.open(pathToFile);
      PcapDecoder decoder = new PcapDecoder(IOUtils.toByteArray(in));
      decoder.decode();
      this.it = decoder.getSectionList().iterator();
      setupProjection();
    } catch (IOException io) {
      throw UserException.dataReadError(io)
          .addContext("File name:", pathToFile.toUri().getPath())
          .build(logger);
    }
  }

  @Override
  public int next() {
    if (isSkipQuery()) {
      return iterateOverBlocks((block, counter) -> {
      });
    } else {
      return iterateOverBlocks((block, counter) -> putToTable((IEnhancedPacketBLock) block, counter));
    }
  }

  private void putToTable(IEnhancedPacketBLock bLock, Integer counter) {
    for (ProjectedColumnInfo pci : projectedCols) {
      pci.getColumn().process(bLock, pci.getVv(), counter);
    }
  }

  @Override
  public void close() throws Exception {
    if (in != null) {
      in.close();
      in = null;
    }
  }

  private void setupProjection() {
    if (isSkipQuery()) {
      projectedCols = projectNone();
    } else if (isStarQuery()) {
      projectedCols = projectAllCols(Schema.getColumnsNames());
    } else {
      projectedCols = projectCols(columns);
    }
  }

  private List<ProjectedColumnInfo> projectNone() {
    List<ProjectedColumnInfo> pciBuilder = new ArrayList<>();
    pciBuilder.add(makeColumn("dummy", new DummyImpl()));
    return Collections.unmodifiableList(pciBuilder);
  }

  private List<ProjectedColumnInfo> projectAllCols(final Set<String> columns) {
    List<ProjectedColumnInfo> pciBuilder = new ArrayList<>();
    for (String colName : columns) {
      pciBuilder.add(makeColumn(colName, Schema.getColumns().get(colName)));
    }
    return Collections.unmodifiableList(pciBuilder);
  }

  private List<ProjectedColumnInfo> projectCols(final List<SchemaPath> columns) {
    List<ProjectedColumnInfo> pciBuilder = new ArrayList<>();
    for (SchemaPath schemaPath : columns) {
      String projectedName = schemaPath.rootName();
      if (schemaPath.isArray()) {
        pciBuilder.add(makeColumn(projectedName, new DummyArrayImpl()));
      } else if (Schema.getColumns().containsKey(projectedName.toLowerCase())) {
        pciBuilder.add(makeColumn(projectedName,
            Schema.getColumns().get(projectedName.toLowerCase())));
      } else {
        pciBuilder.add(makeColumn(projectedName, new DummyImpl()));
      }
    }
    return Collections.unmodifiableList(pciBuilder);
  }

  private ProjectedColumnInfo makeColumn(final String colName, final Column column) {
    MaterializedField field = MaterializedField.create(colName, column.getMinorType());
    ValueVector vector = getValueVector(field, output);
    return new ProjectedColumnInfo(vector, column, colName);
  }

  private ValueVector getValueVector(final MaterializedField field, final OutputMutator output) {
    try {
      TypeProtos.MajorType majorType = field.getType();
      final Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(
          majorType.getMinorType(), majorType.getMode());

      return output.addField(field, clazz);
    } catch (SchemaChangeException sce) {
      throw UserException.internalError(sce)
          .addContext("The addition of this field is incompatible with this OutputMutator's capabilities")
          .build(logger);
    }
  }

  private Integer iterateOverBlocks(BiConsumer<IPcapngType, Integer> consumer) {
    int counter = 0;
    while (it.hasNext() && counter < BATCH_SIZE) {
      IPcapngType block = it.next();
      if (block instanceof IEnhancedPacketBLock) {
        consumer.accept(block, counter);
        counter++;
      }
    }
    return counter;
  }

  private static class ProjectedColumnInfo {

    private ValueVector vv;
    private Column colDef;
    private String columnName;

    ProjectedColumnInfo(ValueVector vv, Column colDef, String columnName) {
      this.vv = vv;
      this.colDef = colDef;
      this.columnName = columnName;
    }

    public ValueVector getVv() {
      return vv;
    }

    Column getColumn() {
      return colDef;
    }

    public String getColumnName() {
      return columnName;
    }
  }
}
