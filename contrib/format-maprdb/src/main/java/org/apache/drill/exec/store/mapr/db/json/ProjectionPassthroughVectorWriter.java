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
package org.apache.drill.exec.store.mapr.db.json;

import static org.apache.drill.exec.store.mapr.PluginErrorHandler.dataReadError;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.vector.complex.impl.StructOrListWriterImpl;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.StructOrListWriter;
import org.ojai.DocumentConstants;
import org.ojai.DocumentReader.EventType;
import org.ojai.util.DocumentReaderWithProjection;
import org.ojai.util.FieldProjector;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import com.mapr.db.DBConstants;
import com.mapr.db.ojai.DBDocumentReaderBase;

/**
 *  This implementation of DocumentReaderVectorWriter writes the encoded MapR-DB OJAI Document
 *  as binary data along with the fields required to execute Drill's operators.
 */
class ProjectionPassthroughVectorWriter extends DocumentReaderVectorWriter {

  private final boolean includeId;
  private final FieldProjector projector;

  protected ProjectionPassthroughVectorWriter(final OjaiValueWriter valueWriter,
      final FieldProjector projector, final boolean includeId) {
    super(valueWriter);
    this.includeId = includeId;
    this.projector = Preconditions.checkNotNull(projector);
  }

  @Override
  protected void writeDBDocument(VectorContainerWriter vectorWriter, DBDocumentReaderBase reader)
      throws SchemaChangeException {
    if (reader.next() != EventType.START_MAP) {
      throw dataReadError(logger, "The document did not start with START_MAP!");
    }

    StructOrListWriterImpl writer = new StructOrListWriterImpl(vectorWriter.rootAsStruct());
    writer.start();
    StructOrListWriter documentStructWriter = writer.struct(DBConstants.DOCUMENT_FIELD);
    documentStructWriter.start();

    // write _id field data
    if (includeId) {
      valueWriter.writeBinary(documentStructWriter, DocumentConstants.ID_KEY, reader.getIdData());
    }

    // write rest of the data buffers
    Map<Integer, ByteBuffer> dataMap = reader.getDataMap();
    for (Entry<Integer, ByteBuffer> familyData : dataMap.entrySet()) {
      valueWriter.writeBinary(documentStructWriter, String.valueOf(familyData.getKey()), familyData.getValue());
    }
    documentStructWriter.end();

    DocumentReaderWithProjection p = new DocumentReaderWithProjection(reader, projector);
    valueWriter.writeToListOrStruct(writer, p);
  }

}
