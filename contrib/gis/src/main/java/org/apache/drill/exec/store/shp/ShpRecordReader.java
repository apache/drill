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
package org.apache.drill.exec.store.shp;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.jamel.dbf.DbfReader;
import org.jamel.dbf.structure.DbfField;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.ShapefileReader;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;

import io.netty.buffer.DrillBuf;

/**
 * A RecordReader implementation for Shapefile data files.
 *
 * @see RecordReader
 */
public class ShpRecordReader extends AbstractRecordReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShpRecordReader.class);

  private final Path hadoopShp;
  private final Path hadoopDbf;
  private final Path hadoopPrj;
  private final long start;
  private final long end;
  private DrillBuf buffer;
  private VectorContainerWriter writer;

  private FSDataInputStream fileReaderShp = null;
  private FSDataInputStream fileReaderDbf = null;
  private FSDataInputStream fileReaderPrj = null;

  private GeometryCursor geomCursor = null;
  private DbfReader dbfReader = null;
  private FileSystem fs;

  private final String opUserName;
  private final String queryUserName;

  private int srid;
  private SpatialReference spatialReference;

  private static final int DEFAULT_BATCH_SIZE = 1000;

  public ShpRecordReader(final FragmentContext fragmentContext, final String inputPath, final long start,
      final long length, final FileSystem fileSystem, final List<SchemaPath> projectedColumns, final String userName) {
    this(fragmentContext, inputPath, start, length, fileSystem, projectedColumns, userName, DEFAULT_BATCH_SIZE);
  }

  public ShpRecordReader(final FragmentContext fragmentContext, final String inputPath, final long start,
      final long length, final FileSystem fileSystem, List<SchemaPath> projectedColumns, final String userName,
      final int defaultBatchSize) {

    hadoopShp = new Path(inputPath);
    hadoopDbf = new Path(inputPath.replace("shp", "dbf"));
    hadoopPrj = new Path(inputPath.replace("shp", "prj"));
    this.start = start;
    this.end = start + length;
    buffer = fragmentContext.getManagedBuffer();
    this.fs = fileSystem;
    this.opUserName = userName;
    this.queryUserName = fragmentContext.getQueryUserName();
    setColumns(projectedColumns);
  }

  private FSDataInputStream getReader(final Path hadoop, final FileSystem fs) throws ExecutionSetupException {
    try {
      final UserGroupInformation ugi = ImpersonationUtil.createProxyUgi(this.opUserName, this.queryUserName);
      return ugi.doAs(new PrivilegedExceptionAction<FSDataInputStream>() {
        @Override
        public FSDataInputStream run() throws Exception {
          return fs.open(hadoop);
        }
      });
    } catch (IOException | InterruptedException e) {
      throw new ExecutionSetupException(String.format("Error in creating shp reader for file: %s", hadoop), e);
    }
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    writer = new VectorContainerWriter(output);

    try {
      fileReaderShp = getReader(hadoopShp, fs);
      byte[] shpBuf = new byte[fileReaderShp.available()];
      fileReaderShp.readFully(shpBuf);

      ByteBuffer byteBuffer = ByteBuffer.wrap(shpBuf);
      byteBuffer.position(byteBuffer.position() + 100);

      ShapefileReader shpReader = new ShapefileReader();
      geomCursor = shpReader.getGeometryCursor(byteBuffer);

      fileReaderDbf = getReader(hadoopDbf, fs);
      dbfReader = new DbfReader(fileReaderDbf);

      fileReaderPrj = getReader(hadoopPrj, fs);
      byte[] prjBuf = new byte[fileReaderPrj.available()];
      fileReaderPrj.readFully(prjBuf);
      fileReaderPrj.close();

      String wktReference = new String(prjBuf);

      String sridPatternText = "AUTHORITY\\[\"\\w+\"\\s*,\\s*\"*(\\d+)\"*\\]\\]$";
      Pattern pattern = Pattern.compile(sridPatternText);
      Matcher matcher = pattern.matcher(wktReference);
      if (matcher.find()) {
        srid = Integer.parseInt(matcher.group(1));
        spatialReference = SpatialReference.create(srid);
      }

      logger.debug("Processing file : {}, start position : {}, end position : {} ", hadoopShp, start, end);
    } catch (IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    final Stopwatch watch = new Stopwatch().start();

    if (fileReaderShp == null) {
      throw new IllegalStateException("Shapefile reader is not open.");
    }
    Geometry geom = geomCursor.next();
    if (geom == null) {
      return 0;
    }

    int recordCount = 0;
    writer.allocate();
    writer.reset();

    try {
      while (geom != null) {
        writer.setPosition(recordCount);
        Object[] dbfRow = dbfReader.nextRecord();
        processShapefileSet(geomCursor.getGeometryID(), geom, dbfRow);
        geom = geomCursor.next();
        recordCount++;
      }

      writer.setValueCount(recordCount);
    } catch (Exception e) {
      throw new DrillRuntimeException(e);
    }

    logger.debug("Read {} records in {} ms", recordCount, watch.elapsed(TimeUnit.MILLISECONDS));
    return recordCount;
  }

  private void processShapefileSet(final int gid, final Geometry geom, final Object[] dbfRow) {
    MapWriter mapWriter = writer.rootAsMap();
    mapWriter.start();

    mapWriter.integer("gid").writeInt(gid);
    mapWriter.integer("srid").writeInt(srid);

    byte[] binary = null;
    binary = geom.getType().toString().getBytes(Charsets.UTF_8);
    final int length = binary.length;
    final VarCharHolder vh = new VarCharHolder();
    ensureBuffer(length);
    buffer.setBytes(0, binary);
    vh.buffer = buffer;
    vh.start = 0;
    vh.end = length;
    mapWriter.varChar("shapeType").write(vh);

    final VarBinaryHolder vb = new VarBinaryHolder();
    final ByteBuffer buf = (ByteBuffer) OGCGeometry.createFromEsriGeometry(geom, spatialReference).asBinary();
    final byte[] bytes = buf.array();
    ensureBuffer(bytes.length);
    buffer.setBytes(0, bytes);
    vb.buffer = buffer;
    vb.start = 0;
    vb.end = bytes.length;
    mapWriter.varBinary("geom").write(vb);

    writeDbfRow(dbfRow, mapWriter);

    mapWriter.end();
  }

  private void writeDbfRow(final Object[] dbfRow, MapWriter mapWriter) {
    int dbfFieldCount = dbfReader.getHeader().getFieldsCount();

    for (int i = 0; i < dbfFieldCount; i++) {
      DbfField field = dbfReader.getHeader().getField(i);

      if (dbfRow[i] == null) {
        continue;
      }

      switch (field.getDataType()) {
      case CHAR:
        byte[] strType = (byte[]) dbfRow[i];
        final int strLen = trimmedLen(strType); // trim empty chars from dbf field

        final VarCharHolder strHolder = new VarCharHolder();
        ensureBuffer(strLen);
        buffer.setBytes(0, strType, 0, strLen);
        strHolder.buffer = buffer;
        strHolder.start = 0;
        strHolder.end = strLen;
        mapWriter.varChar(field.getName()).write(strHolder);
        break;
      case FLOAT:
        final double floatVal = ((Float) dbfRow[i]).doubleValue();
        mapWriter.float8(field.getName()).writeFloat8(floatVal);
        break;
      case DATE:
        final long dataVal = ((java.util.Date) dbfRow[i]).getTime();
        mapWriter.date(field.getName()).writeDate(dataVal);
        break;
      case LOGICAL:
        int logicalVal = (Boolean) dbfRow[i] ? 1 : 0;
        mapWriter.bit(field.getName()).writeBit(logicalVal);
        break;
      case NUMERIC:
        double numericVal = ((Number) dbfRow[i]).doubleValue();
        if (field.getDecimalCount() == 0) {
          mapWriter.integer(field.getName()).writeInt((int) numericVal);
        } else {
          mapWriter.float8(field.getName()).writeFloat8(numericVal);
        }
        break;
      default:
        break;
      }
    }
  }

  private int trimmedLen(byte[] charArray) {
    int trimTrail = charArray.length;
    while (trimTrail-- > 0 && charArray[trimTrail] == 32) {
    }
    return trimTrail + 1;
  }

  private void ensureBuffer(final int length) {
    buffer = buffer.reallocIfNeeded(length);
  }

  @Override
  public void close() {
    closeReader(fileReaderShp);
    closeReader(fileReaderDbf);
    closeReader(fileReaderPrj);
    closeReader(dbfReader);

    fileReaderShp = null;
    fileReaderDbf = null;
    fileReaderPrj = null;
    dbfReader = null;
  }

  private void closeReader(Closeable r) {
    if (r != null) {
      try {
        r.close();
      } catch (IOException e) {
        logger.warn("Error closing shapefile reader", e);
      }
    }
  }
}
