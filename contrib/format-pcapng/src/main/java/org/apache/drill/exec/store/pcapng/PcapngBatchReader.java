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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileDescrip;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.pcap.TcpSessionizer;
import org.apache.drill.exec.store.pcap.plugin.PcapFormatConfig;
import org.apache.drill.exec.store.pcap.protocol.ProtocolColumns;
import org.apache.drill.exec.store.pcap.protocol.ProtocolDecoders;
import org.apache.drill.exec.store.pcap.schema.Schema;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PcapngBatchReader implements ManagedReader {
  private static final Logger logger = LoggerFactory.getLogger(PcapngBatchReader.class);
  private static final int SECTION_HEADER_TYPE = 0x0A0D0D0A;
  private static final int INTERFACE_DESCRIPTION_TYPE = 1;
  private static final int NAME_RESOLUTION_TYPE = 4;
  private static final int INTERFACE_STATISTICS_TYPE = 5;
  private static final int ENHANCED_PACKET_TYPE = 6;
  private static final int BYTE_ORDER_MAGIC = 0x1A2B3C4D;
  // libpcap's limit; a longer block is treated as damage rather than allocated
  private static final int MAX_BLOCK_LENGTH = 16 * 1024 * 1024;
  // Option codes
  private static final int OPT_ENDOFOPT = 0;
  private static final int OPT_COMMENT = 1;
  private static final int IF_NAME = 2;
  private static final int IF_TSRESOL = 9;
  private static final int IF_TSOFFSET = 14;
  private static final int EPB_FLAGS = 2;
  private static final int EPB_HASH = 3;
  private static final int EPB_DROPCOUNT = 4;
  private static final String[] HASH_ALGORITHMS = {"2s-complement", "xor", "crc32", "md5", "sha1", "toeplitz"};

  private final PcapFormatConfig config;
  private final EasySubScan scan;
  private final FileDescrip file;

  private CustomErrorContext errorContext;
  private List<SchemaPath> columns;
  private List<ColumnDefn> projectedColumns;
  private final byte[] header = new byte[12];
  private ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
  // Interfaces of the current section, indexed by interface ID
  private final List<Interface> interfaces = new ArrayList<>();
  private RowSetLoader loader;
  // Set when TCP packets are grouped into sessions instead of returned as rows
  private TcpSessionizer sessionizer;
  // A packet held back because its block already wrote an error row this iteration
  private PacketDecoder pendingPacket;
  private ProtocolColumns protocolColumns;
  private InputStream in;
  private Path path;
  // Bytes of the file consumed so far, for error messages
  private long position;
  // Set when damage makes the rest of the file unreadable
  private boolean finished;

  public PcapngBatchReader(final PcapFormatConfig config, final EasySubScan scan,
    FileSchemaNegotiator negotiator) {
    this.config = config;
    this.scan = scan;
    this.columns = scan.getColumns();
    this.file = negotiator.file();
    try {
      // init InputStream for pcap file
      errorContext = negotiator.parentErrorContext();
      DrillFileSystem dfs = file.fileSystem();
      path = dfs.makeQualified(file.split().getPath());
      // Blocks are decoded one at a time as next() pulls them, so memory use
      // does not grow with file size.
      in = new BufferedInputStream(dfs.openPossiblyCompressedStream(path));
      logger.debug("The config is {}, root is {}, columns has {}", config, scan.getSelectionRoot(), columns);
    } catch (IOException e) {
      throw UserException
             .dataReadError(e)
             .message("Failure in initial pcapng inputstream. " + e.getMessage())
             .addContext(errorContext)
             .build(logger);
    }
    if (isSessionQuery()) {
      SchemaBuilder builder = new SchemaBuilder();
      new Schema(true).addColumns(builder);
      ProtocolColumns.addColumns(builder, ProtocolColumns.Mode.SESSION, ProtocolDecoders.get());
      negotiator.tableSchema(builder.buildSchema(), false);
      loader = negotiator.build().writer();
      protocolColumns = new ProtocolColumns(loader, ProtocolColumns.Mode.SESSION, ProtocolDecoders.get(),
          config.getExposeCredentials());
      sessionizer = new TcpSessionizer(loader, protocolColumns);
      return;
    }
    // define the schema
    negotiator.tableSchema(defineMetadata(), true);
    ResultSetLoader resultSetLoader = negotiator.build();
    loader = resultSetLoader.writer();
    // bind the writer for columns
    bindColumns(loader);
    protocolColumns = new ProtocolColumns(loader, rowMode(), ProtocolDecoders.get(), config.getExposeCredentials());
  }

  private ProtocolColumns.Mode rowMode() {
    return config.getStat() ? ProtocolColumns.Mode.STAT : ProtocolColumns.Mode.PACKET;
  }

  /**
   * The default of the `stat` parameter is false,
   * which means that the packet data is parsed and returned,
   * but if true, will return the statistics data about the each pcapng file only
   * (consist of the information about collect devices and the summary of the packet data above).
   *
   * In addition, a pcapng file contains a single Section Header Block (SHB),
   * a single Interface Description Block (IDB) and a few Enhanced Packet Blocks (EPB).
   * <pre>
   * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   * | SHB | IDB | EPB | EPB |    ...    | EPB |
   * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   * </pre>
   * https://pcapng.github.io/pcapng/draft-tuexen-opsawg-pcapng.html#name-physical-file-layout
   */
  @Override
  public boolean next() {
    while (!loader.isFull()) {
      if (pendingPacket != null) {
        PacketDecoder packet = pendingPacket;
        pendingPacket = null;
        sessionizer.addPacket(packet);
        continue;
      }
      PcapngBlock block;
      try {
        block = nextRow();
      } catch (IOException e) {
        // The stream cannot be read further: report it, then finish like end of file
        finished = true;
        protocolColumns.writeErrorRow("file: read failed at byte " + position + ": " + ProtocolDecoders.describe(e));
        continue;
      }
      if (block == null) {
        // At end of file, more batches are needed only for open sessions that did not fit
        return sessionizer != null && !sessionizer.writeOpenSessions();
      }
      if (sessionizer != null) {
        // Packets are not rows here, so their problems become error rows, once each.
        // A block writes at most one row per iteration so a full batch is never overrun.
        boolean wroteRow = block.errors != null
            && protocolColumns.writeErrorRowOnce(String.join("; ", block.errors));
        if (block.packet != null) {
          if (wroteRow) {
            pendingPacket = block.packet;
          } else {
            sessionizer.addPacket(block.packet);
          }
        }
      } else {
        processBlock(block);
      }
    }
    return true;
  }

  /**
   * Reads blocks until one produces a row: an Enhanced Packet Block for
   * packet queries, a descriptive block for stat queries. Section and
   * interface blocks are always tracked, since packets depend on them.
   *
   * @return the next row, or null at end of file
   */
  private PcapngBlock nextRow() throws IOException {
    while (true) {
      if (finished) {
        return null;
      }
      long blockStart = position;
      int n = IOUtils.read(in, header, 0, 8);
      if (n == 0) {
        return null;
      }
      if (n < 8) {
        finished = true;
        return PcapngBlock.errorRow("file: truncated block header at byte " + blockStart);
      }
      ByteBuffer buf = ByteBuffer.wrap(header).order(byteOrder);
      int type = buf.getInt(0);
      int consumed = 8;
      if (type == SECTION_HEADER_TYPE) {
        // Each section declares its own byte order; the type itself is a palindrome
        if (IOUtils.read(in, header, 8, 4) < 4) {
          finished = true;
          return PcapngBlock.errorRow("file: truncated block header at byte " + blockStart);
        }
        consumed = 12;
        byteOrder = ByteBuffer.wrap(header, 8, 4).order(ByteOrder.BIG_ENDIAN).getInt() == BYTE_ORDER_MAGIC
            ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        buf.order(byteOrder);
      }
      int totalLength = buf.getInt(4);
      if (totalLength < consumed + 4 || totalLength % 4 != 0 || totalLength > MAX_BLOCK_LENGTH) {
        finished = true;
        return PcapngBlock.errorRow("file: invalid block length " + totalLength + " at byte " + blockStart);
      }
      // Body excludes the header and the trailing copy of the block length
      byte[] body = new byte[totalLength - consumed - 4];
      try {
        IOUtils.readFully(in, body);
        IOUtils.skipFully(in, 4);
      } catch (EOFException e) {
        finished = true;
        return PcapngBlock.errorRow("file: truncated block at byte " + blockStart);
      }
      position += totalLength;
      ByteBuffer block = ByteBuffer.wrap(body).order(byteOrder);

      if (type == SECTION_HEADER_TYPE) {
        // Interface IDs are scoped to their section
        interfaces.clear();
      } else if (type == INTERFACE_DESCRIPTION_TYPE) {
        interfaces.add(new Interface(block, interfaces.size()));
      } else if (type == ENHANCED_PACKET_TYPE && !config.getStat()) {
        return readPacket(block, blockStart);
      }

      if (config.getStat() && (type == SECTION_HEADER_TYPE || type == INTERFACE_DESCRIPTION_TYPE
          || type == NAME_RESOLUTION_TYPE || type == INTERFACE_STATISTICS_TYPE)) {
        PcapngBlock row = new PcapngBlock();
        row.comment = readComment(block, optionsOffset(type, block));
        row.stats = readStats(type, block);
        if (type == INTERFACE_DESCRIPTION_TYPE && interfaces.get(interfaces.size() - 1).error != null) {
          row.addError(interfaces.get(interfaces.size() - 1).error);
        }
        return row;
      }
    }
  }

  private PcapngBlock readPacket(ByteBuffer block, long blockStart) {
    PcapngBlock row = new PcapngBlock();
    row.interfaceId = block.getInt(0);
    Interface iface;
    if (row.interfaceId < 0 || row.interfaceId >= interfaces.size()) {
      // Unknown link type and resolution: keep the row, decode nothing
      iface = Interface.UNDEFINED;
      row.addError("file: packet references undefined interface " + row.interfaceId);
    } else {
      iface = interfaces.get(row.interfaceId);
      if (iface.error != null) {
        row.addError(iface.error);
      }
    }
    row.interfaceName = iface.name;
    row.linkType = iface.linkType;
    // 64-bit timestamp stored as high then low 32-bit words
    row.timestamp = iface.toInstant((long) block.getInt(4) << 32 | (block.getInt(8) & 0xFFFFFFFFL));
    row.capturedLength = block.getInt(12);
    row.originalLength = block.getInt(16);
    if (row.capturedLength < 0 || 20 + row.capturedLength > block.capacity()) {
      return PcapngBlock.errorRow("file: block at byte " + blockStart + " has invalid captured length "
          + row.capturedLength);
    }
    row.data = Arrays.copyOfRange(block.array(), 20, 20 + row.capturedLength);

    StringBuilder comment = new StringBuilder();
    forEachOption(block, 20 + pad4(row.capturedLength), (code, start, length) -> {
      switch (code) {
        case OPT_COMMENT:
          appendComment(comment, block, start, length);
          break;
        case EPB_FLAGS:
          row.flags = block.getInt(start);
          break;
        case EPB_HASH:
          row.hash = formatHash(block, start, length);
          break;
        case EPB_DROPCOUNT:
          row.dropCount = block.getLong(start);
          break;
        default:
          break;
      }
    });
    row.comment = comment.length() == 0 ? null : comment.toString();

    // Decode once here instead of once per projected column
    if (!isSkipQuery() || sessionizer != null) {
      PacketDecoder packet = new PacketDecoder();
      boolean decoded = packet.readPcapng(row.data, row.linkType);
      if (packet.getDecodeError() != null) {
        row.addError("packet: " + packet.getDecodeError());
      }
      if (decoded) {
        packet.setTimestamp(row.timestamp);
        row.packet = packet;
      }
    }
    return row;
  }

  /**
   * Offset of the options in a descriptive block body (which, for a section
   * header, starts after the byte-order magic).
   */
  private static int optionsOffset(int type, ByteBuffer block) {
    switch (type) {
      case SECTION_HEADER_TYPE:
        return 12;
      case INTERFACE_DESCRIPTION_TYPE:
        return 8;
      case INTERFACE_STATISTICS_TYPE:
        return 12;
      case NAME_RESOLUTION_TYPE:
        // Options follow the name records, which end with a zero record type
        int offset = 0;
        while (offset + 4 <= block.capacity()) {
          int recordType = block.getShort(offset) & 0xFFFF;
          int length = block.getShort(offset + 2) & 0xFFFF;
          offset += 4 + pad4(length);
          if (recordType == 0) {
            break;
          }
        }
        return offset;
      default:
        return block.capacity();
    }
  }

  /**
   * Decodes the options of a descriptive block into stat column values.
   * Repeated options, such as several interface addresses, are comma separated.
   */
  private Map<String, Object> readStats(int type, ByteBuffer block) {
    Map<String, Object> stats = new HashMap<>();
    // Statistics timestamps use the resolution of the interface they describe
    Interface iface = type == INTERFACE_STATISTICS_TYPE && block.getInt(0) >= 0 && block.getInt(0) < interfaces.size()
        ? interfaces.get(block.getInt(0)) : null;
    forEachOption(block, optionsOffset(type, block), (code, start, length) -> {
      String name = statName(type, code);
      if (name == null) {
        return;
      }
      Object value;
      switch (name) {
        case "if_speed":
        case "if_tsoffset":
        case "isb_ifrecv":
        case "isb_ifdrop":
        case "isb_filteraccept":
        case "isb_osdrop":
        case "isb_usrdeliv":
          value = block.getLong(start);
          break;
        case "if_tsresol":
        case "if_fcslen":
          value = block.get(start) & 0xFF;
          break;
        case "if_tzone":
          value = block.getInt(start);
          break;
        case "isb_starttime":
        case "isb_endtime":
          long timestamp = (long) block.getInt(start) << 32 | (block.getInt(start + 4) & 0xFFFFFFFFL);
          value = iface == null ? Interface.DEFAULT.toInstant(timestamp) : iface.toInstant(timestamp);
          break;
        case "if_ipv4addr":
          // Address followed by netmask
          value = formatAddress(block, start, 4) + "/" + Integer.bitCount(ByteBuffer.wrap(block.array(), start + 4, 4).getInt());
          break;
        case "ns_dnsip4addr":
          value = formatAddress(block, start, 4);
          break;
        case "if_ipv6addr":
          // Address followed by prefix length
          value = formatAddress(block, start, 16) + "/" + (block.get(start + 16) & 0xFF);
          break;
        case "ns_dnsip6addr":
          value = formatAddress(block, start, 16);
          break;
        case "if_macaddr":
        case "if_euiaddr":
          StringBuilder mac = new StringBuilder();
          for (int i = start; i < start + length; i++) {
            mac.append(mac.length() == 0 ? "" : ":").append(String.format("%02X", block.get(i)));
          }
          value = mac.toString();
          break;
        default:
          value = new String(block.array(), start, length, StandardCharsets.UTF_8);
          break;
      }
      stats.merge(name, value, (a, b) -> a instanceof String ? a + ", " + b : b);
    });
    return stats;
  }

  private static String statName(int type, int code) {
    switch (type) {
      case SECTION_HEADER_TYPE:
        return code >= 2 && code <= 4 ? new String[] {"shb_hardware", "shb_os", "shb_userappl"}[code - 2] : null;
      case INTERFACE_DESCRIPTION_TYPE:
        String[] names = {"if_name", "if_description", "if_ipv4addr", "if_ipv6addr", "if_macaddr", "if_euiaddr",
            "if_speed", "if_tsresol", "if_tzone", null, "if_os", "if_fcslen", "if_tsoffset"};
        return code >= 2 && code <= 14 ? names[code - 2] : null;
      case NAME_RESOLUTION_TYPE:
        return code >= 2 && code <= 4 ? new String[] {"ns_dnsname", "ns_dnsip4addr", "ns_dnsip6addr"}[code - 2] : null;
      case INTERFACE_STATISTICS_TYPE:
        String[] isb = {"isb_starttime", "isb_endtime", "isb_ifrecv", "isb_ifdrop", "isb_filteraccept",
            "isb_osdrop", "isb_usrdeliv"};
        return code >= 2 && code <= 8 ? isb[code - 2] : null;
      default:
        return null;
    }
  }

  private static String formatAddress(ByteBuffer block, int start, int length) {
    try {
      return InetAddress.getByAddress(Arrays.copyOfRange(block.array(), start, start + length)).getHostAddress();
    } catch (IOException e) {
      return null;
    }
  }

  private static String readComment(ByteBuffer block, int offset) {
    StringBuilder comment = new StringBuilder();
    forEachOption(block, offset, (code, start, length) -> {
      if (code == OPT_COMMENT) {
        appendComment(comment, block, start, length);
      }
    });
    return comment.length() == 0 ? null : comment.toString();
  }

  private static void appendComment(StringBuilder comment, ByteBuffer block, int start, int length) {
    if (comment.length() > 0) {
      comment.append('\n');
    }
    comment.append(new String(block.array(), start, length, StandardCharsets.UTF_8));
  }

  private static String formatHash(ByteBuffer block, int start, int length) {
    if (length < 1) {
      return null;
    }
    int algorithm = block.get(start) & 0xFF;
    StringBuilder hash = new StringBuilder(algorithm < HASH_ALGORITHMS.length
        ? HASH_ALGORITHMS[algorithm] : String.valueOf(algorithm)).append(':');
    for (int i = start + 1; i < start + length; i++) {
      hash.append(String.format("%02x", block.get(i)));
    }
    return hash.toString();
  }

  private interface OptionVisitor {
    void visit(int code, int start, int length);
  }

  /** Calls the visitor for each option from offset up to opt_endofopt or the end of the block. */
  private static void forEachOption(ByteBuffer block, int offset, OptionVisitor visitor) {
    while (offset + 4 <= block.capacity()) {
      int code = block.getShort(offset) & 0xFFFF;
      int length = block.getShort(offset + 2) & 0xFFFF;
      if (code == OPT_ENDOFOPT || offset + 4 + length > block.capacity()) {
        return;
      }
      visitor.visit(code, offset + 4, length);
      offset += 4 + pad4(length);
    }
  }

  private static int pad4(int length) {
    return (length + 3) & ~3;
  }

  /** What packets need from their Interface Description Block. */
  private static class Interface {
    static final Interface DEFAULT = new Interface(PacketDecoder.LINKTYPE_ETHERNET);
    // For packets naming an interface that was never described: nothing is decoded
    static final Interface UNDEFINED = new Interface(-1);
    final int linkType;
    String name;
    // Why the interface's settings could not all be used, or null
    String error;
    // Timestamps are counted in units of 1 / unitsPerSecond, plus an offset in seconds
    long unitsPerSecond = 1_000_000;
    long offsetSeconds;

    private Interface(int linkType) {
      this.linkType = linkType;
    }

    Interface(ByteBuffer block, int index) {
      linkType = block.getShort(0) & 0xFFFF;
      forEachOption(block, 8, (code, start, length) -> {
        switch (code) {
          case IF_NAME:
            name = new String(block.array(), start, length, StandardCharsets.UTF_8);
            break;
          case IF_TSRESOL:
            int resolution = block.get(start) & 0xFF;
            int exponent = resolution & 0x7F;
            if ((resolution & 0x80) == 0 && exponent <= 18) {
              unitsPerSecond = (long) Math.pow(10, exponent);
            } else if ((resolution & 0x80) != 0 && exponent <= 62) {
              unitsPerSecond = 1L << exponent;
            } else {
              error = "file: interface " + index + " has unsupported if_tsresol " + resolution
                  + "; timestamps assume microseconds";
            }
            break;
          case IF_TSOFFSET:
            offsetSeconds = block.getLong(start);
            break;
          default:
            break;
        }
      });
    }

    Instant toInstant(long timestamp) {
      long seconds = Long.divideUnsigned(timestamp, unitsPerSecond);
      long remainder = Long.remainderUnsigned(timestamp, unitsPerSecond);
      // remainder * 1e9 overflows only for resolutions finer than ~2^-33 s
      long nanos = remainder <= Long.MAX_VALUE / 1_000_000_000L
          ? remainder * 1_000_000_000L / unitsPerSecond
          : (long) (remainder / (double) unitsPerSecond * 1_000_000_000L);
      return Instant.ofEpochSecond(seconds + offsetSeconds, nanos);
    }
  }

  @Override
  public void close() {
    if (protocolColumns != null) {
      protocolColumns.logSummary(logger, path.toString());
    }
    AutoCloseables.closeSilently(in);
  }

  private void processBlock(PcapngBlock block) {
    if (block.errorRow) {
      protocolColumns.writeErrorRow(String.join("; ", block.errors));
      return;
    }
    loader.start();
    for (ColumnDefn columnDefn : projectedColumns) {
      try {
        if (columnDefn.getName().equals(PcapColumn.PATH_NAME)) {
          // pcapng file name
          columnDefn.load(path.getName());
        } else {
          // pcapng block data
          columnDefn.load(block);
        }
      } catch (RuntimeException e) {
        block.addError("packet: " + columnDefn.getName() + ": " + ProtocolDecoders.describe(e));
      }
    }
    if (config.getStat()) {
      protocolColumns.writeErrors(block.errors);
    } else {
      protocolColumns.writePacket(block.packet, block.errors);
    }
    loader.save();
  }

  private static boolean isDecoderColumn(String name) {
    return name.equals(ProtocolColumns.PARSED_PROTOCOL) || name.equals(ProtocolColumns.PARSED_DATA)
        || name.equals(ProtocolColumns.DECODE_ERROR);
  }

  /**
   * Stat queries take precedence: they describe the capture, not its packets.
   */
  private boolean isSessionQuery() {
    return config.getSessionizeTCPStreams() && !config.getStat();
  }

  private boolean isSkipQuery() {
    return columns.isEmpty();
  }

  private boolean isStarQuery() {
    return Utilities.isStarQuery(columns);
  }

  private TupleMetadata defineMetadata() {
    SchemaBuilder builder = new SchemaBuilder();
    processProjected(columns);
    for (ColumnDefn columnDefn : projectedColumns) {
      columnDefn.define(builder);
    }
    ProtocolColumns.addColumns(builder, rowMode(), ProtocolDecoders.get());
    return builder.buildSchema();
  }

  /**
   * <b> Define the schema based on projected </b><br/>
   * 1. SkipQuery: no field specified, such as count(*) <br/>
   * 2. StarQuery: select * <br/>
   * 3. ProjectPushdownQuery: select a,b,c <br/>
   */
  private void processProjected(List<SchemaPath> columns) {
    projectedColumns = new ArrayList<ColumnDefn>();
    if (isSkipQuery()) {
      projectedColumns.add(new ColumnDefn(PcapColumn.DUMMY_NAME, new PcapColumn.PcapDummy()));
    } else if (isStarQuery()) {
      Set<Map.Entry<String, PcapColumn>> pcapColumns;
      if (config.getStat()) {
        pcapColumns = PcapColumn.getSummaryColumns().entrySet();
      } else {
        pcapColumns = PcapColumn.getColumns().entrySet();
      }
      for (Map.Entry<String, PcapColumn> pcapColumn : pcapColumns) {
        makePcapColumns(projectedColumns, pcapColumn.getKey(), pcapColumn.getValue());
      }
    } else {
      for (SchemaPath schemaPath : columns) {
        // Support Case-Insensitive
        String projectedName = schemaPath.rootName().toLowerCase();
        if (isDecoderColumn(projectedName)) {
          continue;
        }
        PcapColumn pcapColumn;
        if (config.getStat()) {
          pcapColumn = PcapColumn.getSummaryColumns().get(projectedName);
        } else {
          pcapColumn = PcapColumn.getColumns().get(projectedName);
        }
        if (pcapColumn != null) {
          makePcapColumns(projectedColumns, projectedName, pcapColumn);
        } else {
          makePcapColumns(projectedColumns, projectedName, new PcapColumn.PcapDummy());
          logger.debug("{} missing the PcapColumn implement class.", projectedName);
        }
      }
    }
    Collections.unmodifiableList(projectedColumns);
  }

  private void makePcapColumns(List<ColumnDefn> projectedColumns, String name, PcapColumn column) {
    projectedColumns.add(new ColumnDefn(name, column));
  }

  private void bindColumns(RowSetLoader loader) {
    for (ColumnDefn columnDefn : projectedColumns) {
      columnDefn.bind(loader);
    }
  }

  private static class ColumnDefn {

    private final String name;
    private PcapColumn processor;
    private ScalarWriter writer;

    public ColumnDefn(String name, PcapColumn column) {
      this.name = name;
      this.processor = column;
    }

    public String getName() {
      return name;
    }

    public PcapColumn getProcessor() {
      return processor;
    }

    public void bind(RowSetLoader loader) {
      writer = loader.scalar(getName());
    }

    public void define(SchemaBuilder builder) {
      if (getProcessor().getType().getMode() == DataMode.REQUIRED) {
        builder.add(getName(), getProcessor().getType().getMinorType());
      } else {
        builder.addNullable(getName(), getProcessor().getType().getMinorType());
      }
    }

    public void load(PcapngBlock block) {
      getProcessor().process(block, writer);
    }

    public void load(String value) {
      writer.setString(value);
    }
  }
}
