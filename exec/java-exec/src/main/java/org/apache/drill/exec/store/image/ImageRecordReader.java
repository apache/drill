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

package org.apache.drill.exec.store.image;

import io.netty.buffer.DrillBuf;

import java.io.BufferedInputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import com.adobe.xmp.XMPException;
import com.adobe.xmp.XMPMeta;
import com.adobe.xmp.options.IteratorOptions;
import com.adobe.xmp.properties.XMPPropertyInfo;

import com.drew.imaging.FileType;
import com.drew.imaging.FileTypeDetector;
import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.lang.Charsets;
import com.drew.lang.KeyValuePair;
import com.drew.lang.Rational;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.StringValue;
import com.drew.metadata.Tag;
import com.drew.metadata.eps.EpsDirectory;
import com.drew.metadata.exif.ExifIFD0Directory;
import com.drew.metadata.exif.ExifInteropDirectory;
import com.drew.metadata.exif.ExifSubIFDDirectory;
import com.drew.metadata.exif.GpsDirectory;
import com.drew.metadata.exif.PanasonicRawIFD0Directory;
import com.drew.metadata.exif.makernotes.FujifilmMakernoteDirectory;
import com.drew.metadata.exif.makernotes.NikonType2MakernoteDirectory;
import com.drew.metadata.exif.makernotes.OlympusCameraSettingsMakernoteDirectory;
import com.drew.metadata.exif.makernotes.OlympusEquipmentMakernoteDirectory;
import com.drew.metadata.exif.makernotes.OlympusFocusInfoMakernoteDirectory;
import com.drew.metadata.exif.makernotes.OlympusImageProcessingMakernoteDirectory;
import com.drew.metadata.exif.makernotes.OlympusMakernoteDirectory;
import com.drew.metadata.exif.makernotes.OlympusRawDevelopment2MakernoteDirectory;
import com.drew.metadata.exif.makernotes.OlympusRawDevelopmentMakernoteDirectory;
import com.drew.metadata.exif.makernotes.OlympusRawInfoMakernoteDirectory;
import com.drew.metadata.exif.makernotes.PanasonicMakernoteDirectory;
import com.drew.metadata.exif.makernotes.SamsungType2MakernoteDirectory;
import com.drew.metadata.exif.makernotes.SonyType6MakernoteDirectory;
import com.drew.metadata.icc.IccDirectory;
import com.drew.metadata.jpeg.JpegComponent;
import com.drew.metadata.photoshop.PhotoshopDirectory;
import com.drew.metadata.png.PngDirectory;
import com.drew.metadata.xmp.XmpDirectory;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class ImageRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImageRecordReader.class);

  private final DrillFileSystem fs;
  private final Path hadoopPath;
  private final boolean fileSystemMetadata;
  private final boolean descriptive;
  private final TimeZone timeZone;

  private VectorContainerWriter writer;
  private FileStatus fileStatus;
  private BufferedInputStream metadataStream;
  private DrillBuf managedBuffer;
  private boolean finish;

  public ImageRecordReader(FragmentContext context, DrillFileSystem fs, String inputPath,
                           boolean fileSystemMetadata, boolean descriptive, String timeZone) {
    this.fs = fs;
    hadoopPath = fs.makeQualified(new Path(inputPath));
    this.fileSystemMetadata = fileSystemMetadata;
    this.descriptive = descriptive;
    this.timeZone = (timeZone != null) ? TimeZone.getTimeZone(timeZone) : TimeZone.getDefault();
    managedBuffer = context.getManagedBuffer();
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {

    try {
      fileStatus = fs.getFileStatus(hadoopPath);
      metadataStream = new BufferedInputStream(fs.open(hadoopPath));
      writer = new VectorContainerWriter(output);
      finish = false;
    } catch (Exception e) {
      throw handleAndRaise("Failure in creating record reader", e);
    }
  }

  private DrillBuf drillBuffer(byte[] b) {
    if (managedBuffer.capacity() < b.length) {
      managedBuffer = managedBuffer.reallocIfNeeded(b.length);
    }
    managedBuffer.clear();
    managedBuffer.writeBytes(b);
    return managedBuffer;
  }

  protected RuntimeException handleAndRaise(String s, Exception e) {
    throw UserException.dataReadError(e)
        .message(s + "\n%s", e.getMessage())
        .addContext("Path", hadoopPath.toUri().getPath())
        .build(logger);
  }

  @Override
  public int next() {

    if (finish) {
      return 0;
    }

    try {
      writer.allocate();
      writer.reset();

      final MapWriter rootWriter = writer.rootAsMap();
      final FileType fileType = FileTypeDetector.detectFileType(metadataStream);
      final Metadata metadata = ImageMetadataReader.readMetadata(metadataStream);

      try {
        new GenericMetadataReader().read(fileType, fileStatus, metadata);
        processGenericMetadataDirectory(rootWriter,
            metadata.getFirstDirectoryOfType(GenericMetadataDirectory.class));
      } catch (Exception e) {
        // simply skip this directory
      }

      boolean skipEPSPreview = false;

      for (Directory directory : metadata.getDirectories()) {
        try {
          if (directory instanceof GenericMetadataDirectory) {
            continue;
          }
          if (directory instanceof ExifIFD0Directory && skipEPSPreview) {
            skipEPSPreview = false;
            continue;
          }
          if (directory instanceof EpsDirectory) {
            // If an EPS file contains a TIFF preview, skip the next IFD0
            skipEPSPreview = directory.containsTag(EpsDirectory.TAG_TIFF_PREVIEW_SIZE);
          }
          final MapWriter directoryWriter = rootWriter.map(formatName(directory.getName()));
          processDirectory(directoryWriter, directory, metadata);
          if (directory instanceof XmpDirectory) {
            processXmpDirectory(directoryWriter, (XmpDirectory) directory);
          }
        } catch (Exception e) {
          // simply skip this directory
        }
      }

      writer.setValueCount(1);
      finish = true;
      return 1;
    } catch (ImageProcessingException e) {
      finish = true;
      return 0;
    } catch (Exception e) {
      throw handleAndRaise("Failure while reading image metadata record.", e);
    }
  }

  private void processGenericMetadataDirectory(final MapWriter writer,
                                               final GenericMetadataDirectory directory) {
    for (Tag tag : directory.getTags()) {
      try {
        final int tagType = tag.getTagType();
        if (tagType != GenericMetadataDirectory.TAG_FILE_SIZE &&
            tagType != GenericMetadataDirectory.TAG_FILE_DATE_TIME || fileSystemMetadata) {
          writeValue(writer, formatName(tag.getTagName()),
              descriptive ? directory.getDescription(tagType) : directory.getObject(tagType));
        }
      } catch (Exception e) {
        // simply skip this tag
      }
    }
  }

  private void processDirectory(final MapWriter writer, final Directory directory, final Metadata metadata) {
    for (Tag tag : directory.getTags()) {
      try {
        final int tagType = tag.getTagType();
        Object value;
        if (descriptive || isDescriptionTag(directory, tagType)) {
          value = directory.getDescription(tagType);
          if (directory instanceof PngDirectory) {
            if (((PngDirectory) directory).getPngChunkType().areMultipleAllowed()) {
              value = new String[] { (String) value };
            }
          }
        } else {
          value = directory.getObject(tagType);
          if (directory instanceof ExifIFD0Directory && tagType == ExifIFD0Directory.TAG_DATETIME) {
            ExifSubIFDDirectory exifSubIFDDir = metadata.getFirstDirectoryOfType(ExifSubIFDDirectory.class);
            String subsecond = null;
            if (exifSubIFDDir != null) {
              subsecond = exifSubIFDDir.getString(ExifSubIFDDirectory.TAG_SUBSECOND_TIME);
            }
            value = directory.getDate(tagType, subsecond, timeZone);
          } else if (directory instanceof ExifSubIFDDirectory) {
            if (tagType == ExifSubIFDDirectory.TAG_DATETIME_ORIGINAL) {
              value = ((ExifSubIFDDirectory) directory).getDateOriginal(timeZone);
            } else if (tagType == ExifSubIFDDirectory.TAG_DATETIME_DIGITIZED) {
              value = ((ExifSubIFDDirectory) directory).getDateDigitized(timeZone);
            }
          } else if (directory instanceof GpsDirectory) {
            if (tagType == GpsDirectory.TAG_LATITUDE) {
              value = ((GpsDirectory) directory).getGeoLocation().getLatitude();
            } else if (tagType == GpsDirectory.TAG_LONGITUDE) {
              value = ((GpsDirectory) directory).getGeoLocation().getLongitude();
            }
          }
          if (isVersionTag(directory, tagType)) {
            value = directory.getString(tagType, "US-ASCII");
          } else if (isDateTag(directory, tagType)) {
            value = directory.getDate(tagType, timeZone);
          }
        }
        writeValue(writer, formatName(tag.getTagName()), value);
      } catch (Exception e) {
        // simply skip this tag
      }
    }
  }

  private void processXmpDirectory(final MapWriter writer, final XmpDirectory directory) {
    HashSet<String> listItems = new HashSet<>();
    XMPMeta xmpMeta = directory.getXMPMeta();
    if (xmpMeta != null) {
      try {
        IteratorOptions iteratorOptions = new IteratorOptions().setJustLeafnodes(true);
        for (final Iterator i = xmpMeta.iterator(iteratorOptions); i.hasNext(); ) {
          try {
            XMPPropertyInfo prop = (XMPPropertyInfo) i.next();
            String path = prop.getPath();
            String value = prop.getValue();
            if (path != null && value != null) {
              // handling lang-alt array items
              if (prop.getOptions().getHasLanguage()) {
                XMPPropertyInfo langProp = (XMPPropertyInfo) i.next();
                if (langProp.getPath().endsWith("/xml:lang")) {
                  String lang = langProp.getValue();
                  path = path.replaceFirst("\\[\\d+\\]$", "") +
                      (lang.equals("x-default") ? "" : "_" + lang);
                }
              }

              FieldWriter writerSub = (FieldWriter) writer;
              String[] elements = path.replaceAll("/\\w+:", "/").split(":|/|(?=\\[)");
              for (int j = 1; j < elements.length; j++) {
                String parent = elements[j - 1];
                boolean isList = elements[j].startsWith("[");
                if (parent.startsWith("[")) {
                  writerSub = (FieldWriter) (isList ? writerSub.list() : writerSub.map());
                  if (listItems.add(path.replaceFirst("[^\\]]+$", ""))) {
                    writerSub.start();
                  }
                } else {
                  writerSub = (FieldWriter)
                      (isList ? writerSub.list(formatName(parent)) : writerSub.map(formatName(parent)));
                }
              }
              String parent = elements[elements.length - 1];
              VarCharWriter varCharWriter = parent.startsWith("[") ?
                  writerSub.varChar() : writerSub.varChar(formatName(parent));
              writeString(varCharWriter, value);
            }
          } catch (Exception e) {
            // simply skip this property
          }
        }
      } catch (XMPException ignored) {
      }
    }
  }

  private void writeValue(final MapWriter writer, final String tagName, final Object value) {
    if (value == null) {
      return;
    }

    if (value instanceof Boolean) {
      writer.bit(tagName).writeBit((Boolean) value ? 1 : 0);
    } else if (value instanceof Byte) {
      // TINYINT is not supported
      writer.integer(tagName).writeInt(((Byte) value).intValue());
    } else if (value instanceof Short) {
      // SMALLINT is not supported
      writer.integer(tagName).writeInt(((Short) value).intValue());
    } else if (value instanceof Integer) {
      writer.integer(tagName).writeInt((Integer) value);
    } else if (value instanceof Long) {
      writer.bigInt(tagName).writeBigInt((Long) value);
    } else if (value instanceof Float) {
      writer.float4(tagName).writeFloat4((Float) value);
    } else if (value instanceof Double) {
      writer.float8(tagName).writeFloat8((Double) value);
    } else if (value instanceof Rational) {
      writer.float8(tagName).writeFloat8(((Rational) value).doubleValue());
    } else if (value instanceof String) {
      writeString(writer.varChar(tagName), (String) value);
    } else if (value instanceof StringValue) {
      writeString(writer.varChar(tagName), ((StringValue) value).toString());
    } else if (value instanceof Date) {
      writer.timeStamp(tagName).writeTimeStamp(((Date) value).getTime());
    } else if (value instanceof boolean[]) {
      for (boolean v : (boolean[]) value) {
        writer.list(tagName).bit().writeBit(v ? 1 : 0);
      }
    } else if (value instanceof byte[]) {
      final byte[] bytes = (byte[]) value;
      if (bytes.length == 1) {
        writer.integer(tagName).writeInt(bytes[0]);
      } else if (bytes.length <= 4) {
        ListWriter listWriter = writer.list(tagName);
        for (byte v : bytes) {
          listWriter.integer().writeInt(v);
        }
      } else {
        writer.varBinary(tagName).writeVarBinary(0, bytes.length, drillBuffer(bytes));
      }
    } else if (value instanceof short[]) {
      ListWriter listWriter = writer.list(tagName);
      for (short v : (short[]) value) {
        // SMALLINT is not supported
        listWriter.integer().writeInt(v);
      }
    } else if (value instanceof int[]) {
      ListWriter listWriter = writer.list(tagName);
      for (int v : (int[]) value) {
        listWriter.integer().writeInt(v);
      }
    } else if (value instanceof long[]) {
      ListWriter listWriter = writer.list(tagName);
      for (long v : (long[]) value) {
        listWriter.bigInt().writeBigInt(v);
      }
    } else if (value instanceof float[]) {
      ListWriter listWriter = writer.list(tagName);
      for (float v : (float[]) value) {
        listWriter.float4().writeFloat4(v);
      }
    } else if (value instanceof double[]) {
      ListWriter listWriter = writer.list(tagName);
      for (double v : (double[]) value) {
        listWriter.float8().writeFloat8(v);
      }
    } else if (value instanceof Rational[]) {
      ListWriter listWriter = writer.list(tagName);
      for (Rational v : (Rational[]) value) {
        listWriter.float8().writeFloat8(v.doubleValue());
      }
    } else if (value instanceof String[]) {
      ListWriter listWriter = writer.list(tagName);
      for (String v : (String[]) value) {
        writeString(listWriter.varChar(), v);
      }
    } else if (value instanceof StringValue[]) {
      ListWriter listWriter = writer.list(tagName);
      for (StringValue v : (StringValue[]) value) {
        writeString(listWriter.varChar(), v.toString());
      }
    } else if (value instanceof JpegComponent) {
      final JpegComponent v = (JpegComponent) value;
      writer.map(tagName).integer("ComponentId").writeInt(v.getComponentId());
      writer.map(tagName).integer("HorizontalSamplingFactor").writeInt(v.getHorizontalSamplingFactor());
      writer.map(tagName).integer("VerticalSamplingFactor").writeInt(v.getVerticalSamplingFactor());
      writer.map(tagName).integer("QuantizationTableNumber").writeInt(v.getQuantizationTableNumber());
    } else if (value instanceof List<?>) {
      ListWriter listWriter = writer.list(tagName);
      for (Object v : (List<?>) value) {
        if (v instanceof KeyValuePair) {
          listWriter.map().start();
          writeString(listWriter.map().varChar("Key"), ((KeyValuePair) v).getKey());
          writeString(listWriter.map().varChar("Value"), ((KeyValuePair) v).getValue().toString());
          listWriter.map().end();
        } else {
          writeString(listWriter.varChar(), v.toString());
        }
      }
    } else {
      writeString(writer.varChar(tagName), value.toString());
    }
  }

  private void writeString(final VarCharWriter writer, final String value) {
    final byte[] stringBytes = value.getBytes(Charsets.UTF_8);
    writer.writeVarChar(0, stringBytes.length, drillBuffer(stringBytes));
  }

  private String formatName(final String tagName) {
    StringBuilder builder = new StringBuilder();
    boolean upperCase = true;
    for (char c : tagName.toCharArray()) {
      if (c == ' ' || c == '-' || c == '/') {
        upperCase = true;
      } else {
        builder.append(upperCase ? Character.toUpperCase(c) : c);
        upperCase = false;
      }
    }
    return builder.toString();
  }

  private boolean isDescriptionTag(final Directory directory, final int tagType) {
    return directory instanceof IccDirectory && tagType > 0x20202020 && tagType < 0x7a7a7a7a ||
        directory instanceof PhotoshopDirectory;
  }

  private boolean isVersionTag(final Directory directory, final int tagType) {
    return directory instanceof ExifSubIFDDirectory &&
        (tagType == ExifSubIFDDirectory.TAG_EXIF_VERSION || tagType == ExifSubIFDDirectory.TAG_FLASHPIX_VERSION) ||
        directory instanceof ExifInteropDirectory &&
        tagType == ExifInteropDirectory.TAG_INTEROP_VERSION ||
        directory instanceof FujifilmMakernoteDirectory &&
        tagType == FujifilmMakernoteDirectory.TAG_MAKERNOTE_VERSION ||
        directory instanceof NikonType2MakernoteDirectory &&
        tagType == NikonType2MakernoteDirectory.TAG_FIRMWARE_VERSION ||
        directory instanceof OlympusCameraSettingsMakernoteDirectory &&
        tagType == OlympusCameraSettingsMakernoteDirectory.TagCameraSettingsVersion ||
        directory instanceof OlympusEquipmentMakernoteDirectory &&
        tagType == OlympusEquipmentMakernoteDirectory.TAG_EQUIPMENT_VERSION ||
        directory instanceof OlympusFocusInfoMakernoteDirectory &&
        tagType == OlympusFocusInfoMakernoteDirectory.TagFocusInfoVersion ||
        directory instanceof OlympusImageProcessingMakernoteDirectory &&
        tagType == OlympusImageProcessingMakernoteDirectory.TagImageProcessingVersion ||
        directory instanceof OlympusMakernoteDirectory &&
        tagType == OlympusMakernoteDirectory.TAG_MAKERNOTE_VERSION ||
        directory instanceof OlympusRawDevelopment2MakernoteDirectory &&
        tagType == OlympusRawDevelopment2MakernoteDirectory.TagRawDevVersion ||
        directory instanceof OlympusRawDevelopmentMakernoteDirectory &&
        tagType == OlympusRawDevelopmentMakernoteDirectory.TagRawDevVersion ||
        directory instanceof OlympusRawInfoMakernoteDirectory &&
        tagType == OlympusRawInfoMakernoteDirectory.TagRawInfoVersion ||
        directory instanceof PanasonicMakernoteDirectory &&
        (tagType == PanasonicMakernoteDirectory.TAG_FIRMWARE_VERSION || tagType == PanasonicMakernoteDirectory.TAG_MAKERNOTE_VERSION || tagType == PanasonicMakernoteDirectory.TAG_EXIF_VERSION) ||
        directory instanceof SamsungType2MakernoteDirectory &&
        tagType == SamsungType2MakernoteDirectory.TagMakerNoteVersion ||
        directory instanceof SonyType6MakernoteDirectory &&
        tagType == SonyType6MakernoteDirectory.TAG_MAKERNOTE_THUMB_VERSION ||
        directory instanceof PanasonicRawIFD0Directory &&
        tagType == PanasonicRawIFD0Directory.TagPanasonicRawVersion;
  }

  private boolean isDateTag(final Directory directory, final int tagType) {
    return directory instanceof IccDirectory && tagType == IccDirectory.TAG_PROFILE_DATETIME ||
        directory instanceof PngDirectory && tagType == PngDirectory.TAG_LAST_MODIFICATION_TIME;
  }

  @Override
  public void close() throws Exception {
    if (metadataStream != null) {
      metadataStream.close();
    }
  }

  @Override
  public String toString() {
    return "ImageRecordReader[Path=" + hadoopPath.toUri().getPath() + "]";
  }
}
