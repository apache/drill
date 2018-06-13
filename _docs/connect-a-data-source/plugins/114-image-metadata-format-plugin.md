---
title: "Image Metadata Format Plugin"
date: 2018-06-13 18:31:03 UTC
parent: "Connect a Data Source"
---

**Note:** You can see the source of this content at [image-metadata-format-plugin](https://gist.github.com/nagix/6cac019b7bec698a1b8a234a5268d09d#file-image-metadata-format-plugin-md). 

As of 1.14, Drill can query the metadata in various image formats. The metadata format plugin is useful for querying a large number of image files stored in a distributed file system. You do not have to build a metadata repository in advance.

The metadata format plugin supports the following file formats:

JPEG, TIFF, PSD, PNG, BMP, GIF, ICO, PCX, WAV, AVI, WebP, MOV, MP4, EPS
Camera Raw: ARW (Sony), CRW/CR2 (Canon), NEF (Nikon), ORF (Olympus), RAF (FujiFilm), RW2 (Panasonic), RWL (Leica), SRW (Samsung), X3F (Foveon)  

The metadata format plugin enables Drill to read the following metadata:

Exif, IPTC, XMP, JFIF / JFXX, ICC Profiles, Photoshop fields, PNG properties, BMP properties, GIF properties, ICO properties, PCX properties, WAV properties, AVI properties, WebP properties, QuickTime properties, MP4 properties, EPS properties

Since each type of metadata has a different set of fields, the plugin returns a set of commonly-used fields, such as the image width, height, and bits per pixels for ease of use.

To configure Drill to read image metadata, you must modify the extensions section in the dfs storage plugin configuration, as shown:  

       formats: {
         "image": {
           "type": "image",
           "extensions": [
             "jpg", "jpeg", "jpe", "tif", "tiff", "dng", "psd", "png", "bmp", "gif",
             "ico", "pcx", "wav", "wave", "avi", "webp", "mov", "mp4", "m4a", "m4p",
             "m4b", "m4r", "m4v", "3gp", "3g2", "eps", "epsf", "epsi", "ai", "arw",
             "crw", "cr2", "nef", "orf", "raf", "rw2", "rwl", "srw", "x3f"
           ]
           "fileSystemMetadata": true,
           "descriptive": true,
           "timeZone": null
         }  
       }  

**Note:** The result does not include file names, but you can use [implicit columns]({{site.baseurl}}/docs/querying-a-file-system-introduction/#implicit-columns) to get file names, full paths, fully qualified names, and file suffixes.  


##Attributes

The following table lists configuration attributes:  

Attribute|Default Value|Description
---------|-------------|-----------
fileSystemMetadata|true|Set to true to extract filesystem metadata including the file size and the last modified timestamp, false otherwise. 
descriptive|true|Set to true to extract metadata in a human-readable string format. Set false to extract metadata in a machine-readable typed format.
timeZone|null|Specify the time zone to interpret the timestamp with no time zone information. If the timestamp includes the time zone information, this value is ignored. If null is set, the local time zone is used.  

##Examples  

A Drill query on a JPEG file with the property descriptive: true

       0: jdbc:drill:zk=local> select FileName, * from dfs.`4349313028_f69ffa0257_o.jpg`;  
       +----------+----------+--------------+--------+------------+-------------+--------------+----------+-----------+------------+-----------+----------+----------+------------+-----------+------------+-----------------+-----------------+------+------+----------+------------+------------------+-----+---------------+-----------+------+---------+----------+
       | FileName | FileSize | FileDateTime | Format | PixelWidth | PixelHeight | BitsPerPixel | DPIWidth | DPIHeight | Orientaion | ColorMode | HasAlpha | Duration | VideoCodec | FrameRate | AudioCodec | AudioSampleSize | AudioSampleRate | JPEG | JFIF | ExifIFD0 | ExifSubIFD | Interoperability | GPS | ExifThumbnail | Photoshop | IPTC | Huffman | FileType |
       +----------+----------+--------------+--------+------------+-------------+--------------+----------+-----------+------------+-----------+----------+----------+------------+-----------+------------+-----------------+-----------------+------+------+----------+------------+------------------+-----+---------------+-----------+------+---------+----------+
       | 4349313028_f69ffa0257_o.jpg | 257213 bytes | Fri Mar 09 12:09:34 +08:00 2018 | JPEG | 1199 | 800 | 24 | 96 | 96 | Unknown (0) | RGB | false | 00:00:00 | Unknown | 0 | Unknown | 0 | 0 | {"CompressionType":"Baseline","DataPrecision":"8 bits","ImageHeight":"800 pixels","ImageWidth":"1199 pixels","NumberOfComponents":"3","Component1":"Y component: Quantization table 0, Sampling factors 2 horiz/2 vert","Component2":"Cb component: Quantization table 1, Sampling factors 1 horiz/1 vert","Component3":"Cr component: Quantization table 1, Sampling factors 1 horiz/1 vert"} | {"Version":"1.1","ResolutionUnits":"inch","XResolution":"96 dots","YResolution":"96 dots","ThumbnailWidthPixels":"0","ThumbnailHeightPixels":"0"} | {"Software":"Picasa 3.0"} | {"ExifVersion":"2.10","UniqueImageID":"d65e93b836d15a0c5e041e6b7258c76e"} | {"InteroperabilityIndex":"Unknown (    )","InteroperabilityVersion":"1.00"} | {"GPSVersionID":".022","GPSLatitudeRef":"N","GPSLatitude":"47° 32' 15.98\"","GPSLongitudeRef":"W","GPSLongitude":"-122° 2' 6.37\"","GPSAltitudeRef":"Sea level","GPSAltitude":"0 metres"} | {"Compression":"JPEG (old-style)","XResolution":"72 dots per inch","YResolution":"72 dots per inch","ResolutionUnit":"Inch","ThumbnailOffset":"414 bytes","ThumbnailLength":"7213 bytes"} | {} | {"Keywords":"135;2002;issaquah;police car;wa;washington"} | {"NumberOfTables":"4 Huffman tables"} | {"DetectedFileTypeName":"JPEG","DetectedFileTypeLongName":"Joint Photographic Experts Group","DetectedMIMEType":"image/jpeg","ExpectedFileNameExtension":"jpg"} |
       +----------+----------+--------------+--------+------------+-------------+--------------+----------+-----------+------------+-----------+----------+----------+------------+-----------+------------+-----------------+-----------------+------+------+----------+------------+------------------+-----+---------------+-----------+------+---------+----------+
 

A Drill query on a JPEG file with the property descriptive: false    

       0: jdbc:drill:zk=local> select FileName, * from dfs.`4349313028_f69ffa0257_o.jpg`;  
       +----------+----------+--------------+--------+------------+-------------+--------------+----------+-----------+------------+-----------+----------+----------+------------+-----------+------------+-----------------+-----------------+------+------+----------+------------+------------------+-----+---------------+-----------+------+---------+----------+
       | FileName | FileSize | FileDateTime | Format | PixelWidth | PixelHeight | BitsPerPixel | DPIWidth | DPIHeight | Orientaion | ColorMode | HasAlpha | Duration | VideoCodec | FrameRate | AudioCodec | AudioSampleSize | AudioSampleRate | JPEG | JFIF | ExifIFD0 | ExifSubIFD | Interoperability | GPS | ExifThumbnail | Photoshop | IPTC | Huffman | FileType |
       +----------+----------+--------------+--------+------------+-------------+--------------+----------+-----------+------------+-----------+----------+----------+------------+-----------+------------+-----------------+-----------------+------+------+----------+------------+------------------+-----+---------------+-----------+------+---------+----------+
       | 4349313028_f69ffa0257_o.jpg | 257213 | 2018-03-09 04:09:34.0 | JPEG | 1199 | 800 | 24 | 96.0 | 96.0 | 0 | RGB | false | 0 | Unknown | 0.0 | Unknown | 0 | 0.0 | {"CompressionType":0,"DataPrecision":8,"ImageHeight":800,"ImageWidth":1199,"NumberOfComponents":3,"Component1":{"ComponentId":1,"HorizontalSamplingFactor":2,"VerticalSamplingFactor":2,"QuantizationTableNumber":0},"Component2":{"ComponentId":2,"HorizontalSamplingFactor":1,"VerticalSamplingFactor":1,"QuantizationTableNumber":1},"Component3":{"ComponentId":3,"HorizontalSamplingFactor":1,"VerticalSamplingFactor":1,"QuantizationTableNumber":1}} | {"Version":257,"ResolutionUnits":1,"XResolution":96,"YResolution":96,"ThumbnailWidthPixels":0,"ThumbnailHeightPixels":0} | {"Software":"Picasa 3.0"} | {"ExifVersion":"0210","UniqueImageID":"d65e93b836d15a0c5e041e6b7258c76e"} | {"InteroperabilityIndex":"    ","InteroperabilityVersion":"0100"} | {"GPSVersionID":[0,0,2,2],"GPSLatitudeRef":"N","GPSLatitude":47.53777313232332,"GPSLongitudeRef":"W","GPSLongitude":-122.03510284423795,"GPSAltitudeRef":0,"GPSAltitude":0.0} | {"Compression":6,"XResolution":72.0,"YResolution":72.0,"ResolutionUnit":2,"ThumbnailOffset":414,"ThumbnailLength":7213} | {} | {"Keywords":["135","2002","issaquah","police car","wa","washington"]} | {"NumberOfTables":4} | {"DetectedFileTypeName":"JPEG","DetectedFileTypeLongName":"Joint Photographic Experts Group","DetectedMIMEType":"image/jpeg","ExpectedFileNameExtension":"jpg"} |
       +----------+----------+--------------+--------+------------+-------------+--------------+----------+-----------+------------+-----------+----------+----------+------------+-----------+------------+-----  

Retrieving GPS location data from the Exif metadata for the use of GIS functions.

       0: jdbc:drill:zk=local> select t.GPS.GPSLatitude as lat, t.GPS.GPSLongitude as lon from dfs.`4349313028_f69ffa0257_o.jpg` t;
       +--------------------+----------------------+
       |        lat         |         lon          |
       +--------------------+----------------------+
       | 47.53777313232332  | -122.03510284423795  |
       +--------------------+----------------------+  
       
Retrieving the images that are larger than 640 x 480 pixels.
       
       0: jdbc:drill:zk=local> select FileName, PixelWidth, PixelHeight from dfs.`/images/*.png` where PixelWidth >= 640 and PixelHeight >= 480;
       +--------------------------+-------------+--------------+
       |         FileName         | PixelWidth  | PixelHeight  |
       +--------------------------+-------------+--------------+
       | 1.png                    | 2788        | 1758         |
       | 1500x500.png             | 1500        | 500          |
       | 2.png                    | 2788        | 1758         |
       | 9784873116914_1.png      | 874         | 1240         |
       | Driven-Example-Load.png  | 1208        | 970          |
       | features-diagram.png     | 1170        | 644          |
       | hal1.png                 | 1223        | 772          |
       | hal2.png                 | 1184        | 768          |
       | image-3.png              | 1200        | 771          |
       | image-4.png              | 1200        | 771          |
       | image002.png             | 1689        | 695          |
       +--------------------------+-------------+--------------+  

## Supported File Formats

Format|File extension|Description
------|--------------|-----------
JPEG|jpg, jpeg, jpe, nef|Joint Photographic Expert Group
TIFF|tif, tiff, dng, nef, srw|Tagged Image File Format
PSD|psd|Photoshop Document
PNG|png|Portable Network Graphics
BMP|bmp|Device Independent Bitmap
GIF|gif|Graphics Interchange Format
ICO|ico|Windows Icon
PCX|pcx|PiCture eXchange
WAV|wav, wave|Waveform Audio File Format
AVI|avi|Audio Video Interleaved
WebP|webp|WebP
MOV|mov|QuickTime Movie
MP4|mp4, m4a, m4p, m4b, m4r, m4v, 3gp, 3g2|MPEG-4 Part 14
EPS|eps, epsf, epsi, ai|Encapsulated PostScript
ARW|arw, dng|Sony Camera Raw
CRW|crw|Canon Camera Raw version 1
CR2|cr2|Canon Camera Raw version 2
ORF|orf|Olympus Camera Raw
RAF|raf|FujiFilm Camera Raw
RW2|rw2, rwl|Panasonic Camera Raw

## Supported Metadata

Metadata|JPEG|TIFF|PSD|PNG|BMP|GIF|ICO|PCX|WAV|AVI|WebP|MOV|MP4|EPS|ARW|CRW|CR2|ORF|RAF|RW2
--------|:--:|:--:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:--:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:
FileType|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x
JFIF|x|||||||||||||||||||
ExifIFD0|x|x|x|||||||||||x|x||x|x|x|x
ExifSubIFD|x|x|x|||||||||||x|x||x|x|x|x
Interoperability|x||||||||||||||x||x||x|x
GPS|x|x|||||||||||||x||x|||
PrintIM|x||||||||||||||x|||x|x|x
ExifThumnail|x|x|x|||||||||||x|x||x||x|x
XMP|x|x|x|x|||||||x||||x||x|||
ICCProfile|x|x|x|x|||||||x|||x||||||
JPEG|x||||||||||||||||||x|x
IPTC|x|x|x|||||||||||x||||||
Photoshop|x|x|x|||||||||||x||||||
AdobeJPEG|x|||||||||||||||||||
Huffman|x||||||||||||||||||x|x
PSDHeader|||x|||||||||||||||||
PNGIHDR||||x||||||||||||||||
PNGPLTE||||x||||||||||||||||
PNGTRNS||||x||||||||||||||||
PNGChromaticities||||x||||||||||||||||
PNGGAMA||||x||||||||||||||||
PNGICCP||||x||||||||||||||||
PNGSBIT||||x||||||||||||||||
PNGSRGB||||x||||||||||||||||
PNGTEXt||||x||||||||||||||||
PNGZTXt||||x||||||||||||||||
PNGITXt||||x||||||||||||||||
PNGBKGD||||x||||||||||||||||
PNGPHYs||||x||||||||||||||||
PNGTIME||||x||||||||||||||||
BMPHeader|||||x|||||||||||||||
GIFHeader||||||x||||||||||||||
GIFControl||||||x||||||||||||||
GIFAnimation||||||x||||||||||||||
GIFImage||||||x||||||||||||||
ICO|||||||x|||||||||||||
PCX||||||||x||||||||||||
WAV|||||||||x|||||||||||
AVI||||||||||x||||||||||
WebP|||||||||||x|||||||||
QuickTime||||||||||||x||||||||
QuickTimeVideo||||||||||||x||||||||
QuickTimeSound||||||||||||x||||||||
QuickTimeMetadata||||||||||||x||||||||
MP4|||||||||||||x|||||||
MP4Video|||||||||||||x|||||||
MP4Sound|||||||||||||x|||||||
EPS||||||||||||||x||||||
SonyMakernote|x||||||||||||||x|||||
CanonMakernote|x||||||||||||||||x|||
NikonMakernote|x|x||||||||||||||||||
OlympusMakernote|x|||||||||||||||||x||
OlympusEquipment|x|||||||||||||||||x||
OlympusCameraSettings|x|||||||||||||||||x||
OlympusRawDevelopment|x|||||||||||||||||x||
OlympusImageProcessing|x|||||||||||||||||x||
OlympusFocusInfo|x|||||||||||||||||x||
OlympusRawInfo||||||||||||||||||x||
FujifilmMakernote|x||||||||||||||||||x|
PanasonicMakernote|x|||||||||||||||||||x
PanasonicRawExifIFD0||||||||||||||||||||x
PanasonicRawDistortionInfo||||||||||||||||||||x
PanasonicRawWbInfo2||||||||||||||||||||x
AppleMakernote|x|||||||||||||||||||
CasioMakernote|x|||||||||||||||||||
KyoceraContaxMakernote|x|||||||||||||||||||
LeicaMakernote|||||||||||||||x|||||
PentaxMakernote|x|||||||||||||||||||
ReconyxHyperFireMakernote|x|||||||||||||||||||
Samsung Makernote|x|x|||||||||||||x|||||
SanyoMakernote|x|||||||||||||||||||
SigmaMakernote|x|||||||||||||||||||

## Columns

Column Name|Data Type|Description
-----------|---------|-----------
FileSize|BIGINT|File size in bytes
FileDateTime|TIMESTAMP|File modification date and time
Format|VARCHAR|File format: `JPEG`, `TIFF`, `PSD`, `PNG`, `BMP`, `GIF`, `ICO`, `PCX`, `RIFF`, `WAV`, `AVI`, `WebP`, `MOV`, `MP4`, `EPS`, `ARW`, `CRW`, `CR2`, `NEF`, `ORF`, `RAF`, `RW2` or `UNKNOWN`
DPIWidth|DOUBLE|Horizontal dot per inch
DPIHeight|DOUBLE|Vertical dot per inch
PixelWidth|INTEGER|Image width in pixels
PixelHeight|INTEGER|Image height in pixels
BitsPerPixel|INTEGER|Bits per pixel
Orientation|INTEGER|Orientation of the image:<br>`1` (Horizontal / normal)<br>`2` (Mirror horizontal)<br>`3` (Rotate 180)<br>`4` (Mirror vertical)<br>`5` (Mirror horizontal and rotate 270 CW)<br>`6` (Rotate 90 CW)<br>`7` (Mirror horizontal and rotate 90 CW)<br>`8` (Rotate 270 CW)
ColorMode|VARCHAR|Color mode: `RGB`, `Lab`, `CMYK`, `Multichannel`, `Duotone`, `Grayscale`, `Bitmap` or `Indexed`
HasAlpha|BOOLEAN|`true` if the image has an alpha channel
Duration|BIGINT|Duration in seconds
VideoCodec|VARCHAR|Video Codec
FrameRate|DOUBLE|Frames per second
AudioCodec|VARCHAR|Audio Codec
AudioSampleSize|INTEGER|Bits per sample
AudioSampleRate|DOUBLE|Samples per second
FileType.DetectedFileTypeName|VARCHAR|Detected file type: `JPEG`, `TIFF`, `PSD`, `PNG`, `BMP`, `GIF`, `ICO`, `PCX`, `RIFF`, `WAV`, `AVI`, `WebP`, `MOV`, `MP4`, `EPS`, `ARW`, `CRW`, `CR2`, `NEF`, `ORF`, `RAF`, `RW2` or `UNKNOWN`
FileType.DetectedFileTypeLongName|VARCHAR|
FileType.DetectedMIMEType|VARCHAR|MIME type: `image/jpeg`
FileType.ExpectedFileNameExtension|VARCHAR|Expected file extension: `jpg`
JFIF.Version|INTEGER|The second least significant byte is used for major revisions, and the least significant byte for minor revisions
JFIF.ResolutionUnits|INTEGER|Resolution unit: `0` (None), `2` (inches) or `3` (cm)
JFIF.XResolution|INTEGER|Horizontal pixel resolution
JFIF.YResolution|INTEGER|Vertical pixel resolution
JFIF.ThumbnailWidthPixels|INTEGER|Thumbnail horizontal pixel count
JFIF.ThumbnailHeightPixels|INTEGER|Thumbnail vertical pixel count
ExifIFD0.NewSubfileType|INTEGER|New subfile type:<br>`0` (Full-resolution Image)<br>`1` (Reduced-resolution image)<br>`2` (Single page of multi-page image)<br>`3` (Single page of multi-page reduced-resolution image)<br>`4` (Transparency mask)<br>`5` (Transparency mask of reduced-resolution image)<br>`6` (Transparency mask of multi-page image)<br>`7` (Transparency mask of reduced-resolution multi-page image)
ExifIFD0.ImageWidth|INTEGER|Number of columns of image data, equal to the number of pixels per row
ExifIFD0.ImageHeight|INTEGER|Number of rows of image data
ExifIFD0.BitsPerSample[3]|INTEGER|Number of bits per image component
ExifIFD0.Compression|INTEGER|Compression scheme used for the image data:<br>`1` (Uncompressed)<br>`2` (CCITT 1D)<br>`3` (T4/Group 3 Fax)<br>`4` (T6/Group 4 Fax)<br>`5` (LZW)<br>`6` (JPEG (old-style))<br>`7` (JPEG)<br>`8` (Adobe Deflate)<br>`9` (JBIG B&W)<br>`10` (JBIG Color)<br>`99` (JPEG)<br>`262` (Kodak 262)<br>`32766` (Next)<br>`32767` (Sony ARW Compressed)<br>`32769` (Packed RAW)<br>`32770` (Samsung SRW Compressed)<br>`32771` (CCIRLEW)<br>`32772` (Samsung SRW Compressed 2)<br>`32773` (PackBits)<br>`32809` (Thunderscan)<br>`32867` (Kodak KDC Compressed)<br>`32895` (IT8CTPAD)<br>`32896` (IT8LW)<br>`32897` (IT8MP<br>`32898` (IT8BL)<br>`32908` (PixarFilm)<br>`32909` (PixarLog)<br>`32946` (Deflate)<br>`32947` (DCS)<br>`34661` (JBIG)<br>`34676` (SGILog)<br>`34677` (SGILog24)<br>`34712` (JPEG 2000)<br>`34713` (Nikon NEF Compressed)<br>`34715` (JBIG2 TIFF FX)<br>`34718` (Microsoft Document Imaging (MDI) Binary Level Codec)<br>`34719` (Microsoft Document Imaging (MDI) Progressive Transform Codec)<br>`34720` (Microsoft Document Imaging (MDI) Vector)<br>`34892` (Lossy JPEG)<br>`65000` (Kodak DCR Compressed)<br>`65535` (Pentax PEF Compressed)
ExifIFD0.PhotometricInterpretation|INTEGER|Color space of the image data components:<br>`0` (WhiteIsZero)<br>`1` (BlackIsZero)<br>`2` (RGB)<br>`3` (RGB Palette)<br>`4` (Transparency Mask)<br>`5` (CMYK)<br>`6` (YCbCr)<br>`8` (CIELab)<br>`9` (ICCLab)<br>`10` (ITULab)<br>`32803` (Color Filter Array)<br>`32844` (Pixar LogL)<br>`32845` (Pixar LogLuv)<br>`34892` (Linear Raw)
ExifIFD0.ImageDescription|VARCHAR|Title of the image
ExifIFD0.Make|VARCHAR|Manufacturer of the recording equipment
ExifIFD0.Model|VARCHAR|Model name of the recording equipment
ExifIFD0.StripOffsets|INTEGER|Position in the file of raster data
ExifIFD0.Orientation|INTEGER|Orientation of the image:<br>`1` (Horizontal / normal)<br>`2` (Mirror horizontal)<br>`3` (Rotate 180)<br>`4` (Mirror vertical)<br>`5` (Mirror horizontal and rotate 270 CW)<br>`6` (Rotate 90 CW)<br>`7` (Mirror horizontal and rotate 90 CW)<br>`8` (Rotate 270 CW)
ExifIFD0.SamplesPerPixel|INTEGER|Samples per pixel
ExifIFD0.RowsPerStrip|INTEGER|Raster is codified by a single block of data holding this many rows
ExifIFD0.StripByteCounts|INTEGER|Size of the raster data in bytes
ExifIFD0.XResolution|DOUBLE|Horizontal pixel resolution
ExifIFD0.YResolution|DOUBLE|Vertical pixel resolution
ExifIFD0.PlanarConfiguration|INTEGER|Indicates whether pixel components are recorded in chunky or planar format: `1` (Chunky) or `2` (Planar)
ExifIFD0.ResolutionUnit|INTEGER|Resolution unit: `1` (No unit), `2` (inches) or `3` (cm)
ExifIFD0.TransferFunction[768]|INTEGER|Transfer function for the image, described in tabular style
ExifIFD0.Software|VARCHAR|Name and version of the software used to generate the image
ExifIFD0.DateTime|TIMESTAMP|Date and time of image creation
ExifIFD0.Artist|VARCHAR|Person who created the image
ExifIFD0.Predictor|INTEGER|Mathematical operator that is applied to the image data before an encoding scheme is applied: `1` (None) or `2` (Horizontal differencing)
ExifIFD0.WhitePoint[2]|DOUBLE|Chromaticity of the white point of the image
ExifIFD0.PrimaryChromaticities[6]|DOUBLE|Chromaticity of the three primary colors of the image
ExifIFD0.YCbCrCoefficients[3]|DOUBLE|Matrix coefficients for transformation from RGB to YCbCr image data
ExifIFD0.YCbCrSubSampling[2]|INTEGER|Sampling ratio of chrominance components in relation to the luminance component
ExifIFD0.YCbCrPositioning|INTEGER|Positioning of subsampled chrominance components relative to luminance samples: `1` (Centered) or `2` (Co-sited)
ExifIFD0.ReferenceBlackWhite[6]|DOUBLE|Pair of headroom and footroom image data values (codes) for each pixel component. [blackR, blackG, blackB, whiteR, whiteG, whiteB]
ExifIFD0.Copyright|VARCHAR|Copyright notice
ExifIFD0.InterColorProfile|VARBINARY|ICC profile information
ExifSubIFD.ExposureTime|DOUBLE|Exposure time in seconds
ExifSubIFD.FNumber|DOUBLE|F Number
ExifSubIFD.ExposureProgram|INTEGER|Class of the program used by the camera to set exposure when the picture is taken:<br>`1` (Manual control)<br>`2` (Program normal)<br>`3` (Aperture priority)<br>`4` (Shutter priority)<br>`5` (Program creative (slow program))<br>`6` (Program action (high-speed program))<br>`7` (Portrait mode)<br>`8` (Landscape mode)
ExifSubIFD.SpectralSensitivity|VARCHAR|Spectral sensitivity of each channel of the camera used
ExifSubIFD.ISOSpeedRatings|INTEGER|ISO Speed
ExifSubIFD.OptoElectricConversionFunction(OECF)|VARBINARY|Opto-Electric Conversion Function (OECF) specified in ISO 14524
ExifSubIFD.SensitivityType|INTEGER|Indicates which one of the parameters of ISO12232 is the PhotographicSensitivity tag:<br>`0` (Unknown)<br>`1` (Standard Output Sensitivity)<br>`2` (Recommended Exposure Index)<br>`3` (ISO Speed)<br>`4` (Standard Output Sensitivity and Recommended Exposure Index)<br>`5` (Standard Output Sensitivity and ISO Speed)<br>`6` (Recommended Exposure Index and ISO Speed)<br>`7` (Standard Output Sensitivity, Recommended Exposure Index and ISO Speed)
ExifSubIFD.StandardOutputSensitivity|INTEGER|Standard output sensitivity value of a camera or input device defined in ISO 12232
ExifSubIFD.RecommendedExposureIndex|INTEGER|Recommended exposure index value of a camera or input device defined in ISO 12232
ExifSubIFD.ISOSpeed|INTEGER|ISO speed value of a camera or input device that is defined in ISO 12232
ExifSubIFD.ISOSpeedLatitudeYyy|INTEGER|ISO speed latitude yyy value of a camera or input device that is defined in ISO 12232
ExifSubIFD.ISOSpeedLatitudeZzz|INTEGER|ISO speed latitude zzz value of a camera or input device that is defined in ISO 12232
ExifSubIFD.ExifVersion|VARCHAR|The version is given as a string "0231" to indicate version 2.31
ExifSubIFD.DateTimeOriginal|TIMESTAMP|Date and time when the original image data was generated
ExifSubIFD.DateTimeDigitized|TIMESTAMP|Date and time when the image was stored as digital data
ExifSubIFD.OffsetTime|VARCHAR|Offset from UTC (the time difference from Universal Time Coordinated including daylight saving time) of the time of DateTime tag
ExifSubIFD.OffsetTimeOriginal|VARCHAR|Offset from UTC (the time difference from Universal Time Coordinated including daylight saving time) of the time of DateTimeOriginal tag
ExifSubIFD.OffsetTimeDigitized|VARCHAR|Offset from UTC (the time difference from Universal Time Coordinated including daylight saving time) of the time of DateTimeDigitized tag
ExifSubIFD.ComponentsConfiguration[4]|INTEGER|Channels of each component are arranged in order from the 1st component to the 4th (specific to compressed data)
ExifSubIFD.CompressedBitsPerPixel|DOUBLE|Compression mode used for a compressed image is indicated in unit bits per pixel
ExifSubIFD.ShutterSpeedValue|DOUBLE|Shutter speed. The unit is the APEX (Additive System of Photographic Exposure) setting
ExifSubIFD.ApertureValue|DOUBLE|Lens aperture. The unit is the APEX value
ExifSubIFD.BrightnessValue|DOUBLE|Brightness. The unit is the APEX value
ExifSubIFD.ExposureBiasValue|DOUBLE|Exposure bias. The units is the APEX value
ExifSubIFD.MaxApertureValue|DOUBLE|Smallest F number of the lens. The unit is the APEX value
ExifSubIFD.SubjectDistance|DOUBLE|Distance to the subject in meters
ExifSubIFD.MeteringMode|INTEGER|Metering mode:<br>`0` (Unknown)<br>`1` (Average)<br>`2` (Center weighted average)<br>`3` (Spot)<br>`4` (Multi-spot)<br>`5` (Multi-segment)<br>`6` (Partial)<br>`255` (Other)
ExifSubIFD.WhiteBalance|INTEGER|White balance:<br>`0` (Unknown)<br>`1` (Daylight)<br>`2` (Florescent)<br>`3` (Tungsten)<br>`4` (Flash)<br>`9` (Fine Weather)<br>`10` (Cloudy)<br>`11` (Shade)<br>`12` (Daylight Fluorescent)<br>`13` (Day White Fluorescent)<br>`14` (Cool White Fluorescent)<br>`15` (White Fluorescent)<br>`16` (Warm White Fluorescent)<br>`17` (Standard light)<br>`18` (Standard light (B))<br>`19` (Standard light (C))<br>`20` (D55)<br>`21` (D65)<br>`22` (D75)<br>`23` (D50)<br>`24` (Studio Tungsten)<br>`255` (Other)
ExifSubIFD.Flash|INTEGER|Flash mode:<br>`0x0` (No Flash)<br>`0x1`	(Fired)<br>`0x5`	(Fired, Return not detected)<br>`0x7`	(Fired, Return detected)<br>`0x8`	(On, Did not fire)<br>`0x9`	(On, Fired)<br>`0xd`	(On, Return not detected)<br>`0xf`	(On, Return detected)<br>`0x10` (Off, Did not fire)<br>`0x14` (Off, Did not fire, Return not detected)<br>`0x18` (Auto, Did not fire)<br>`0x19` (Auto, Fired)<br>`0x1d` (Auto, Fired, Return not detected)<br>`0x1f` (Auto, Fired, Return detected)<br>`0x20` (No flash function)<br>`0x30` (Off, No flash function)<br>`0x41` (Fired, Red-eye reduction)<br>`0x45` (Fired, Red-eye reduction, Return not detected)<br>`0x47` (Fired, Red-eye reduction, Return detected)<br>`0x49` (On, Red-eye reduction)<br>`0x4d` (On, Red-eye reduction, Return not detected)<br>`0x4f` (On, Red-eye reduction, Return detected)<br>`0x50` (Off, Red-eye reduction)<br>`0x58` (Auto, Did not fire, Red-eye reduction)<br>`0x59` (Auto, Fired, Red-eye reduction)<br>`0x5d` (Auto, Fired, Red-eye reduction, Return not detected)<br>`0x5f` (Auto, Fired, Red-eye reduction, Return detected)
ExifSubIFD.FocalLength|DOUBLE|Actual focal length of the lens in mm
ExifSubIFD.UserComment|VARBINARY|Keywords or comments on the image besides those in ImageDescription, and without the character code limitations of the ImageDescription tag
ExifSubIFD.SubSecTime|VARCHAR|Fractions of seconds for the DateTime tag
ExifSubIFD.SubSecTimeOriginal|VARCHAR|Fractions of seconds for the DateTimeOriginal tag
ExifSubIFD.SubSecTimeDigitized|VARCHAR|Fractions of seconds for the DateTimeDigitized tag
ExifSubIFD.Temperature|DOUBLE|Temperature as the ambient situation at the shot. The unit is °C
ExifSubIFD.Humidity|DOUBLE|Humidity as the ambient situation at the shot. The unit is %
ExifSubIFD.Pressure|DOUBLE|Pressure as the ambient situation at the shot. The unit is hPa
ExifSubIFD.WaterDepth|DOUBLE|Water depth as the ambient situation at the shot. The unit is m
ExifSubIFD.Acceleration|DOUBLE|Acceleration (a scalar regardless of direction) as the ambient situation at the shot. The unit is mGal (10<sup>-5</sup> m/s<sup>2</sup>)
ExifSubIFD.CameraElevationAngle|DOUBLE|Elevation/depression angle of the orientation of the camera (imaging optical axis) as the ambient situation at the shot. The unit is degree(°)
ExifSubIFD.FlashPixVersion|VARCHAR|The version is given as a string "0100" to indicate version 1.0
ExifSubIFD.ColorSpace|INTEGER|Color space: `1` (sRGB) or `65535` (Undefined)
ExifSubIFD.ExifImageWidth|INTEGER|Valid width of the meaningful image (specific to compressed data)
ExifSubIFD.ExifImageHeight|INTEGER|Valid height of the meaningful image (specific to compressed data)
ExifSubIFD.RelatedSoundFile|VARCHAR|Name of an audio file related to the image data
ExifSubIFD.FlashEnergy|DOUBLE|Strobe energy at the time the image is captured, as measured in Beam Candle Power Seconds (BCPS)
ExifSubIFD.SpatialFrequencyResponse|VARBINARY|Camera or input device spatial frequency table and SFR values in the direction of image width, image height, and diagonal direction, as specified in ISO 12233
ExifSubIFD.FocalPlaneXResolution|DOUBLE|Number of pixels in the image width direction per FocalPlaneResolutionUnit on the camera focal plane
ExifSubIFD.FocalPlaneYResolution|DOUBLE|Number of pixels in the image height direction per FocalPlaneResolutionUnit on the camera focal plane
ExifSubIFD.FocalPlaneResolutionUnit|INTEGER|Unit for measuring FocalPlaneXResolution and FocalPlaneYResolution
ExifSubIFD.SubjectLocation[]|INTEGER|Location of the main subject in the scene
ExifSubIFD.ExposureIndex|DOUBLE|Exposure index selected on the camera or input device at the time the image is captured
ExifSubIFD.SensingMethod|INTEGER|Image sensor type:<br>`1` (Not defined)<br>`2` (One-chip color area sensor)<br>`3` (Two-chip color area sensor)<br>`4` (Three-chip color area sensor)<br>`5` (Color sequential area sensor)<br>`7` (Trilinear sensor)<br>`8` (Color sequential linear sensor
ExifSubIFD.FileSource|INTEGER|Image source: `1` (Film Scanner), `2` (Reflection Print Scanner) or `3` (Digital Still Camera (DSC))
ExifSubIFD.SceneType|INTEGER|Type of scene: `1` (Directly photographed image)
ExifSubIFD.CFAPattern|VARBINARY|Color filter array (CFA) geometric pattern of the image sensor when a one-chip color area sensor is used
ExifSubIFD.CustomRendered|INTEGER|Special processing on image data, such as rendering geared to output: `0` (Normal process) or `1` (Custom process)
ExifSubIFD.ExposureMode|INTEGER|Exposure mode set when the image was shot: `0` (Auto exposure), `1`(Manual exposure) or `2` (Auto bracket)
ExifSubIFD.WhiteBalanceMode|INTEGER|White balance mode set when the image was shot: `0` (Auto white balance) or `1` (Manual white balance)
ExifSubIFD.DigitalZoomRatio|DOUBLE|Digital zoom ratio when the image was shot
ExifSubIFD.FocalLength35|INTEGER|Equivalent focal length assuming a 35mm film camera in mm
ExifSubIFD.SceneCaptureType|INTEGER|Type of scene that was shot: `0` (Standard), `1` (Landscape), `2` (Portrait) or `3` (Night scene)
ExifSubIFD.GainControl|INTEGER|Degree of overall image gain adjustment: `0` (None), `1` (Low gain up), `2` (Low gain down), `3` (High gain up) or `4` (High gain down)
ExifSubIFD.Contrast|INTEGER|Direction of contrast processing applied by the camera when the image was shot: `0` (None), `1` (Soft) or `2` (Hard)
ExifSubIFD.Saturation|INTEGER|Direction of saturation processing applied by the camera when the image was shot: `0` (None), `1` (Low saturation) or `2` (High saturation)
ExifSubIFD.Sharpness|INTEGER|Direction of sharpness processing applied by the camera when the image was shot: `0` (None), `1` (Low) or `2` (Hard)
ExifSubIFD.DeviceSettingDescription|VARBINARY|Picture-taking conditions of a particular camera model
ExifSubIFD.SubjectDistanceRange|INTEGER|distance to the subject: `0` (Unknown), `1` (Macro), `2` (Close view) or `3` (Distant view)
ExifSubIFD.UniqueImageID|VARCHAR|Identifier assigned uniquely to each image
ExifSubIFD.CameraOwnerName|VARCHAR|Owner of a camera used in photography
ExifSubIFD.BodySerialNumber|VARCHAR|Serial number of the body of the camera that was used in photography
ExifSubIFD.LensSpecification[4]|DOUBLE|Minimum focal length, maximum focal length, minimum F number in the minimum focal length, and minimum F number in the maximum focal length, which are specification information for the lens that was used in photography
ExifSubIFD.LensMake|VARCHAR|Lens manufacturer
ExifSubIFD.LensModel|VARCHAR|Lens’s model name and model number
ExifSubIFD.LensSerialNumber|VARCHAR|Serial number of the interchangeable lens that was used in photography
ExifSubIFD.Gamma|DOUBLE|Value of coefficient gamma
Interoperability.InteroperabilityIndex|VARCHAR|Identification of the Interoperability rule:<br>`R98` (DCF basic file)<br>`THM` (DCF thumbnail file)<br>`R03` (DCF option file)
Interoperability.InteroperabilityVersion|VARCHAR|The version is given as a string "0100" to indicate version 1.0
GPS.GPSVersionID[4]|INTEGER|The version is given as an integer array [2, 2, 0, 0] to indicate version 2.2
GPS.GPSLatitudeRef|VARCHAR|Indicates whether the latitude is north or south latitude: `N` (North) or `S` (South)
GPS.GPSLatitude|DOUBLE|Latitude in degrees
GPS.GPSLongitudeRef|VARCHAR|Indicates whether the longitude is east or west longitude: `E` (East) or `W` (West)
GPS.GPSLongitude|DOUBLE|Longitude in degrees
GPS.GPSAltitudeRef|INTEGER|Reference altitude: `0` (Above Sea Level) or `1` (Below Sea Level)
GPS.GPSAltitude|DOUBLE|Altitude in meters
GPS.GPSTimeStamp[3]|DOUBLE|Time as UTC. [hour, minute, second]
GPS.GPSSatellites|VARCHAR|GPS satellites used for measurements
GPS.GPSStatus|VARCHAR|Status of the GPS receiver: `A` (Measurement in progress) or `V` (Measurement interrupted)
GPS.GPSMeasureMode|VARCHAR|GPS measurement mode: `2` (2-dimensional measurement) or `3` (3-dimensional measurement)
GPS.GPSDOP|DOUBLE|GPS DOP (data degree of precision)
GPS.GPSSpeedRef|VARCHAR|Unit used to express the GPS receiver speed of movement: `K` (kilometers per hour), `M` (miles per hour) or `N` (knots)
GPS.GPSSpeed|DOUBLE|Speed of GPS receiver movement
GPS.GPSTrackRef|VARCHAR|Reference for giving the direction of GPS receiver movement: `T` (True direction) or `M` (Magnetic direction)
GPS.GPSTrack|DOUBLE|Direction of GPS receiver movement in degrees
GPS.GPSImgDirectionRef|VARCHAR|Reference for giving the direction of the image: `T` (True direction) or `M` (Magnetic direction)
GPS.GPSImgDirection|DOUBLE|Direction of the image in degrees
GPS.GPSMapDatum|VARCHAR|Geodetic survey data used by the GPS receiver
GPS.GPSDestLatitudeRef|VARCHAR|Indicates whether the latitude of the destination point is north or south latitude: `N` (North) or `S` (South)
GPS.GPSDestLatitude[3]|DOUBLE|Latitude of the destination point in degrees
GPS.GPSDestLongitudeRef|VARCHAR|Indicates whether the longitude of the destination point is east or west longitude: `E` (East) or `W` (West)
GPS.GPSDestLongitude[3]|DOUBLE|longitude of the destination point in degrees
GPS.GPSDestBearingRef|VARCHAR|Reference used for giving the bearing to the destination point: `T` (True direction) or `M` (Magnetic direction)
GPS.GPSDestBearing|DOUBLE|Bearing to the destination point in degrees
GPS.GPSDestDistanceRef|VARCHAR|Unit used to express the distance to the destination point: `K` (kilometers), `M` (miles) or `N` (knots)
GPS.GPSDestDistance|DOUBLE|Distance to the destination point
GPS.GPSProcessingMethod|VARBINARY|Name of the method used for location finding
GPS.GPSAreaInformation|VARBINARY|Name of the GPS area
GPS.GPSDateStamp|VARCHAR|Date as UTC. The format is "YYYY:MM:DD"
GPS.GPSDifferential|INTEGER|Indicates whether differential correction is applied to the GPS receiver:<br>`0` (Measurement without differential correction)<br>`1` (Differential correction applied)
GPS.GPSHPositioningError|DOUBLE|Horizontal positioning errors in meters
ExifThumbnail.Compression|INTEGER|Compression scheme used for the image data:<br>`1` (Uncompressed)<br>`2` (CCITT 1D)<br>`3` (T4/Group 3 Fax)<br>`4` (T6/Group 4 Fax)<br>`5` (LZW)<br>`6` (JPEG (old-style))<br>`7` (JPEG)<br>`8` (Adobe Deflate)<br>`9` (JBIG B&W)<br>`10` (JBIG Color)<br>`99` (JPEG)<br>`262` (Kodak 262)<br>`32766` (Next)<br>`32767` (Sony ARW Compressed)<br>`32769` (Packed RAW)<br>`32770` (Samsung SRW Compressed)<br>`32771` (CCIRLEW)<br>`32772` (Samsung SRW Compressed 2)<br>`32773` (PackBits)<br>`32809` (Thunderscan)<br>`32867` (Kodak KDC Compressed)<br>`32895` (IT8CTPAD)<br>`32896` (IT8LW)<br>`32897` (IT8MP<br>`32898` (IT8BL)<br>`32908` (PixarFilm)<br>`32909` (PixarLog)<br>`32946` (Deflate)<br>`32947` (DCS)<br>`34661` (JBIG)<br>`34676` (SGILog)<br>`34677` (SGILog24)<br>`34712` (JPEG 2000)<br>`34713` (Nikon NEF Compressed)<br>`34715` (JBIG2 TIFF FX)<br>`34718` (Microsoft Document Imaging (MDI) Binary Level Codec)<br>`34719` (Microsoft Document Imaging (MDI) Progressive Transform Codec)<br>`34720` (Microsoft Document Imaging (MDI) Vector)<br>`34892` (Lossy JPEG)<br>`65000` (Kodak DCR Compressed)<br>`65535` (Pentax PEF Compressed)
ExifThumbnail.XResolution|DOUBLE|Horizontal pixel resolution
ExifThumbnail.YResolution|DOUBLE|Vertical pixel resolution
ExifThumbnail.ResolutionUnit|INTEGER|Resolution unit: `1` (None), `2` (inches) or `3` (cm)
ExifThumbnail.ThumbnailOffset|INTEGER|Position in the file of thumbnail data
ExifThumbnail.ThumbnailLength|INTEGER|Length of thumbnail data
XMP.XMPValueCount|INTEGER|Number of properties in the XMP property tree
XMP.Dc.Contributors[]|VARCHAR|List of contributors
XMP.Dc.Coverage|VARCHAR|Extent or scope of the resource
XMP.Dc.Creator[]|VARCHAR|List of creators
XMP.Dc.Date|VARCHAR|Point or period of time associated with an event in the life cycle of the resource
XMP.Dc.Description|VARCHAR|Textual descriptions of the content of the resource
XMP.Dc.Description_&lt;lang&gt;|VARCHAR|Textual descriptions of the content of the resource, given in various languages
XMP.Dc.Format|VARCHAR|MIME type of the resource
XMP.Dc.Identifier|VARCHAR|Unambiguous reference to the resource within a given context
XMP.Dc.Language[]|VARCHAR|List of languages used in the content of the resource
XMP.Dc.Publisher[]|VARCHAR|List of publishers
XMP.Dc.Relation[]|VARCHAR|List of related resources
XMP.Dc.Rights|VARCHAR|List of informal rights statements
XMP.Dc.Rights_&lt;lang&gt;|VARCHAR|List of informal rights statements, given in various languages
XMP.Dc.Source|VARCHAR|Related resource from which the described resource is derived
XMP.Dc.Subject[]|VARCHAR|List of descriptive phrases or keywords that specify the content of the resource
XMP.Dc.Title|VARCHAR|Title or name given to the resource
XMP.Dc.Title_&lt;lang&gt;|VARCHAR|Title or name, given in various languages
XMP.Dc.Type[]|VARCHAR|Nature or genre of the resource
XMP.Xmp.Advisory[]|VARCHAR|Unordered array specifying properties that were edited outside the authoring application
XMP.Xmp.BaseURL|VARCHAR|Base URL for relative URLs in the document content
XMP.Xmp.CreateDate|VARCHAR|Date and time the resource was created
XMP.Xmp.CreatorTool|VARCHAR|Name of the first known tool used to create the resource
XMP.Xmp.Identifier[]|VARCHAR|Unordered array of text strings that unambiguously identify the resource within a given context
XMP.Xmp.Label|VARCHAR|Word or short phrase that identifies a resource as a member of a user-defined collection
XMP.Xmp.MetadataDate|VARCHAR|Date and time that any metadata for this resource was last changed
XMP.Xmp.ModifyDate|VARCHAR|Date and time the resource was last modified
XMP.Xmp.Nickname|VARCHAR|Short informal name for the resource
XMP.Xmp.Rating|VARCHAR|User-assigned rating for this file
XMP.XmpRights.Certificate|VARCHAR|Web URL for a rights management certificate
XMP.XmpRights.Marked|VARCHAR|When `true`, indicates that this is a rights-managed resource. When `false`, indicates that this is a public-domain resource. null if the state is unknown
XMP.XmpRights.Owner|VARCHAR|List of legal owners of the resource
XMP.XmpRights.UsageTerms|VARCHAR|Text instructions on how a resource can be legally used
XMP.XmpRights.UsageTerms_&lt;lang&gt;|VARCHAR|Collection of text instructions on how a resource can be legally used, given in a variety of languages
XMP.XmpRights.WebStatement|VARCHAR|Web URL for a statement of the ownership and usage rights for this resource
XMP.XmpMM.DerivedFrom.DocumentID|VARCHAR|Value of the xmpMM:DocumentID property from the referenced resource
XMP.XmpMM.DerivedFrom.FilePath|VARCHAR|Referenced resource’s file path or URL
XMP.XmpMM.DerivedFrom.InstanceID|VARCHAR|Value of the xmpMM:InstanceID property from the referenced resource
XMP.XmpMM.DerivedFrom.RenditionClass|VARCHAR|Value of the xmpMM:RenditionClass property from the referenced resource
XMP.XmpMM.DerivedFrom.RenditionParams|VARCHAR|value of the xmpMM:RenditionParams property from the referenced resource
XMP.XmpMM.DocumentID|VARCHAR|Common identifier for all versions and renditions of a resource
XMP.XmpMM:History[].Action|VARCHAR|Action that occurred
XMP.XmpMM:History[].Changed|VARCHAR|Semicolon-delimited list of the parts of the document that were changed since the previous event history
XMP.XmpMM:History[].InstanceID|VARCHAR|Instance ID of the modified resource
XMP.XmpMM:History[].Parameters|VARCHAR|Additional description of the action
XMP.XmpMM:History[].SoftwareAgent|VARCHAR|Software agent that performed the action
XMP.XmpMM:History[].When|VARCHAR|Timestamp of when the action occurred
XMP.XmpMM.InstanceID|VARCHAR|Identifier for a specific incarnation of a resource, updated each time a file is saved
XMP.XmpMM.Manager|VARCHAR|Name of the asset management system that manages this resource
XMP.XmpMM.ManageTo|VARCHAR|URI identifying the managed resource to the asset management system
XMP.XmpMM.ManageUI|VARCHAR|URI that can be used to access information about the managed resource through a web browser
XMP.XmpMM.ManagerVariant|VARCHAR|Particular variant of the asset management system
XMP.XmpMM.OriginalDocumentID|VARCHAR|Common identifier for the original resource from which the current resource is derived
XMP.XmpMM.RenditionClass|VARCHAR|Rendition class name for this resource
XMP.XmpMM.RenditionParams|VARCHAR|Additional rendition parameters that are too complex or verbose to encode in xmpMM:RenditionClass
XMP.XmpMM.VersionID|VARCHAR|Document version identifier for this resource
XMP.XmpMM.Versions[].Comments|VARCHAR|Comments concerning what was changed
XMP.XmpMM.Versions[].Modifier|VARCHAR|Person who modified this version
XMP.XmpMM.Versions[].ModifyDate|VARCHAR|Date on which this version was checked in
XMP.XmpMM.Versions[].Version|VARCHAR|New version number
XMP.Xmpidq.Scheme|VARCHAR|Qualifier providing the name of the formal identification scheme used for an item in the xmp:Identifier array
XMP.XmpBJ.JobRef[].Id|VARCHAR|Unique ID for the job
XMP.XmpBJ.JobRef[].Name|VARCHAR|Informal name of job
XMP.XmpBJ.JobRef[].Url|VARCHAR|File URL referencing an external job management file
XMP.Photoshop.AuthorsPosition|VARCHAR|By-line title
XMP.Photoshop.CaptionWriter|VARCHAR|Writer/editor
XMP.Photoshop.Category|VARCHAR|Category. Limited to 3 7-bit ASCII characters
XMP.Photoshop.City|VARCHAR|City
XMP.Photoshop.ColorMode|VARCHAR|
XMP.Photoshop.Country|VARCHAR|Country/primary location
XMP.Photoshop.Credit|VARCHAR|Credit
XMP.Photoshop.DateCreated|VARCHAR|Date the intellectual content of the document was created
XMP.Photoshop.Headline|VARCHAR|Headline
XMP.Photoshop.History|VARCHAR|
XMP.Photoshop.ICCProfile|VARCHAR|
XMP.Photoshop.Source|VARCHAR|Source
XMP.Photoshop.State|VARCHAR|Province/state
XMP.Photoshop.SupplementalCategories[]|VARCHAR|Supplemental category
XMP.Photoshop.Urgency|VARCHAR|Urgency. Valid range is 1-8
XMP.Tiff.Orientation|VARCHAR|Orientation:<br>`1` (0th row at top, 0th column at left)<br>`2` (0th row at top, 0th column at right)<br>`3` (0th row at bottom, 0th column at right)<br>`4` (0th row at bottom, 0th column at left)<br>`5` (0th row at left, 0th column at top)<br>`6` (0th row at right, 0th column at top)<br>`7` (0th row at right, 0th column at bottom)<br>`8` (0th row at left, 0th column at bottom)
XMP.Tiff.XResolution|VARCHAR|Horizontal resolution in pixels per unit
XMP.Tiff.YResolution|VARCHAR|Vertical resolution in pixels per unit
XMP.Tiff.ResolutionUnit|VARCHAR|Unit used for XResolution and YResolution: `2` (Inches) or `3` (Centimeters)
XMP.Tiff.NativeDigest|VARCHAR|Digest to detect changes to the TIFF tags
XMP.Exif.ColorSpace|VARCHAR|Color space information: `1` (sRGB) or `65535` (Uncalibrated)
XMP.Exif.PixelXDimension|VARCHAR|Valid image width in pixels
XMP.Exif.PixelYDimension|VARCHAR|Valid image height in pixels
XMP.Exif.NativeDigest|VARCHAR|Digest to detect changes to the Exif tags
ICCProfile.ProfileSize|INTEGER|Total size of the profile in bytes
ICCProfile.CMMType|VARCHAR|Identifies the preferred CMM to be used
ICCProfile.Version|INTEGER|Profile version number where the first 8 bits are the major version number and the next 8 bits are for the minor version number
ICCProfile.Class|VARCHAR|Class of device profile:<br>`scnr` (Input device)<br>`mntr` (Display device)<br>`prtr` (Output device)<br>`link` (Device link)<br> `spac` (Color space conversion)<br>`abst` (Abstract)<br> `nmcl` (Named color)
ICCProfile.ColorSpace|VARCHAR|Color space of data:<br><code>XYZ&nbsp;</code> (XYZ)<br><code>Lab&nbsp;</code> (Lab)<br><code>Luv&nbsp;</code> (Luv)<br>`YCbr` (YCbCr)<br><code>Yxy&nbsp;</code> (Yxy)<br><code>RGB&nbsp;</code> (RGB)<br>`GRAY` (Grayscale)<br><code>HSV&nbsp;</code> (HSV)<br><code>HLS&nbsp;</code> (HLS)<br>`CMYK` (CMYK)<br><code>CMY&nbsp;</code> (CMY)
ICCProfile.ProfileConnectionSpace|VARCHAR|Profile connection space:<br><code>XYZ&nbsp;</code> (XYZ)<br><code>Lab&nbsp;</code> (Lab)
ICCProfile.ProfileDateTime|TIMESTAMP|Date and time this profile was first created
ICCProfile.Signature|VARCHAR|Profile file signature : `acsp`
ICCProfile.PrimaryPlatform|VARCHAR|Primary platform/operating system framework for which the profile was created:<br>`APPL` (Apple Computer, Inc.)<br>`MSFT` (Microsoft Corporation)<br><code>SGI&nbsp;</code> (Silicon Graphics, Inc.)<br>`SUNW` (Sun Microsystems, Inc.)<br>`TGNT` (Taligent, Inc.)
ICCProfile.DeviceManufacturer|VARCHAR|Device manufacturer of the device for which this profile is created
ICCProfile.DeviceModel|VARCHAR|Device model of the device for which this profile is created
ICCProfile.RenderingIntent|INTEGER|Style of reproduction to be used during the evaluation of this profile in a sequence of profiles:<br>`0` (Perceptual)<br>`1` (Relative colorimetric)<br>`2` (Saturation)<br>`3` (Absolute colorimetric)
ICCProfile.XYZValues[3]|FLOAT|XYZ values of the illuminant of the profile connection space
ICCProfile.TagCount|INTEGER|Number of tags
ICCProfile.Copyright|VARCHAR|Profile copyright information
ICCProfile.DeviceMfgDescription|VARCHAR|Description of device manufacturer
ICCProfile.DeviceModelDescription|VARCHAR|Description of device model
ICCProfile.ProfileDescription|VARCHAR|Profile description
ICCProfile.AppleMultiLanguageProfileName|VARCHAR|Multi-language profile name
ICCProfile.MediaWhitePoint|VARCHAR|Media white point used for generating absolute colorimetry
ICCProfile.MediaBlackPoint|VARCHAR|Media black point used for generating absolute colorimetry
ICCProfile.RedTRC|VARCHAR|Red channel tone reproduction curve
ICCProfile.GreenTRC|VARCHAR|Green channel tone reproduction curve
ICCProfile.BlueTRC|VARCHAR|Blue channel tone reproduction curve
ICCProfile.RedColorant|VARCHAR|Relative XYZ values of red phosphor or colorant
ICCProfile.GreenColorant|VARCHAR|Relative XYZ values of green phosphor or colorant
ICCProfile.BlueColorant|VARCHAR|Relative XYZ values of blue phosphor or colorant
JPEG.CompressionType|INTEGER|Compression type:<br>`0` (Baseline DCT, Huffman coding)<br>`1` (Extended sequential DCT, Huffman coding)<br>`2` (Progressive DCT, Huffman coding)<br>`3` (Lossless, Huffman coding)<br>`5` (Sequential DCT, differential Huffman coding)<br>`6` (Progressive DCT, differential Huffman coding<br>`7` (Lossless, Differential Huffman coding)<br>`9` (Extended sequential DCT, arithmetic coding)<br>`10` (Progressive DCT, arithmetic coding)<br>`11` (Lossless, arithmetic coding)<br>`13` (Sequential DCT, differential arithmetic coding)<br>`14` (Progressive DCT, differential arithmetic coding)<br>`15` (Lossless, differential arithmetic coding)
JPEG.DataPrecision|INTEGER|Bits per sample
JPEG.ImageHeight|INTEGER|Image height in pixels
JPEG.ImageWidth|INTEGER|Image width in pixels
JPEG.NumberOfComponents|INTEGER|Number of components: `1` (Greyscale), `3` (YCbCr or YIQ) or `4` (CMYK)
JPEG.Component1.ComponentId|INTEGER|Component identifier: `1` (Y), `2` (Cb), `3` (Cr), `4` (I) or `5` (Q)
JPEG.Component1.HorizontalSamplingFactor|INTEGER|Horizontal sampling factors
JPEG.Component1.VerticalSamplingFactor|INTEGER|Vertical sampling factors
JPEG.Component1.QuantizationTableNumber|INTEGER|Quantization table number
JPEG.Component2.ComponentId|INTEGER|Component identifier: `1` (Y), `2` (Cb), `3` (Cr), `4` (I) or `5` (Q)
JPEG.Component2.HorizontalSamplingFactor|INTEGER|Horizontal sampling factors
JPEG.Component2.VerticalSamplingFactor|INTEGER|Vertical sampling factors
JPEG.Component2.QuantizationTableNumber|INTEGER|Quantization table number
JPEG.Component3.ComponentId|INTEGER|Component identifier: `1` (Y), `2` (Cb), `3` (Cr), `4` (I) or `5` (Q)
JPEG.Component3.HorizontalSamplingFactor|INTEGER|Horizontal sampling factors
JPEG.Component3.VerticalSamplingFactor|INTEGER|Vertical sampling factors
JPEG.Component3.QuantizationTableNumber|INTEGER|Quantization table number
JPEG.Component4.ComponentId|INTEGER|Component identifier: `1` (Y), `2` (Cb), `3` (Cr), `4` (I) or `5` (Q)
JPEG.Component4.HorizontalSamplingFactor|INTEGER|Horizontal sampling factors
JPEG.Component4.VerticalSamplingFactor|INTEGER|Vertical sampling factors
JPEG.Component4.QuantizationTableNumber|INTEGER|Quantization table number
IPTC.EnvelopedRecordVersion|INTEGER|Version of the Information Interchange Model, Part I, utilised by the provider.
IPTC.Destination|VARCHAR|Providers who require routing information above the appropriate OSI layers
IPTC.FileFormat|VARCHAR|File format:<br>`00` (No ObjectData)<br>`01` (IPTC-NAA Digital Newsphoto Parameter Record)<br>`02` (IPTC7901 Recommended Message Format)<br>`03` (Tagged Image File Format (Adobe/Aldus Image data))<br>`04` (Illustrator (Adobe Graphics data))<br>`05` (AppleSingle (Apple Computer Inc))<br>`06` (NAA 89-3 (ANPA 1312))<br>`07` (MacBinary II)<br>`08` (IPTC Unstructured Character Oriented File Format (UCOFF))<br>`09` (United Press International ANPA 1312 variant)<br>`10` (United Press International Down-Load Message)<br>`11` (JPEG File Interchange (JFIF))<br>`12` (Photo-CD Image-Pac (Eastman Kodak))<br>`13` (Microsoft Bit Mapped Graphics File [\*.BMP])<br>`14` (Digital Audio File [\*.WAV] (Microsoft & Creative Labs))<br>`15` (Audio plus Moving Video [\*.AVI] (Microsoft))<br>`16` (PC DOS/Windows Executable Files [\*.COM][\*.EXE])<br>`17` (Compressed Binary File [\*.ZIP] (PKWare Inc))<br>`18` (Audio Interchange File Format AIFF (Apple Computer Inc))<br>`19` (RIFF Wave (Microsoft Corporation))<br>`20` (Freehand (Macromedia/Aldus))<br>`21` (Hypertext Markup Language "HTML" (The Internet Society))<br>`22` (MPEG 2 Audio Layer 2 (Musicom), ISO/IEC)<br>`23` (MPEG 2 Audio Layer 3, ISO/IEC)<br>`24` (Portable Document File (\*.PDF) Adobe)<br>`25` (News Industry Text Format (NITF))<br>`26` (Tape Archive (\*.TAR))<br>`27` (Tidningarnas Telegrambyrå NITF version (TTNITF DTD))<br>`28` (Ritzaus Bureau NITF version (RBNITF DTD))<br>`29` (Corel Draw [\*.CDR])
IPTC.FileVersion|INTEGER|Particular version of the File Format
IPTC.ServiceIdentifier|VARCHAR|Provider and product
IPTC.EnvelopeNumber|VARCHAR|Number that will be unique for the date and for the Service Identifier
IPTC.ProductIdentifier|VARCHAR|Identifies subsets of provider's overall service
IPTC.EnvelopePriority|VARCHAR|Envelope handling priority and not the editorial urgency. `1` indicates the most urgent, `5` the normal urgency, and `8` the least urgent copy. `9` indicates a User Defined Priority
IPTC.DateSent|VARCHAR|Year, month and day the service sent the material in the form CCYYMMDD (century, year, month, day) as defined in ISO 8601
IPTC.TimeSent|VARCHAR|Time the service sent the material in the form HHMMSS±HHMM where HHMMSS refers to local hour, minute and seconds and HHMM refers to hours and minutes ahead (+) or behind (-) Universal Coordinated Time as described in ISO 8601
IPTC.CodedCharacterSet|VARCHAR|One or more control functions used for the announcement, invocation or designation of coded character sets
IPTC.UniqueObjectName|VARCHAR|Unique Name of Object, providing eternal, globally unique identification for objects as specified in the IIM, independent of provider and for any media form
IPTC.ARMIdentifier|VARCHAR|Identifies the Abstract Relationship Method (ARM)
IPTC.ARMVersion|INTEGER|Version of the ARM
IPTC.ApplicationRecordVersion|INTEGER|Version of the Information Interchange Model, Part II
IPTC.ObjectAttributeReference|VARCHAR|Nature, intellectual, artistic or journalistic characteristic of an image
IPTC.ObjectName|VARCHAR|Shorthand reference for the object
IPTC.SubjectReference[]|VARCHAR|Specifies one or more Subjects from the IPTC Subject-NewsCodes taxonomy to categorise the image
IPTC.Keywords[]|VARCHAR|Specific information retrieval words
IPTC.SpecialInstructions|VARCHAR|Other editorial instructions concerning the use of the objectdata
IPTC.DateCreated|VARCHAR|Date the intellectual content of the objectdata was created in the form CCYYMMDD
IPTC.TimeCreated|VARCHAR|Time the intellectual content of the objectdata current source material was created in the form HHMMSS±HHMM
IPTC.DigitalDateCreated|VARCHAR|Date the digital representation of the objectdata was created in the form CCYYMMDD
IPTC.DigitalTimeCreated|VARCHAR|Time the digital representation of the objectdata was created in the form HHMMSS±HHMM
IPTC.ByLine|VARCHAR|Name of the creator of the objectdata
IPTC.BylineTitle|VARCHAR|Job title of the photographer
IPTC.City|VARCHAR|Name of the city of the location shown in the image
IPTC.SubLocation|VARCHAR|Exact name of the sublocation shown in the image
IPTC.ProvinceState|VARCHAR|Name of the subregion of a country of the location shown in the image
IPTC.CountryPrimaryLocationCode|VARCHAR|Code of the country of the location shown in the image
IPTC.CountryPrimaryLocationName|VARCHAR|Full name of the country of the location shown in the image
IPTC.OriginalTransmissionReference|VARCHAR|Number or identifier for the purpose of improved workflow handling
IPTC.CopyrightNotice|VARCHAR|Copyright notice for claiming the intellectual property for this photograph
IPTC.Headline|VARCHAR|Brief synopsis of the caption
IPTC.Credit|VARCHAR|Credit to person(s) and/or organisation(s) required by the supplier of the image to be used when published
IPTC.Source|VARCHAR|Name of a person or party who has a role in the content supply chain
IPTC.CaptionAbstract|VARCHAR|Textual description of the objectdata
IPTC.CaptionWriterEditor|VARCHAR|Name of the person involved in the writing, editing or correcting the objectdata or caption/abstract
IPTC.RasterizedCaption|VARCHAR|Rasterized objectdata description and is used where characters that have not been coded are required for the caption
IPTC.ImageType|VARCHAR|Octet 1 indicates number of components in an image:<br>`0` (No objectdata)<br>`1` (Single component)<br>`2` (2 components)<br>`3` (3 components)<br>`4` (4 components)<br>`9` (Supplemental objects related to other objectdata)<br>Octet 2 indicates the exact content of the current objectdata in terms of colour composition:<br>`W` (Monochrome)<br>`Y` (Yellow component)<br>`M` (Magenta component)<br>`C` (Cyan component)<br>`K` (Black component)<br>`R` (Red component)<br>`G` (Green component)<br>`B` (Blue component)<br>`T` (Text only)<br>`F` (Full colour composite, frame sequential)<br>`L` (Full colour composite, line sequential)<br>`P` (Full colour composite, pixel sequential)<br>`S` (Full colour composite, special interleaving)
IPTC.ImageOrientation|VARCHAR|Layout of the image area: `P` (Portrait), `L` (Landscape) or `S` (Square)
IPTC.LanguageIdentifier|VARCHAR|Major national language of the object, according to the 2-letter codes of ISO 639:1988
IPTC.AudioType|VARCHAR|Octet 1 represents the number of channels:<br>`0` (No objectdata)<br>`1` (Monaural (1 channel) audio)<br>`2` (Stereo (2 channel) audio)<br>Octet 2 indicates the exact type of audio contained in the current objectdata:<br>`A` (Actuality)<br>`C` (Question and answer session)<br>`M` (Music, transmitted by itself)<br>`Q` (Response to a question)<br>`R` (Raw sound)<br>`S` (Scener)<br>`T` (Text only)<br>`V` (Voicer)<br>`W` (Wrap)
IPTC.AudioSamplingRate|VARCHAR|Six octets with leading zero(s), consisting of Sampling rate numeric characters, representing the sampling rate in hertz (Hz)
IPTC.AudioSamplingResolution|VARCHAR|Two octets with leading zero(s), consisting of resolution numeric characters representing the number of bits in each audio sample
IPTC.AudioDuration|VARCHAR|Duration Designates in the form HHMMSS the running time of an audio objectdata when played back at the speed at which it was recorded
IPTC.AudioOutcue|VARCHAR|Content of the end of an audio objectdata, according to guidelines established by the provider
IPTC.ObjectDataPreviewFileFormat|VARCHAR|File format of the ObjectData Preview:<br>`00` (No ObjectData)<br>`01` (IPTC-NAA Digital Newsphoto Parameter Record)<br>`02` (IPTC7901 Recommended Message Format)<br>`03` (Tagged Image File Format (Adobe/Aldus Image data))<br>`04` (Illustrator (Adobe Graphics data))<br>`05` (AppleSingle (Apple Computer Inc))<br>`06` (NAA 89-3 (ANPA 1312))<br>`07` (MacBinary II)<br>`08` (IPTC Unstructured Character Oriented File Format (UCOFF))<br>`09` (United Press International ANPA 1312 variant)<br>`10` (United Press International Down-Load Message)<br>`11` (JPEG File Interchange (JFIF))<br>`12` (Photo-CD Image-Pac (Eastman Kodak))<br>`13` (Microsoft Bit Mapped Graphics File [\*.BMP])<br>`14` (Digital Audio File [\*.WAV] (Microsoft & Creative Labs))<br>`15` (Audio plus Moving Video [\*.AVI] (Microsoft))<br>`16` (PC DOS/Windows Executable Files [\*.COM][\*.EXE])<br>`17` (Compressed Binary File [\*.ZIP] (PKWare Inc))<br>`18` (Audio Interchange File Format AIFF (Apple Computer Inc))<br>`19` (RIFF Wave (Microsoft Corporation))<br>`20` (Freehand (Macromedia/Aldus))<br>`21` (Hypertext Markup Language "HTML" (The Internet Society))<br>`22` (MPEG 2 Audio Layer 2 (Musicom), ISO/IEC)<br>`23` (MPEG 2 Audio Layer 3, ISO/IEC)<br>`24` (Portable Document File (\*.PDF) Adobe)<br>`25` (News Industry Text Format (NITF))<br>`26` (Tape Archive (\*.TAR))<br>`27` (Tidningarnas Telegrambyrå NITF version (TTNITF DTD))<br>`28` (Ritzaus Bureau NITF version (RBNITF DTD))<br>`29` (Corel Draw [\*.CDR])
Photoshop.ResolutionInfo|VARCHAR|ResolutionInfo structure
Photoshop.AlphaChannels|VARCHAR|Names of the alpha channels
Photoshop.DisplayInfo(Obsolete)|VARCHAR|DisplayInfo structure
Photoshop.PrintFlags|VARCHAR|Series of boolean values: Labels, Crop marks, Color bars, Registration marks, Negative, Flip, Interpolate, Caption, Print flags
Photoshop.ColorHalftoningInformation|VARCHAR|Color halftoning information
Photoshop.ColorTransferFunctions|VARCHAR|Color transfer functions
Photoshop.EPSOptions|VARCHAR|EPS options
Photoshop.LayerStateInformation|VARCHAR|Layer state information. Index of target layer (0 = bottom layer)
Photoshop.LayersGroupInformation|VARCHAR|Working path (not saved)
Photoshop.JPEGQuality|VARCHAR|JPEG quality
Photoshop.GridAndGuidesInformation|VARCHAR|Grid and guides information
Photoshop.CopyrightFlag|VARCHAR|Whether image is copyrighted
Photoshop.URL|VARCHAR|Uniform resource locator
Photoshop.ThumbnailData|VARCHAR|Photoshop 5.0 Thumbnail resource
Photoshop.GlobalAngle|VARCHAR|Global lighting angle for effects layer
Photoshop.ICCUntaggedProfile|VARCHAR|ICC Untagged Profile. 1 = intentionally untagged
Photoshop.SeedNumber|VARCHAR|Document-specific IDs seed number
Photoshop.UnicodeAlphaNames|VARCHAR|Unicode Alpha Names
Photoshop.GlobalAltitude|VARCHAR|Global Altitude
Photoshop.Slices|VARCHAR|Slices
Photoshop.AlphaIdentifiers|VARCHAR|Alpha Identifiers
Photoshop.URLList|VARCHAR|URL List. Count of URLs, followed by ID and Unicode string
Photoshop.VersionInfo|VARCHAR|Version Info. Version, hasRealMergedData, Writer name, Reader name and File version
Photoshop.CaptionDigest|VARCHAR|RSA Data Security, MD5 message-digest algorithm
Photoshop.PrintScale|VARCHAR|Print scale. Style: `0` (Centered), `1` (Size to fit) or `2` (User defined), X location, Y location and Scale
Photoshop.PixelAspectRatio|VARCHAR|Pixel Aspect Ratio. X / Y of a pixel
Photoshop.LayerSelectionIDs|VARCHAR|Layer Selection ID(s). Count and repeated layer ID
Photoshop.LayerGroupsEnabledID|VARCHAR|Layer Group(s) Enabled ID. 1 byte for each layer in the document, repeated by length of the resource
Photoshop.PrintInfo2|VARCHAR|Information about the current print settings in the document. Color management options
Photoshop.PlugIn\*Data|VARCHAR|Plug-in data
Photoshop.PrintStyle|VARCHAR|Information about the current print style in the document. Printing marks, labels, ornaments, etc.
Photoshop.PrintFlagsInformation|VARCHAR|Print flags information. Version: `1`, Center crop marks, `0`, Bleed width value and Bleed width scale
AdobeJPEG.DCTEncodeVersion|INTEGER|DCT encode version
AdobeJPEG.Flags0|INTEGER|APP14 Flags 0: `0` (None) or `32768` (Encoded with Blend=1 downsampling)
AdobeJPEG.Flags1|INTEGER|APP14 Flags 1: `0` (None)
AdobeJPEG.ColorTransform|INTEGER|Color transform: `0` (Unknown (RGB or CMYK)) , `1` (YCbCr) or `2` (YCCK)
Huffman.NumberOfTables|INTEGER|Number of Huffman tables
PSDHeader.ChannelCount|INTEGER|Number of channels
PSDHeader.ImageHeight|INTEGER|Image height in pixels
PSDHeader.ImageWidth|INTEGER|Image width in pixels
PSDHeader.BitsPerChannel|INTEGER|Bits per channel
PSDHeader.ColorMode|INTEGER|Color mode: `0` (Bitmap), `1` (Grayscale), `2` (Indexed), `3` (RGB), `4` (CMYK), `7` (Multichannel), `8` (Duotone) or `9` (Lab)
PNGIHDR.ImageWidth|INTEGER|Image width in pixels
PNGIHDR.ImageHeight|INTEGER|Image height in pixels
PNGIHDR.BitsPerSample|INTEGER|Number of bits per sample or per palette index: `1`, `2`, `4`, `8` or `16`
PNGIHDR.ColorType|INTEGER|PNG image type:<br>`0` (Greyscale)<br>`2` (True color)<br>`3` (Indexed color)<br>`4` (Greyscale with alpha)<br>`6` (True color with alpha)
PNGIHDR.CompressionType|INTEGER|Method used to compress the image data: `0` (Deflate/Inflate)
PNGIHDR.FilterMethod|INTEGER|Preprocessing method applied to the image data before compression: `0` (Adaptive)
PNGIHDR.InterlaceMethod|INTEGER|Transmission order of the image data: `0` (No interlace) or `1` (Adam7 interlace)
PNGPLTE.PaletteSize|INTEGER|Number of palette entries
PNGTRNS.PaletteHasTransparency|INTEGER|`1` if the palette has transparent colors 
PNGChromaticities.WhitePointX|INTEGER|White point, X axis
PNGChromaticities.WhitePointY|INTEGER|White point, Y axis
PNGChromaticities.RedX|INTEGER|Chromaticities of red, X axis
PNGChromaticities.RedY|INTEGER|Chromaticities of red, Y axis
PNGChromaticities.GreenX|INTEGER|Chromaticities of green, X axis
PNGChromaticities.GreenY|INTEGER|Chromaticities of green, Y axis
PNGChromaticities.BlueX|INTEGER|Chromaticities of blue, X axis
PNGChromaticities.BlueY|INTEGER|Chromaticities of blue, Y axis
PNGGAMA.ImageGamma|DOUBLE|Gamma
PNGICCP.ICCProfileName|VARCHAR|Name for referring to the profile
PNGSBIT.SignificantBits[]|INTEGER|Original number of significant bits
PNGSRGB.SRGBRenderingIntent|INTEGER|Rendering intent: `0` (Perceptual), `1` (Relative Colorimetric), `2` (Saturation) or `3` (Absolute Colorimetric)
PNGTEXt.TextualData[]|MAP|Pairs of keyword and text string
PNGZTXt.TextualData[]|MAP|Pairs of keyword and text string
PNGBKGD.BackgroundColor|VARBINARY|Default background colour to present the image against
PNGPHYs.PixelsPerUnitX|INTEGER|Pixels per unit, X axis
PNGPHYs.PixelsPerUnitY|INTEGER|Pixels per unit, Y axis
PNGPHYs.UnitSpecifier|INTEGER|Unit specifier: `0` (Unspecified) or `1` (Metres)
PNGTIME.LastModificationTime|TIMESTAMP|Time of the last image modification
GIFHeader.GIFFormatVersion|VARCHAR|Version number
GIFHeader.ImageWidth|INTEGER|Logical screen width in pixels
GIFHeader.ImageHeight|INTEGER|Logical screen height in pixels
GIFHeader.ColorTableSize|INTEGER|Size of the Global Color Table
GIFHeader.IsColorTableSorted|BOOLEAN|Whether the Global Color Table is sorted
GIFHeader.BitsPerPixel|INTEGER|Number of bits per primary color available to the original image
GIFHeader.HasGlobalColorTable|BOOLEAN|Indicates the presence of a Global Color Table
GIFHeader.BackgroundColorIndex|INTEGER|Color used for those pixels on the screen that are not covered by an image
GIFHeader.PixelAspectRatio|FLOAT|Factor used to compute an approximation of the aspect ratio of the pixel in the original image
GIFControl.DisposalMethod|VARCHAR|Way in which the graphic is to be treated after being displayed:<br>`Don't Dispose`<br>`Not Specified`<br>`Restore to Background Color`<br>`Restore to Previous`<br>`To Be Defined`
GIFControl.UserInputFlag|BOOLEAN|Whether or not user input is expected before continuing
GIFControl.TransparentColorFlag|BOOLEAN|Whether a transparency index is given in the Transparent Index field
GIFControl.Delay|INTEGER|Number of hundredths (1/100) of a second to wait before continuing with the processing of the Data Stream
GIFControl.TransparentColorIndex|INTEGER|Color used for those pixel of the display device is not modified and processing goes on to the next pixel
GIFAnimation.IterationCount|INTEGER|Number of iterations the animated GIF should be executed
GIFImage.Left|INTEGER|Column number, in pixels, of the left edge of the image, with respect to the left edge of the Logical Screen
GIFImage.Top|INTEGER|Row number, in pixels, of the top edge of the image with respect to the top edge of the Logical Screen
GIFImage.Width|INTEGER|Width of the image in pixels
GIFImage.Height|INTEGER|Height of the image in pixels
GIFImage.HasLocalColourTable|BOOLEAN|Indicates the presence of a Local Color Table
GIFImage.IsInterlaced|BOOLEAN|Whether the image is interlaced
ICO.ImageType|INTEGER|Image type: `1` (Icon) or `2` (Cursor)
ICO.ImageWidth|INTEGER|Image width in pixels
ICO.ImageHeight|INTEGER|Image height in pixels
ICO.ColourPaletteSize|INTEGER|Number of colors in the color palette
ICO.ColourPlanes|INTEGER|Number of colour planes (only for icon)
ICO.BitsPerPixel|INTEGER|Bits per pixel (only for icon)
ICO.HotspotX|INTEGER|Horizontal coordinates of the hotspot in number of pixels from the left (only for cursor)
ICO.HotspotY|INTEGER|Vertical coordinates of the hotspot in number of pixels from the top (only for cursor)
ICO.ImageSizeBytes|INTEGER|Size of the image's data in bytes
ICO.ImageOffsetBytes|INTEGER|Cffset of BMP or PNG data from the beginning of the ICO/CUR file
PCX.Version|INTEGER|PCX Paintbrush version:<br>`0` (2.5 with fixed EGA palette information)<br>`2` (2.8 with modifiable EGA palette information)<br>`3` (2.8 without palette information (default palette))<br>`4` (PC Paintbrush for Windows)<br>`5` (3.0 or better)
PCX.BitsPerPixel|INTEGER|Number of bits per pixel in each colour plane
PCX.XMin|INTEGER|Column number, in pixels, of the left edge of the image
PCX.YMin|INTEGER|Row number, in pixels, of the top edge of the image
PCX.XMax|INTEGER|Column number, in pixels, of the right edge of the image
PCX.YMax|INTEGER|Row number, in pixels, of the bottom edge of the image
PCX.HorizontalDPI|INTEGER|Horizontal resolution, in DPI (dots per inch)
PCX.VerticalDPI|INTEGER|Vertical resolution, in DPI (dots per inch)
PCX.Palette|VARBINARY|For 16 colors or less, entries of RGB for the palette, similar to bitmap palette, but each entry 3 bytes long only
PCX.ColorPlanes|INTEGER|Number of colour planes: `3` (24-bit color) or `4` (16 colors)
PCX.BytesPerLine|INTEGER|Number of bytes to read for a single plane's scanline
PCX.PaletteType|INTEGER|How to interpret palette: `1` (Color or B&W) or `2` (Grayscale)
PCX.HScrSize|INTEGER|Horizontal scrolling size
PCX.VScrSize|INTEGER|Vertical scrolling size
WAV.BitsPerSample|INTEGER|Bits per sample
WAV.Format|VARCHAR|Audio format name
WAV.Channels|INTEGER|Number of channels
WAV.SamplesPerSecond|INTEGER|Samples per second
WAV.BytesPerSecond|INTEGER|Bytes per second
WAV.BlockAlignment|INTEGER|Number of bytes for one sample including all channels
WAV.Software|VARCHAR|Name of the software used to generate the data
WAV.Duration|VARCHAR|Duration
AVI.FramesPerSecond|DOUBLE|Frames per second
AVI.SamplesPerSecond|DOUBLE|Samples per second
AVI.Duration|VARCHAR|Duration
AVI.VideoCodec|VARCHAR|Video codec
AVI.Width|INTEGER|Width of video stream
AVI.Height|INTEGER|Height of video stream
AVI.StreamCount|INTEGER|Number of streams in the file
WebP.ImageWidth|INTEGER|Image width in pixels
WebP.ImageHeight|INTEGER|Image height in pixels
WebP.HasAlpha|BOOLEAN|`true` if any of the frames of the image contain transparency information ("alpha")
WebP.IsAnimation|BOOLEAN|`true` if this is an animated image
QuickTime.MajorBrand|VARCHAR|Major file format brand:<br>`3g2a` (3GPP2 Media (.3G2) compliant with 3GPP2 C.S0050-0 V1.0)<br>`3g2b` (3GPP2 Media (.3G2) compliant with 3GPP2 C.S0050-A V1.0.0)<br>`3g2c` (3GPP2 Media (.3G2) compliant with 3GPP2 C.S0050-B v1.0)<br>`3ge6` (3GPP (.3GP) Release 6 MBMS Extended Presentations)<br>`3ge7` (3GPP (.3GP) Release 7 MBMS Extended Presentations)<br>`3gg6` (3GPP Release 6 General Profile)<br>`3gp1` (3GPP Media (.3GP) Release 1 (probably non-existent))<br>`3gp2` (3GPP Media (.3GP) Release 2 (probably non-existent))<br>`3gp3` (3GPP Media (.3GP) Release 3 (probably non-existent))<br>`3gp4` (3GPP Media (.3GP) Release 4)<br>`3gp5` (3GPP Media (.3GP) Release 5)<br>`3gp6` (3GPP Media (.3GP) Release 6 Basic Profile)<br>`3gp6` (3GPP Media (.3GP) Release 6 Progressive Download)<br>`3gp6` (3GPP Media (.3GP) Release 6 Streaming Servers)<br>`3gs7` (3GPP Media (.3GP) Release 7 Streaming Servers)<br>`avc1` (MP4 Base w/ AVC ext [ISO 14496-12:2005])<br>`CAEP` (Canon Digital Camera)<br>`caqv` (Casio Digital Camera)<br>`CDes` (Convergent Design)<br>`da0a` (DMB MAF w/ MPEG Layer II aud, MOT slides, DLS, JPG/PNG/MNG images)<br>`da0b` (DMB MAF, extending DA0A, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`da1a` (DMB MAF audio with ER-BSAC audio, JPG/PNG/MNG images)<br>`da1b` (DMB MAF, extending da1a, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`da2a` (DMB MAF aud w/ HE-AAC v2 aud, MOT slides, DLS, JPG/PNG/MNG images)<br>`da2b` (DMB MAF, extending da2a, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`da3a` (DMB MAF aud with HE-AAC aud, JPG/PNG/MNG images)<br>`da3b` (DMB MAF, extending da3a w/ BIFS, 3GPP timed text, DID, TVA, REL, IPMP)<br>`dmb1` (DMB MAF supporting all the components defined in the specification)<br>`dmpf` (Digital Media Project)<br>`drc1` (Dirac (wavelet compression), encapsulated in ISO base media (MP4))<br>`dv1a` (DMB MAF vid w/ AVC vid, ER-BSAC aud, BIFS, JPG/PNG/MNG images, TS)<br>`dv1b` (DMB MAF, extending dv1a, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`dv2a` (DMB MAF vid w/ AVC vid, HE-AAC v2 aud, BIFS, JPG/PNG/MNG images, TS)<br>`dv2b` (DMB MAF, extending dv2a, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`dv3a` (DMB MAF vid w/ AVC vid, HE-AAC aud, BIFS, JPG/PNG/MNG images, TS)<br>`dv3b` (DMB MAF, extending dv3a, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`dvr1` (DVB (.DVB) over RTP)<br>`dvt1` (DVB (.DVB) over MPEG-2 Transport Stream)<br><code>F4V&nbsp;</code> (Video for Adobe Flash Player 9+ (.F4V))<br><code>F4P&nbsp;</code> (Protected Video for Adobe Flash Player 9+ (.F4P))<br><code>F4A&nbsp;</code> (Audio for Adobe Flash Player 9+ (.F4A))<br><code>F4B&nbsp;</code> (Audio Book for Adobe Flash Player 9+ (.F4B))<br>`isc2` (ISMACryp 2.0 Encrypted File)<br>`iso2` (MP4 Base Media v2 [ISO 14496-12:2005])<br>`isom` (MP4  Base Media v1 [IS0 14496-12:2003])<br><code>JP2&nbsp;</code> (JPEG 2000 Image (.JP2) [ISO 15444-1 ?])<br>`JP20` (Unknown, from GPAC samples (prob non-existent))<br><code>jpm&nbsp;</code> (JPEG 2000 Compound Image (.JPM) [ISO 15444-6])<br><code>jpx&nbsp;</code> (JPEG 2000 w/ extensions (.JPX) [ISO 15444-2])<br>`KDDI` (3GPP2 EZmovie for KDDI 3G cellphones)<br><code>M4A&nbsp;</code> (Apple iTunes AAC-LC (.M4A) Audio)<br><code>M4B&nbsp;</code> (Apple iTunes AAC-LC (.M4B) Audio Book)<br><code>M4P&nbsp;</code> (Apple iTunes AAC-LC (.M4P) AES Protected Audio)<br><code>M4V&nbsp;</code> (Apple iTunes Video (.M4V) Video)<br>`M4VH` (Apple TV (.M4V))<br>`M4VP` (Apple iPhone (.M4V))<br>`mj2s` (Motion JPEG 2000 [ISO 15444-3] Simple Profile)<br>`mjp2` (Motion JPEG 2000 [ISO 15444-3] General Profile)<br>`mmp4` (MPEG-4/3GPP Mobile Profile (.MP4 / .3GP) (for NTT))<br>`mp21` (MPEG-21 [ISO/IEC 21000-9])<br>`mp41` (MP4 v1 [ISO 14496-1:ch13])<br>`mp42` (MP4 v2 [ISO 14496-14])<br>`mp71` (MP4 w/ MPEG-7 Metadata [per ISO 14496-12])<br>`MPPI` (Photo Player, MAF [ISO/IEC 23000-3])<br><code>mqt&nbsp;</code> (Sony / Mobile QuickTime (.MQV)  US Patent 7,477,830 (Sony Corp))<br>`MSNV` (MPEG-4 (.MP4) for SonyPSP)<br>`NDAS` (MP4 v2 [ISO 14496-14] Nero Digital AAC Audio)<br>`NDSC` (MPEG-4 (.MP4) Nero Cinema Profile)<br>`NDSH` (MPEG-4 (.MP4) Nero HDTV Profile)<br>`NDSM` (MPEG-4 (.MP4) Nero Mobile Profile)<br>`NDSP` (MPEG-4 (.MP4) Nero Portable Profile)<br>`NDSS` (MPEG-4 (.MP4) Nero Standard Profile)<br>`NDXC` (H.264/MPEG-4 AVC (.MP4) Nero Cinema Profile)<br>`NDXH` (H.264/MPEG-4 AVC (.MP4) Nero HDTV Profile)<br>`NDXM` (H.264/MPEG-4 AVC (.MP4) Nero Mobile Profile)<br>`NDXP` (H.264/MPEG-4 AVC (.MP4) Nero Portable Profile)<br>`NDXS` (H.264/MPEG-4 AVC (.MP4) Nero Standard Profile)<br>`odcf` (OMA DCF DRM Format 2.0 (OMA-TS-DRM-DCF-V2_0-20060303-A))<br>`opf2` (OMA PDCF DRM Format 2.1 (OMA-TS-DRM-DCF-V2_1-20070724-C))<br>`opx2` (OMA PDCF DRM + XBS extensions (OMA-TS-DRM_XBS-V1_0-20070529-C))<br>`pana` (Panasonic Digital Camera)<br><code>qt&nbsp;&nbsp;</code> (Apple QuickTime (.MOV/QT))<br>`ROSS` (Ross Video)<br><code>sdv&nbsp;</code> (SD Memory Card Video)<br>`ssc1` (Samsung stereoscopic, single stream (patent pending, see notes))<br>`ssc2` (Samsung stereoscopic, dual stream (patent pending, see notes))
QuickTime.MinorVersion|INTEGER|File format specification version
QuickTime.CompatibleBrands[]|VARCHAR|Compatible file format brands
QuickTime.CreationTime|TIMESTAMP|Date and time when the movie atom was created
QuickTime.ModificationTime|TIMESTAMP|Date and time when the movie atom was changed
QuickTime.Duration|INTEGER|Duration of the movie in time scale units
QuickTime.MediaTimeScale|INTEGER|Time scale for this movie — that is, the number of time units that pass per second in its time coordinate system. A time coordinate system that measures time in sixtieths of a second, for example, has a time scale of 60
QuickTime.TransformationMatrix[9]|INTEGER|The matrix structure associated with this movie. A matrix shows how to map points from one coordinate space into another
QuickTime.PreferredRate|DOUBLE|Rate at which to play this movie. A value of `1.0` indicates normal rate
QuickTime.PreferredVolume|DOUBLE|How loud to play this movie’s sound. A value of `1.0` indicates full volume
QuickTime.PreviewTime|INTEGER|Time value in the movie at which the preview begins
QuickTime.PreviewDuration|INTEGER|Duration of the movie preview in movie time scale units
QuickTime.PosterTime|INTEGER|Time value of the time of the movie poster
QuickTime.SelectionTime|INTEGER|Time value for the start time of the current selection
QuickTime.SelectionDuration|INTEGER|Duration of the current selection in movie time scale units
QuickTime.CurrentTime|INTEGER|Time value for current time position within the movie
QuickTime.NextTrackID|INTEGER|Value to use for the track ID number of the next track added to this movie
QuickTimeVideo.CreationTime|TIMESTAMP|Date and time when the media atom was created
QuickTimeVideo.ModificationTime|TIMESTAMP|Date and time when the media atom was changed
QuickTimeVideo.Opcolor[3]|INTEGER|Red, green, and blue colors for the transfer mode operation indicated in the graphics mode field
QuickTimeVideo.GraphicsMode|INTEGER|Transfer mode that specifies which Boolean operation QuickDraw should perform when drawing or transferring an image from one location to another:<br>`0x0` (Copy)<br>`0x20` (Blend)<br>`0x24` (Transparent)<br>`0x40` (Dither copy)<br>`0x100` (Straight alpha)<br>`0x101` (Premul white alpha)<br>`0x102` (Premul black alpha)<br>`0x103` (Composition (dither copy))<br>`0x104` (Straight alpha blend)
QuickTimeVideo.Vendor|VARCHAR|Developer of the compressor that generated the compressed data
QuickTimeVideo.CompressionType|VARCHAR|Type of compression that was used to compress the image data, or the color space representation of uncompressed video data
QuickTimeVideo.TemporalQuality|INTEGER|Degree of temporal compression. A value from `0` to `1023`
QuickTimeVideo.SpatialQuality|INTEGER|Degree of spatial compression. A value from `0` to `1024`
QuickTimeVideo.Width|INTEGER|Width of the source image in pixels
QuickTimeVideo.Height|INTEGER|Height of the source image in pixels
QuickTimeVideo.CompressorName|VARCHAR|Name of the compressor that created the image
QuickTimeVideo.Depth|INTEGER|Pixel depth of the compressed image. Values of `1`, `2`, `4`, `8` ,`16`, `24`, and `32` indicate the depth of color images. The value `32` should be used only if the image contains an alpha channel. Values of `34`, `36`, and `40` indicate 2-, 4-, and 8-bit grayscale, respectively, for grayscale images
QuickTimeVideo.ColorTable|INTEGER|Color table ID. If this field is set to `–1`, the default color table should be used for the specified depth. For all depths below 16 bits per pixel, this indicates a standard Macintosh color table for the specified depth. Depths of 16, 24, and 32 have no color table
QuickTimeVideo.HorizontalResolution|DOUBLE|Horizontal resolution of the image in pixels per inch
QuickTimeVideo.VerticalResolution|DOUBLE|Vertical resolution of the image in pixels per inch
QuickTimeSound.CreationTime|TIMESTAMP|TIMESTAMP|Date and time when the media atom was created
QuickTimeSound.ModificationTime|TIMESTAMP|Date and time when the media atom was changed
QuickTimeSound.Balance|DOUBLE|Sound balance of this sound media
QuickTimeSound.Format|VARCHAR|Format of the audio data. This may specify a compression format or one of several uncompressed audio formats
QuickTimeSound.NumberOfChannels|INTEGER|Number of sound channels used by the sound sample: `1` (Monaural sounds) or `2` (Stereo sounds)
QuickTimeSound.SampleSize|INTEGER|Number of bits in each uncompressed sound sample. Allowable values are `8` or `16`
QuickTimeSound.SampleRate|DOUBLE|Rate at which the sound samples were obtained
QuickTimeMetadata.ISO6709|VARCHAR|Geographic point location by coordinates as defined in ISO 6709:2008
QuickTimeMetadata.Make|VARCHAR|Manufacturer of the recording equipment
QuickTimeMetadata.Model|VARCHAR|Model of the recording equipment
QuickTimeMetadata.Software|VARCHAR|Name of software used to create the movie file content
QuickTimeMetadata.CreationDate|VARCHAR|Date the movie file content was created
QuickTimeMetadata.PixelDensity[4]|INTEGER|Pixel Density
MP4.MajorBrand|VARCHAR|Major file format brand:<br>`3g2a` (3GPP2 Media (.3G2) compliant with 3GPP2 C.S0050-0 V1.0)<br>`3g2b` (3GPP2 Media (.3G2) compliant with 3GPP2 C.S0050-A V1.0.0)<br>`3g2c` (3GPP2 Media (.3G2) compliant with 3GPP2 C.S0050-B v1.0)<br>`3ge6` (3GPP (.3GP) Release 6 MBMS Extended Presentations)<br>`3ge7` (3GPP (.3GP) Release 7 MBMS Extended Presentations)<br>`3gg6` (3GPP Release 6 General Profile)<br>`3gp1` (3GPP Media (.3GP) Release 1 (probably non-existent))<br>`3gp2` (3GPP Media (.3GP) Release 2 (probably non-existent))<br>`3gp3` (3GPP Media (.3GP) Release 3 (probably non-existent))<br>`3gp4` (3GPP Media (.3GP) Release 4)<br>`3gp5` (3GPP Media (.3GP) Release 5)<br>`3gp6` (3GPP Media (.3GP) Release 6 Basic Profile)<br>`3gp6` (3GPP Media (.3GP) Release 6 Progressive Download)<br>`3gp6` (3GPP Media (.3GP) Release 6 Streaming Servers)<br>`3gs7` (3GPP Media (.3GP) Release 7 Streaming Servers)<br>`avc1` (MP4 Base w/ AVC ext [ISO 14496-12:2005])<br>`CAEP` (Canon Digital Camera)<br>`caqv` (Casio Digital Camera)<br>`CDes` (Convergent Design)<br>`da0a` (DMB MAF w/ MPEG Layer II aud, MOT slides, DLS, JPG/PNG/MNG images)<br>`da0b` (DMB MAF, extending DA0A, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`da1a` (DMB MAF audio with ER-BSAC audio, JPG/PNG/MNG images)<br>`da1b` (DMB MAF, extending da1a, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`da2a` (DMB MAF aud w/ HE-AAC v2 aud, MOT slides, DLS, JPG/PNG/MNG images)<br>`da2b` (DMB MAF, extending da2a, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`da3a` (DMB MAF aud with HE-AAC aud, JPG/PNG/MNG images)<br>`da3b` (DMB MAF, extending da3a w/ BIFS, 3GPP timed text, DID, TVA, REL, IPMP)<br>`dmb1` (DMB MAF supporting all the components defined in the specification)<br>`dmpf` (Digital Media Project)<br>`drc1` (Dirac (wavelet compression), encapsulated in ISO base media (MP4))<br>`dv1a` (DMB MAF vid w/ AVC vid, ER-BSAC aud, BIFS, JPG/PNG/MNG images, TS)<br>`dv1b` (DMB MAF, extending dv1a, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`dv2a` (DMB MAF vid w/ AVC vid, HE-AAC v2 aud, BIFS, JPG/PNG/MNG images, TS)<br>`dv2b` (DMB MAF, extending dv2a, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`dv3a` (DMB MAF vid w/ AVC vid, HE-AAC aud, BIFS, JPG/PNG/MNG images, TS)<br>`dv3b` (DMB MAF, extending dv3a, with 3GPP timed text, DID, TVA, REL, IPMP)<br>`dvr1` (DVB (.DVB) over RTP)<br>`dvt1` (DVB (.DVB) over MPEG-2 Transport Stream)<br><code>F4V&nbsp;</code> (Video for Adobe Flash Player 9+ (.F4V))<br><code>F4P&nbsp;</code> (Protected Video for Adobe Flash Player 9+ (.F4P))<br><code>F4A&nbsp;</code> (Audio for Adobe Flash Player 9+ (.F4A))<br><code>F4B&nbsp;</code> (Audio Book for Adobe Flash Player 9+ (.F4B))<br>`isc2` (ISMACryp 2.0 Encrypted File)<br>`iso2` (MP4 Base Media v2 [ISO 14496-12:2005])<br>`isom` (MP4  Base Media v1 [IS0 14496-12:2003])<br><code>JP2&nbsp;</code> (JPEG 2000 Image (.JP2) [ISO 15444-1 ?])<br>`JP20` (Unknown, from GPAC samples (prob non-existent))<br><code>jpm&nbsp;</code> (JPEG 2000 Compound Image (.JPM) [ISO 15444-6])<br><code>jpx&nbsp;</code> (JPEG 2000 w/ extensions (.JPX) [ISO 15444-2])<br>`KDDI` (3GPP2 EZmovie for KDDI 3G cellphones)<br><code>M4A&nbsp;</code> (Apple iTunes AAC-LC (.M4A) Audio)<br><code>M4B&nbsp;</code> (Apple iTunes AAC-LC (.M4B) Audio Book)<br><code>M4P&nbsp;</code> (Apple iTunes AAC-LC (.M4P) AES Protected Audio)<br><code>M4V&nbsp;</code> (Apple iTunes Video (.M4V) Video)<br>`M4VH` (Apple TV (.M4V))<br>`M4VP` (Apple iPhone (.M4V))<br>`mj2s` (Motion JPEG 2000 [ISO 15444-3] Simple Profile)<br>`mjp2` (Motion JPEG 2000 [ISO 15444-3] General Profile)<br>`mmp4` (MPEG-4/3GPP Mobile Profile (.MP4 / .3GP) (for NTT))<br>`mp21` (MPEG-21 [ISO/IEC 21000-9])<br>`mp41` (MP4 v1 [ISO 14496-1:ch13])<br>`mp42` (MP4 v2 [ISO 14496-14])<br>`mp71` (MP4 w/ MPEG-7 Metadata [per ISO 14496-12])<br>`MPPI` (Photo Player, MAF [ISO/IEC 23000-3])<br><code>mqt&nbsp;</code> (Sony / Mobile QuickTime (.MQV)  US Patent 7,477,830 (Sony Corp))<br>`MSNV` (MPEG-4 (.MP4) for SonyPSP)<br>`NDAS` (MP4 v2 [ISO 14496-14] Nero Digital AAC Audio)<br>`NDSC` (MPEG-4 (.MP4) Nero Cinema Profile)<br>`NDSH` (MPEG-4 (.MP4) Nero HDTV Profile)<br>`NDSM` (MPEG-4 (.MP4) Nero Mobile Profile)<br>`NDSP` (MPEG-4 (.MP4) Nero Portable Profile)<br>`NDSS` (MPEG-4 (.MP4) Nero Standard Profile)<br>`NDXC` (H.264/MPEG-4 AVC (.MP4) Nero Cinema Profile)<br>`NDXH` (H.264/MPEG-4 AVC (.MP4) Nero HDTV Profile)<br>`NDXM` (H.264/MPEG-4 AVC (.MP4) Nero Mobile Profile)<br>`NDXP` (H.264/MPEG-4 AVC (.MP4) Nero Portable Profile)<br>`NDXS` (H.264/MPEG-4 AVC (.MP4) Nero Standard Profile)<br>`odcf` (OMA DCF DRM Format 2.0 (OMA-TS-DRM-DCF-V2_0-20060303-A))<br>`opf2` (OMA PDCF DRM Format 2.1 (OMA-TS-DRM-DCF-V2_1-20070724-C))<br>`opx2` (OMA PDCF DRM + XBS extensions (OMA-TS-DRM_XBS-V1_0-20070529-C))<br>`pana` (Panasonic Digital Camera)<br><code>qt&nbsp;&nbsp;</code> (Apple QuickTime (.MOV/QT))<br>`ROSS` (Ross Video)<br><code>sdv&nbsp;</code> (SD Memory Card Video)<br>`ssc1` (Samsung stereoscopic, single stream (patent pending, see notes))<br>`ssc2` (Samsung stereoscopic, dual stream (patent pending, see notes))
MP4.MinorVersion|INTEGER|File format specification version
MP4.CompatibleBrands[]|VARCHAR|Compatible file format brands
MP4.CreationTime|TIMESTAMP|Date and time when the movie box was created
MP4.ModificationTime|TIMESTAMP|Date and time when the movie atom was changed
MP4.Duration|INTEGER|Duration of the movie in time scale units
MP4.MediaTimeScale|INTEGER|Time scale for this movie — that is, the number of time units that pass per second in its time coordinate system. A time coordinate system that measures time in sixtieths of a second, for example, has a time scale of 60
MP4.TransformationMatrix[9]|INTEGER|The matrix structure associated with this movie. A matrix shows how to map points from one coordinate space into another
MP4.PreferredRate|DOUBLE|Rate at which to play this movie. A value of 1.0 indicates normal rate
MP4.PreferredVolume|DOUBLE|How loud to play this movie’s sound. A value of 1.0 indicates full volume
MP4.NextTrackID|INTEGER|Value to use for the track ID number of the next track added to this movie
MP4Video.Vendor|VARCHAR|Developer of the compressor that generated the compressed data
MP4Video.TemporalQuality|INTEGER|Degree of temporal compression. A value from `0` to `1023`
MP4Video.Width|INTEGER|Width of the source image in pixels
MP4Video.Height|INTEGER|Height of the source image in pixels
MP4Video.Opcolor[3]|INTEGER|Red, green, and blue colors for the transfer mode operation indicated in the graphics mode field
MP4Video.GraphicsMode|INTEGER|Transfer mode that specifies which Boolean operation QuickDraw should perform when drawing or transferring an image from one location to another:<br>`0x0` (Copy)<br>`0x20` (Blend)<br>`0x24` (Transparent)<br>`0x40` (Dither copy)<br>`0x100` (Straight alpha)<br>`0x101` (Premul white alpha)<br>`0x102` (Premul black alpha)<br>`0x103` (Composition (dither copy))<br>`0x104` (Straight alpha blend)
MP4Video.CompressionType|VARCHAR|Type of compression that was used to compress the image data, or the color space representation of uncompressed video data
MP4Video.Depth|INTEGER|Pixel depth of the compressed image. Values of `1`, `2`, `4`, `8` ,`16`, `24`, and `32` indicate the depth of color images. The value `32` should be used only if the image contains an alpha channel. Values of `34`, `36`, and `40` indicate 2-, 4-, and 8-bit grayscale, respectively, for grayscale images
MP4Video.HorizontalResolution|DOUBLE|Horizontal resolution of the image in pixels per inch
MP4Video.VerticalResolution|DOUBLE|Vertical resolution of the image in pixels per inch
MP4Video.FrameRate|FLOAT|Frame rate
MP4Sound.Format|VARCHAR|Format of the audio data. This may specify a compression format or one of several uncompressed audio formats
MP4Sound.NumberOfChannels|INTEGER|Number of sound channels used by the sound sample: `1` (Monaural sounds) or `2` (Stereo sounds)
MP4Sound.SampleRate|DOUBLE|Rate at which the sound samples were obtained
MP4Sound.Balance|DOUBLE|Sound balance of this sound media
MP4Sound.SampleSize|INTEGER|Number of bits in each uncompressed sound sample. Allowable values are `8` or `16`
EPS.TIFFPreviewSize|INTEGER|Byte length of TIFF section
EPS.TIFFPreviewOffset|INTEGER|Byte position of TIFF representation
EPS.WMFPreviewSize|INTEGER|Byte length of Metafile section (PSize)
EPS.WMFPreviewOffset|INTEGER|Byte position in file for start of Metafile screen representation
EPS.BoundingBox|VARCHAR|Bounding box that encloses all marks painted on all pages of a document
EPS.Copyright|VARCHAR|Copyright information associated with the docu- ment or resource
EPS.Creator|VARCHAR|Document creator, usually the name of the document composition software
EPS.CreationDate|VARCHAR|Date and time the document was created
EPS.DocumentData|VARCHAR|Type of data: `Clean7Bit`, `Clean8Bit` or `Binary` 
EPS.Emulation|VARCHAR|Indicates that the document contains an invocation of the stated emulator: `diablo630`, `fx100`, `lj2000`, `hpgl`, `impress`, `hplj` or `ti855`
EPS.Extensions|VARCHAR|Indicates that in order to print properly, the document requires a PostScript Level 1 interpreter that supports the listed PostScript language extensions: `DPS`, `CMYK`, `Composite` or `FileSystem`
EPS.For|VARCHAR|Person and possibly the company name for whom the document is being printed
EPS.LanguageLevel|VARCHAR|Indicates that the document contains PostScript language operators particular to a certain level of implementation of the PostScript language
EPS.Orientation|VARCHAR|Orientation of the pages in the document: `Portrait` or `Landscape`
EPS.Pages|VARCHAR|Number of virtual pages that a document will image
EPS.PageOrder|VARCHAR|Order of pages in the document file: `Ascend`, `Descend` or `Special`
EPS.Routing|VARCHAR|Information about how to route a document back to its owner after printing
EPS.Title|VARCHAR|Text title for the document that is useful for printing banner pages and for routing or recognizing documents
EPS.Version|VARCHAR|Version and revision number of a document or resource
EPS.OperatorIntervention|VARCHAR|Password that the print operator must supply to release the job
EPS.OperatorMessage|VARCHAR|Message that the document manager can display on the console before printing the job
EPS.ProofMode|VARCHAR|Level of accuracy that is required for printing: `TrustMe`, `Substitute` or `NotifyMe`
EPS.Requirements|VARCHAR|Document requirements, such as duplex printing, hole punching, collating, or other physical document processing needs
EPS.VMLocation|VARCHAR|Whether a resource can be loaded into global or local VM: `global` or `local`
EPS.VMusage|VARCHAR|Amount of VM storage this resource consumes
EPS.ImageData|VARCHAR|%ImageData comment
EPS.ImageWidth|INTEGER|Width of the image in pixels
EPS.ImageHeight|INTEGER|Height of the image in pixels
EPS.ColorType|INTEGER|Color type: `1` (Bitmap/grayscale), `2` (Lab), `3`: RGB or `4` (CMYK)
EPS.RamSize|INTEGER|Ram Size to keep the image in bytes  

     
       
