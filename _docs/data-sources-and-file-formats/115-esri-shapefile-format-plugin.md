---
title: "ESRI Shapefile Format Plugin"
slug: "ESRI Shapefile Format Plugin"
parent: "Data Sources and File Formats"
---

**Introduced in release:** 1.17.

The ESRI Shapefile format plugin enables Drill to query geospatial data contained in ESRI shapefiles.  You can read about the shapefile format [here](https://en.wikipedia.org/wiki/Shapefile).  

## Configuring the ESRI Shapefile Format Plugin

Other than the file extensions, there are no configuration options for this plugin. To use, simply add the following to your configuration:

    "shp": {
      "type": "shp",
      "extensions": [
        "shp"
      ]
    }

## Usage Notes

This plugin creates the following columns from a shapefile.

|-----------|-----------|
| Field     | Data type |
|-----------|-----------|
| gid       | INTEGER   |
| srid      | INTEGER   |
| shapeType | VARCHAR   |
| name      | VARCHAR   |
| geom      | VARBINARY |
|-----------|-----------|

In release 1.14 Drill acquired a set of Geographic Information System (GIS) functions reminiscent of the equivalent functionality in PostGIS.  To use these functions, your spatial data must be defined in the [Well-Known Text (WKT)](https://en.wikipedia.org/wiki/Well-known_text) representation format.  The WKT format allows you to represent points, lines, polygons, and other geometric shapes.  Two example WKT strings follow.

    POINT (30 10)
    POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))

Drill stores points as binary so to read or plot these points, you need to convert them using the `ST_AsText()` and `ST_GeoFromText()` functions. <!-- TODO: provide a link to a description of Drill's GIS functions -->

