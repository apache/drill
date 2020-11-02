---
title: "GIS functions"
date: 2020-11-02
parent: "SQL Functions"
---

**Introduced in release:** 1.14.

Drill contains a suite of Geographic Information System (GIS) functions. Most of the functionality follows that of PostGIS.  To use these functions, your spatial data must be defined in the Well-Known Text (WKT) representation format.  The WKT format allows you to represent points, lines, polygons, and other geometric shapes. Following are two example WKT strings:

```
POINT (30 10)
POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))
```

Drill stores points as binary, so to read or plot these points you need to convert them using the `ST_AsText()` and `ST_GeoFromText()` functions.  The following table list the GIS functions included in Drill.

|---------------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Function                              | Output   | Description                                                                                                                                                                                                  |
|---------------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ST_AsGeoJSON(geometry)                | VARCHAR  | Returns the geometry as a GeoJSON element.                                                                                                                                                                   |
| ST_AsJSON(geometry)                   | VARCHAR  | Returns JSON representation of the geometry.                                                                                                                                                                 |
| ST_AsText(geometry)                   | VARCHAR  | Return a the WKT representation of the geometry/geography without SRID metadata                                                                                                                              |
| ST_Buffer(geometry, radius)           | GEOMETRY | Returns a geometry that represents all points whose distance from this geometry is less than or equal to radius.                                                                                             |
| ST_Contains(geometry_a, geometry_b)   | BOOLEAN  | Returns true if no points of geometry_b lie outside of geometry_a and at least one point of the interior of geometry_b is in the interior of geometry_a.                                                     |
| ST_Crosses(geometry_a, geometry_b)    | BOOLEAN  | Returns true if the supplied geometries have some but not all interior points in common.                                                                                                                     |
| ST_Difference(geometry_a, geometry_b) | GEOMETRY | Returns a geometry that represents the part of geometry_a that does not intersect with geometry_b.                                                                                                           |
| ST_Disjoint(geometry_a, geometry_b)   | BOOLEAN  | Returns true if the two geometries do not spatially intersect.                                                                                                                                               |
| ST_Distance(geometry_a, geometry_b)   | DOUBLE   | For geometry types, returns the 2D Cartesian distance between two geometries in projected units. For geography types, defaults to returning the minimum geodesic distance between two geographies in meters. |
| ST_DWithin(geometry_a, geometry_b,
			distance)                     | BOOLEAN  | Returns true if the geometries are within the specified distance of one another.                                                                                                                                                                                                                                                                                             |
| ST_Envelope(geometry)                 | GEOMETRY | Returns a geometry representing the double-precision bounding box of the supplied geometry. The polygon is defined by the corner points of the bounding box: ((MINX, MINY), (MINX, MAXY), (MAXX, MAXY), (MAXX, MINY), (MINX, MINY)). |
| ST_GeoFromText(text,[SRID])           | GEOMETRY | Returns a specified ST_Geometry value from the WKT representation. If the spatial reference ID (SRID) is included as the second argument the function returns a geometry that includes this SRID as part of its metadata.            |
| ST_Equals(geometry_a, geometry_b)     | BOOLEAN  | Returns true if the given geometries represent the same geometry. Directionality is ignored.                                                                                                                                         |
| ST_Intersects(geometry_a, geometry_b) | BOOLEAN  | Returns true if the geometries/geographies "spatially intersect in 2D" (share any portion of space) and false if they don't (they are disjoint).                                                                                     |
| ST_Overlaps(geometry_a, geometry_b)   | BOOLEAN  | Returns true if the geometries share space and are of the same dimension, but are not completely contained by each other.                                                                                                            |
| ST_Point(long, lat)                   | GEOMETRY | Returns an ST_Point with the given coordinate values.                                                                                                                                                                                |
| ST_Relate(geometry_a, geometry_b
			[,intersectionMatrixPattern]) | BOOLEAN  | Returns true if geometry_a is spatially related to geometry_b, determined by testing for intersections between the interior, boundary, and exterior of the two geometries as specified by the values in the intersection matrix pattern. If no intersection matrix pattern is passed in, returns the maximum intersection matrix pattern that relates the two geometries. |
| ST_Touches(geometry_a, geometry_b)                  | BOOLEAN  | Returns true if the geometries have at least one point in common, but their interiors do not intersect.                                                  |
| ST_Transform(geometry_a [Source_SRID, Target_SRID]) | GEOMETRY | Returns a new geometry with its coordinates transformed to a different spatial reference.                                                                |
| ST_Union(geometry_a, geometry_b)                    | GEOMETRY | Returns a geometry that represents the point set union of the geometries.                                                                                |
| ST_Union_Aggregate(Geometry)                        | GEOMETRY | Returns a geometry that represents the point set union of the geometries. Note: This function is an aggregate function and should be used with GROUP BY. |
| ST_Within(geometry_a, geometry_b)                   | BOOLEAN  | Returns true if geometry_a is completely inside geometry_b.                                                                                              |
| ST_X(geometry)                                      | DOUBLE   | Returns the x coordinate of the point, or NaN if not available.                                                                                          |
| ST_XMax(geometry)                                   | DOUBLE   | Returns the x maxima of a 2D or 3D bounding box or a geometry.                                                                                           |
| ST_XMin(geometry)                                   | DOUBLE   | Returns the x minima of a 2D or 3D bounding box or a geometry.                                                                                           |
| ST_Y(geometry)                                      | DOUBLE   | Return the y coordinate of the point, or NaN if not available.                                                                                           |
| ST_YMax(geometry)                                   | DOUBLE   | Returns the y maxima of a 2D or 3D bounding box or a geometry.                                                                                           |
| ST_YMin(geometry)                                   | DOUBLE   | Returns the y minima of a 2D or 3D bounding box or a geometry.                                                                                           |
|-----------------------------------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------|

