#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Setup script for full US ZIP code data
# Downloads Census Bureau ZCTA (5-digit ZIP Code Tabulation Areas) and converts to GeoJSON
# Requires: GDAL (brew install gdal)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GEOJSON_DIR="$(dirname "$SCRIPT_DIR")/public/geojson"
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

echo "Setting up full US ZIP code dataset..."
echo "This will replace the DC-only placeholder with complete US data (~100 MB)"
echo ""

# Check for ogr2ogr
if ! command -v ogr2ogr &> /dev/null; then
    echo "ERROR: ogr2ogr not found. Install GDAL:"
    echo "  brew install gdal"
    exit 1
fi

# Download Census Bureau ZCTA data
echo "Downloading US ZIP Code Tabulation Areas from Census Bureau..."
ZCTA_URL="https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_zcta520_20m.zip"

cd "$TEMP_DIR"

# Try multiple download methods
if ! curl -fL "$ZCTA_URL" -o us_zcta.zip; then
    echo "Trying 2020 census data..."
    if ! curl -fL "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_zcta520_20m.zip" -o us_zcta.zip; then
        echo "ERROR: Could not download Census Bureau data"
        echo "Try manually downloading from:"
        echo "  $ZCTA_URL"
        exit 1
    fi
fi

# Extract
echo "Extracting shapefile..."
unzip -q us_zcta.zip

# Find the .shp file
SHP_FILE=$(find . -name "*.shp" | head -1)
if [ -z "$SHP_FILE" ]; then
    echo "ERROR: No shapefile found in archive"
    exit 1
fi

echo "Converting to GeoJSON..."
ogr2ogr -f GeoJSON "$GEOJSON_DIR/us-zipcodes.json" "$SHP_FILE"

# Verify
FEATURE_COUNT=$(grep -o '"type":"Feature"' "$GEOJSON_DIR/us-zipcodes.json" | wc -l)
FILE_SIZE=$(du -h "$GEOJSON_DIR/us-zipcodes.json" | cut -f1)

echo ""
echo "✓ Successfully created US ZIP code dataset"
echo "  Location: $GEOJSON_DIR/us-zipcodes.json"
echo "  Size: $FILE_SIZE"
echo "  Features: $FEATURE_COUNT ZIP codes"
echo ""
echo "This file is ignored by git (.gitignore) and will not be committed."
echo "It will only exist in your local development environment."
