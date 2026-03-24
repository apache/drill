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

# Script to download and process GeoJSON files for choropleth maps.
# This script regenerates all GeoJSON files in public/geojson/ from open-data sources.
#
# Dependencies: curl, ogr2ogr (gdal), jq
# To install: brew install gdal jq

set -e

GEOJSON_DIR="$(cd "$(dirname "$0")/../src/main/resources/webapp/public/geojson" && pwd)"
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

echo "Downloading GeoJSON files to $GEOJSON_DIR..."

# World (Natural Earth)
echo "→ Downloading world.json..."
curl -sL "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip" \
  -o "$TEMP_DIR/ne_countries.zip"
unzip -q "$TEMP_DIR/ne_countries.zip" -d "$TEMP_DIR/"
ogr2ogr -f GeoJSON -t_srs EPSG:4326 "$GEOJSON_DIR/world.json" \
  "$TEMP_DIR/ne_110m_admin_0_countries.shp"

# US States (US Census Bureau)
echo "→ Downloading us-states.json..."
curl -sL "https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_state_20m.zip" \
  -o "$TEMP_DIR/us_states.zip"
unzip -q "$TEMP_DIR/us_states.zip" -d "$TEMP_DIR/"
ogr2ogr -f GeoJSON -t_srs EPSG:4326 "$GEOJSON_DIR/us-states.json" \
  "$TEMP_DIR/cb_2021_us_state_20m.shp"

# US Counties (US Census Bureau - 20m simplified)
echo "→ Downloading us-counties.json..."
curl -sL "https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_county_20m.zip" \
  -o "$TEMP_DIR/us_counties.zip"
unzip -q "$TEMP_DIR/us_counties.zip" -d "$TEMP_DIR/"
ogr2ogr -f GeoJSON -t_srs EPSG:4326 "$GEOJSON_DIR/us-counties.json" \
  "$TEMP_DIR/cb_2021_us_county_20m.shp"

# US ZIP Codes (US Census Bureau ZCTA - 5-digit, 20m simplified)
# NOTE: The current us-zipcodes.json is DC-only (~1.2 MB, 172 features).
# For complete US (all 50 states), see instructions below:
#
# To download the full US ZCTA (Census Bureau 5-digit ZIP Code Tabulation Areas):
# 1. Install GDAL tools: brew install gdal
# 2. Run these commands:
#    curl -sL "https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_zcta520_20m.zip" \
#      -o us_zipcodes_temp.zip
#    unzip -q us_zipcodes_temp.zip
#    ogr2ogr -f GeoJSON us-zipcodes.json cb_2021_us_zcta520_20m.shp
#    rm us_zipcodes_temp.zip cb_2021_us_zcta520_20m.*
#
# The full US ZCTA GeoJSON is ~100 MB (uncompressed), so it's delivered
# as a lazy-loaded map file rather than bundled in the JavaScript.
echo "→ US ZIP Codes (use instructions above for full US data)"

# Canadian Provinces (Statistics Canada)
echo "→ Downloading ca-provinces.json..."
curl -sL "https://www12.statcan.gc.ca/cartography/shared/files/lcpr000b21a_e.zip" \
  -o "$TEMP_DIR/ca_provinces.zip"
unzip -q "$TEMP_DIR/ca_provinces.zip" -d "$TEMP_DIR/"
find "$TEMP_DIR" -name "*.shp" | head -1 | xargs \
  ogr2ogr -f GeoJSON -t_srs EPSG:4326 "$GEOJSON_DIR/ca-provinces.json"

# Mexican States (Natural Earth admin-1)
echo "→ Downloading mx-states.json..."
curl -sL "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_1_states_provinces.zip" \
  -o "$TEMP_DIR/ne_admin1.zip"
unzip -q "$TEMP_DIR/ne_admin1.zip" -d "$TEMP_DIR/"
ogr2ogr -f GeoJSON -where "iso_a2='MX'" -t_srs EPSG:4326 "$GEOJSON_DIR/mx-states.json" \
  "$TEMP_DIR/ne_10m_admin_1_states_provinces.shp"

# Brazilian States (Natural Earth)
echo "→ Downloading br-states.json..."
ogr2ogr -f GeoJSON -where "iso_a2='BR'" -t_srs EPSG:4326 "$GEOJSON_DIR/br-states.json" \
  "$TEMP_DIR/ne_10m_admin_1_states_provinces.shp"

# UK Regions (Natural Earth)
echo "→ Downloading gb-regions.json..."
ogr2ogr -f GeoJSON -where "iso_a2='GB'" -t_srs EPSG:4326 "$GEOJSON_DIR/gb-regions.json" \
  "$TEMP_DIR/ne_10m_admin_1_states_provinces.shp"

# German States (Natural Earth)
echo "→ Downloading de-states.json..."
ogr2ogr -f GeoJSON -where "iso_a2='DE'" -t_srs EPSG:4326 "$GEOJSON_DIR/de-states.json" \
  "$TEMP_DIR/ne_10m_admin_1_states_provinces.shp"

# French Departments (Natural Earth)
echo "→ Downloading fr-departments.json..."
ogr2ogr -f GeoJSON -where "iso_a2='FR'" -t_srs EPSG:4326 "$GEOJSON_DIR/fr-departments.json" \
  "$TEMP_DIR/ne_10m_admin_1_states_provinces.shp"

# Australian States (Natural Earth)
echo "→ Downloading au-states.json..."
ogr2ogr -f GeoJSON -where "iso_a2='AU'" -t_srs EPSG:4326 "$GEOJSON_DIR/au-states.json" \
  "$TEMP_DIR/ne_10m_admin_1_states_provinces.shp"

# Indian States (Natural Earth)
echo "→ Downloading in-states.json..."
ogr2ogr -f GeoJSON -where "iso_a2='IN'" -t_srs EPSG:4326 "$GEOJSON_DIR/in-states.json" \
  "$TEMP_DIR/ne_10m_admin_1_states_provinces.shp"

# Chinese Provinces (Natural Earth)
echo "→ Downloading cn-provinces.json..."
ogr2ogr -f GeoJSON -where "iso_a2='CN'" -t_srs EPSG:4326 "$GEOJSON_DIR/cn-provinces.json" \
  "$TEMP_DIR/ne_10m_admin_1_states_provinces.shp"

# Japanese Prefectures (Natural Earth)
echo "→ Downloading jp-prefectures.json..."
ogr2ogr -f GeoJSON -where "iso_a2='JP'" -t_srs EPSG:4326 "$GEOJSON_DIR/jp-prefectures.json" \
  "$TEMP_DIR/ne_10m_admin_1_states_provinces.shp"

echo ""
echo "✓ All GeoJSON files downloaded and converted."
echo ""
echo "Files created:"
ls -lh "$GEOJSON_DIR"/*.json | awk '{print "  " $9 " (" $5 ")"}'
