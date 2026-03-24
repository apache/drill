#!/usr/bin/env python3
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

"""
Download and prepare GeoJSON files for choropleth maps.
Uses open-source data from reliable CDNs and repositories.
"""

import json
import os
import sys
import urllib.request
from pathlib import Path

# Get the public/geojson directory
script_dir = Path(__file__).parent
geojson_dir = script_dir.parent / "public" / "geojson"
geojson_dir.mkdir(parents=True, exist_ok=True)

print(f"📍 Downloading GeoJSON files to {geojson_dir}...\n")

# Define all maps with their sources
# Using jsdelivr CDN which hosts various open-source GeoJSON repositories
MAPS = {
    "world.json": {
        "url": "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-10m.json",
        "description": "World countries (world-atlas)",
        "transform": "world_countries",
    },
    "us-states.json": {
        "url": "https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json",
        "description": "US States (US Census)",
        "transform": "us_states",
    },
    "us-counties.json": {
        "url": "https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json",
        "description": "US Counties (US Census)",
        "transform": "us_counties",
    },
    "ca-provinces.json": {
        "url": "https://cdn.jsdelivr.net/npm/canada-atlas@4/provinces-10m.json",
        "description": "Canadian Provinces",
        "transform": "provinces",
    },
    "mx-states.json": {
        "url": "https://cdn.jsdelivr.net/npm/mexico-atlas@4/states-10m.json",
        "description": "Mexican States",
        "transform": "states",
    },
    "br-states.json": {
        "url": "https://cdn.jsdelivr.net/npm/brasil@0.0.2/json/states.json",
        "description": "Brazilian States",
        "transform": None,
    },
    "gb-regions.json": {
        "url": "https://raw.githubusercontent.com/ONS-OpenData/Census_2021_Atlas/main/geographic_data/gadm/gadm_0_gBr.geojson",
        "description": "UK Regions (ONS)",
        "transform": None,
    },
    "de-states.json": {
        "url": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_1_states_provinces.geojson",
        "description": "German States (Natural Earth)",
        "filter_iso": "DEU",
    },
    "fr-departments.json": {
        "url": "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/metropole.geojson",
        "description": "French Departments",
        "transform": None,
    },
    "au-states.json": {
        "url": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_1_states_provinces.geojson",
        "description": "Australian States (Natural Earth)",
        "filter_iso": "AUS",
    },
    "in-states.json": {
        "url": "https://raw.githubusercontent.com/datameet/india-maps/master/states/india-states.geojson",
        "description": "Indian States",
        "transform": None,
    },
    "cn-provinces.json": {
        "url": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_1_states_provinces.geojson",
        "description": "Chinese Provinces (Natural Earth)",
        "filter_iso": "CHN",
    },
    "jp-prefectures.json": {
        "url": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_1_states_provinces.geojson",
        "description": "Japanese Prefectures (Natural Earth)",
        "filter_iso": "JPN",
    },
}


def download_file(url: str) -> dict:
    """Download a file from URL."""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            content = response.read().decode("utf-8")
            return json.loads(content)
    except Exception as e:
        return None


def topojson_to_geojson(topo_json: dict, object_name: str) -> dict:
    """Convert TopoJSON to GeoJSON (simplified conversion)."""
    if not isinstance(topo_json, dict) or "objects" not in topo_json:
        return topo_json

    if object_name not in topo_json["objects"]:
        # Try to find the first available object
        if topo_json["objects"]:
            object_name = list(topo_json["objects"].keys())[0]
        else:
            return None

    # This is a simplified conversion - for production use topojson library
    # For now, just return features if available
    topo_obj = topo_json["objects"][object_name]
    if "geometries" in topo_obj:
        features = []
        for geom in topo_obj.get("geometries", []):
            features.append({
                "type": "Feature",
                "properties": geom.get("properties", {}),
                "geometry": geom,
            })
        return {
            "type": "FeatureCollection",
            "features": features,
        }
    return None


def filter_by_iso(geojson: dict, iso_a2: str) -> dict:
    """Filter a GeoJSON FeatureCollection by ISO alpha-2 country code."""
    features = [
        f for f in geojson.get("features", [])
        if f.get("properties", {}).get("iso_a2") == iso_a2
    ]
    return {
        "type": "FeatureCollection",
        "features": features,
    }


def download_and_save_map(filename: str, config: dict) -> bool:
    """Download a GeoJSON file and save it locally."""
    print(f"→ {filename:25} {config['description']:40}", end=" ", flush=True)

    data = download_file(config["url"])
    if not data:
        print("❌")
        return False

    # Convert TopoJSON to GeoJSON if needed
    if "transform" in config and config["transform"]:
        data = topojson_to_geojson(data, config["transform"])
        if not data:
            print("❌")
            return False

    # Filter by ISO code if specified
    if "filter_iso" in config:
        data = filter_by_iso(data, config["filter_iso"])

    # Ensure it's a valid FeatureCollection
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        print("❌")
        return False

    # Save to file
    try:
        output_path = geojson_dir / filename
        with open(output_path, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        file_size = output_path.stat().st_size / 1024
        print(f"✅ ({file_size:.1f} KB)")
        return True
    except Exception as e:
        print(f"❌ {e}")
        return False


def main():
    """Download all GeoJSON files."""
    success_count = 0
    fail_count = 0

    for filename, config in MAPS.items():
        if download_and_save_map(filename, config):
            success_count += 1
        else:
            fail_count += 1

    print(f"\n{'='*70}")
    print(f"✅ Downloaded: {success_count}/{len(MAPS)}")
    if fail_count > 0:
        print(f"⚠️  Failed: {fail_count}")
    print(f"\nFiles saved to: {geojson_dir}")
    print(f"\nFile listing:")
    for f in sorted(geojson_dir.glob("*.json")):
        size = f.stat().st_size / 1024
        print(f"  {f.name:25} ({size:>8.1f} KB)")


if __name__ == "__main__":
    main()
