#!/usr/bin/env python3
"""
Download and process US state ZIP code GeoJSON from OpenDataDE GitHub repo.

Source: https://github.com/OpenDataDE/State-zip-code-GeoJSON

Creates:
1. Individual state ZIP code files: us-{state}-zipcodes.json (not committed)
2. Merged all-states file: us-zipcodes.json (committed to git)

Cleanup includes:
- Remove non-5-digit ZIP codes
- Remove duplicates
- Standardize property names
- Ensure all features have 'name' property with ZIP code
- Remove invalid/empty geometries
- Minimize property fields for file size
"""

import json
import os
import subprocess
import tempfile
import shutil
from pathlib import Path
from collections import defaultdict
import sys

SCRIPT_DIR = Path(__file__).parent
GEOJSON_DIR = SCRIPT_DIR.parent / 'public' / 'geojson'
TEMP_DIR = tempfile.mkdtemp()

# GitHub raw content URLs for each state (from OpenDataDE repo)
STATE_ZIP_URLs = {
    'AL': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/al_alabama_zip_codes_geo.min.json',
    'AK': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ak_alaska_zip_codes_geo.min.json',
    'AZ': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/az_arizona_zip_codes_geo.min.json',
    'AR': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ar_arkansas_zip_codes_geo.min.json',
    'CA': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ca_california_zip_codes_geo.min.json',
    'CO': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/co_colorado_zip_codes_geo.min.json',
    'CT': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ct_connecticut_zip_codes_geo.min.json',
    'DE': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/de_delaware_zip_codes_geo.min.json',
    'FL': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/fl_florida_zip_codes_geo.min.json',
    'GA': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ga_georgia_zip_codes_geo.min.json',
    'HI': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/hi_hawaii_zip_codes_geo.min.json',
    'ID': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/id_idaho_zip_codes_geo.min.json',
    'IL': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/il_illinois_zip_codes_geo.min.json',
    'IN': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/in_indiana_zip_codes_geo.min.json',
    'IA': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ia_iowa_zip_codes_geo.min.json',
    'KS': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ks_kansas_zip_codes_geo.min.json',
    'KY': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ky_kentucky_zip_codes_geo.min.json',
    'LA': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/la_louisiana_zip_codes_geo.min.json',
    'ME': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/me_maine_zip_codes_geo.min.json',
    'MD': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/md_maryland_zip_codes_geo.min.json',
    'MA': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ma_massachusetts_zip_codes_geo.min.json',
    'MI': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/mi_michigan_zip_codes_geo.min.json',
    'MN': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/mn_minnesota_zip_codes_geo.min.json',
    'MS': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ms_mississippi_zip_codes_geo.min.json',
    'MO': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/mo_missouri_zip_codes_geo.min.json',
    'MT': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/mt_montana_zip_codes_geo.min.json',
    'NE': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ne_nebraska_zip_codes_geo.min.json',
    'NV': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/nv_nevada_zip_codes_geo.min.json',
    'NH': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/nh_new_hampshire_zip_codes_geo.min.json',
    'NJ': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/nj_new_jersey_zip_codes_geo.min.json',
    'NM': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/nm_new_mexico_zip_codes_geo.min.json',
    'NY': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ny_new_york_zip_codes_geo.min.json',
    'NC': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/nc_north_carolina_zip_codes_geo.min.json',
    'ND': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/nd_north_dakota_zip_codes_geo.min.json',
    'OH': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/oh_ohio_zip_codes_geo.min.json',
    'OK': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ok_oklahoma_zip_codes_geo.min.json',
    'OR': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/or_oregon_zip_codes_geo.min.json',
    'PA': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/pa_pennsylvania_zip_codes_geo.min.json',
    'RI': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ri_rhode_island_zip_codes_geo.min.json',
    'SC': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/sc_south_carolina_zip_codes_geo.min.json',
    'SD': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/sd_south_dakota_zip_codes_geo.min.json',
    'TN': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/tn_tennessee_zip_codes_geo.min.json',
    'TX': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/tx_texas_zip_codes_geo.min.json',
    'UT': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/ut_utah_zip_codes_geo.min.json',
    'VT': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/vt_vermont_zip_codes_geo.min.json',
    'VA': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/va_virginia_zip_codes_geo.min.json',
    'WA': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/wa_washington_zip_codes_geo.min.json',
    'WV': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/wv_west_virginia_zip_codes_geo.min.json',
    'WI': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/wi_wisconsin_zip_codes_geo.min.json',
    'WY': 'https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/wy_wyoming_zip_codes_geo.min.json',
}

def cleanup():
    """Remove temp directory"""
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)

def download_state_files():
    """Download individual state ZIP code GeoJSON files from OpenDataDE"""
    print("Downloading state ZIP code files from OpenDataDE...")

    state_data = {}
    failed_states = []

    for state, url in sorted(STATE_ZIP_URLs.items()):
        try:
            print(f"  Downloading {state}...", end=' ')
            result = subprocess.run(
                ['curl', '-fL', url],
                capture_output=True,
                timeout=30
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                state_data[state] = data
                feature_count = len(data.get('features', []))
                print(f"✓ ({feature_count} ZIP codes)")
            else:
                print(f"✗ (HTTP {result.returncode})")
                failed_states.append(state)
        except Exception as e:
            print(f"✗ ({e})")
            failed_states.append(state)

    if not state_data:
        raise Exception("No state ZIP code files could be downloaded")

    if failed_states:
        print(f"\n  ⚠ Failed to download: {', '.join(failed_states)}")

    return state_data

def is_valid_zipcode(zipcode_str):
    """Check if zipcode is valid 5-digit format"""
    s = str(zipcode_str).strip()
    return len(s) == 5 and s.isdigit()

def clean_feature(feature):
    """Clean a single feature: normalize properties, validate geometry"""
    if not feature.get('geometry') or not feature.get('properties'):
        return None

    props = feature['properties']

    # Try different property names for ZIP code (OpenDataDE uses different names)
    zipcode = None
    for key in ['properties', 'zip', 'ZIPCODE', 'ZIP_CODE', 'ZCTA5CE']:
        if key in props and props[key]:
            val = str(props[key]).strip()
            if is_valid_zipcode(val):
                zipcode = val
                break

    # If not found in those common keys, try any key that looks like a zip code
    if not zipcode:
        for key, val in props.items():
            if val and is_valid_zipcode(str(val).strip()):
                zipcode = str(val).strip()
                break

    if not zipcode:
        return None

    # Validate geometry
    geom = feature['geometry']
    if not geom or geom.get('type') not in ['Polygon', 'MultiPolygon']:
        return None

    coords = geom.get('coordinates', [])
    if not coords:
        return None

    # Create cleaned feature with minimal properties
    cleaned = {
        'type': 'Feature',
        'geometry': geom,
        'properties': {
            'name': zipcode,
            'ZIPCODE': zipcode,
        }
    }

    return cleaned

def process_state_data(state_data):
    """
    Process downloaded state GeoJSON data.
    Returns: (all_features, state_features_dict)
    """
    print(f"Processing and cleaning data...")

    seen_zips = set()
    state_groups = defaultdict(list)
    all_features = []
    total_before = 0
    total_after = 0

    for state in sorted(state_data.keys()):
        data = state_data[state]
        state_features = []

        for feature in data.get('features', []):
            cleaned = clean_feature(feature)
            if not cleaned:
                continue

            zipcode = cleaned['properties']['name']
            total_before += 1

            # Skip duplicates
            if zipcode in seen_zips:
                continue

            seen_zips.add(zipcode)
            all_features.append(cleaned)
            state_features.append(cleaned)
            total_after += 1

        if state_features:
            state_groups[state] = state_features

    print(f"  Processed: {total_after}/{total_before} features ({len(state_groups)} states with ZIP codes)")
    return all_features, state_groups

def write_geojson_file(path, features):
    """Write GeoJSON FeatureCollection to file"""
    geojson = {
        'type': 'FeatureCollection',
        'features': features,
    }
    with open(path, 'w') as f:
        json.dump(geojson, f, separators=(',', ':'))

def create_state_files(state_groups):
    """Create individual state ZIP code files"""
    print("Creating state-level ZIP code files...")

    for state in sorted(state_groups.keys()):
        features = state_groups[state]
        state_file = GEOJSON_DIR / f'us-{state.lower()}-zipcodes.json'
        write_geojson_file(state_file, features)
        size_kb = os.path.getsize(state_file) / 1024
        print(f"  {state_file.name}: {len(features)} ZIP codes ({size_kb:.1f} KB)")

def create_merged_file(all_features):
    """Create merged us-zipcodes.json with all states"""
    print("Creating merged us-zipcodes.json...")

    merged_file = GEOJSON_DIR / 'us-zipcodes.json'
    write_geojson_file(merged_file, all_features)

    file_size = os.path.getsize(merged_file) / 1024 / 1024
    print(f"  ✓ {merged_file.name}: {len(all_features)} ZIP codes ({file_size:.1f} MB)")

def main():
    print("\n=== US ZIP Code Data Processing (OpenDataDE) ===\n")

    try:
        # Ensure geojson directory exists
        GEOJSON_DIR.mkdir(parents=True, exist_ok=True)

        # Download state files from OpenDataDE
        state_data = download_state_files()

        # Process and split by state
        all_features, state_groups = process_state_data(state_data)

        if not all_features:
            print("  ✗ No valid ZIP codes found!")
            sys.exit(1)

        # Create state-level files (not committed to git)
        create_state_files(state_groups)

        # Create merged all-states file (committed to git)
        create_merged_file(all_features)

        print("\n✓ Processing complete!\n")
        print(f"Summary:")
        print(f"  Total US ZIP codes: {len(all_features):,}")
        print(f"  States with data: {len(state_groups)}")
        print(f"  Merged file: {(GEOJSON_DIR / 'us-zipcodes.json').name} ({os.path.getsize(GEOJSON_DIR / 'us-zipcodes.json') / 1024 / 1024:.1f} MB)")
        print(f"  Source: https://github.com/OpenDataDE/State-zip-code-GeoJSON\n")

    except Exception as e:
        print(f"\n✗ Error: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cleanup()

if __name__ == '__main__':
    main()
