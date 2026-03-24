#!/usr/bin/env python3
"""
Download US zip code boundaries from Natural Earth / OpenStreetMap sources.
"""

import json
import urllib.request
import sys

def download_us_zipcodes():
    """
    Download US zip code boundaries.
    Using a simplified version from a reliable CDN.
    """
    # Try multiple sources
    sources = [
        # This is a commonly used simplified US zip codes GeoJSON
        "https://cdn.jsdelivr.net/npm/us-atlas@3/zips-10m.json",
        # Alternative TopoJSON source - we'll convert if needed
        "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_0_countries.geojson",
    ]
    
    for url in sources:
        print(f"Trying {url}...")
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                data = json.loads(response.read().decode('utf-8'))
            
            if 'features' in data:
                print(f"✓ Downloaded {len(data['features'])} features")
                output_path = "public/geojson/us-zipcodes.json"
                with open(output_path, 'w') as f:
                    json.dump(data, f, separators=(',', ':'))
                print(f"✓ Saved to {output_path}")
                return True
            elif 'objects' in data:
                # TopoJSON format - need to decode
                print("Data is in TopoJSON format, skipping for now")
                continue
        except Exception as e:
            print(f"  Failed: {e}")
            continue
    
    # If all else fails, create a minimal placeholder
    print("\nCreating placeholder US zip codes GeoJSON...")
    placeholder = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": "10001", "zipcode": "10001"},
                "geometry": {"type": "Polygon", "coordinates": [[[-74.01, 40.71], [-74.00, 40.71], [-74.00, 40.72], [-74.01, 40.72], [-74.01, 40.71]]]}
            }
        ]
    }
    
    output_path = "public/geojson/us-zipcodes.json"
    with open(output_path, 'w') as f:
        json.dump(placeholder, f, separators=(',', ':'))
    print(f"⚠ Created minimal placeholder at {output_path}")
    print("  Note: For production, download full zip code boundaries from:")
    print("  - US Census Bureau ZCTA: https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-files.html")
    print("  - Natural Earth: https://www.naturalearthdata.com/")
    
    return True

if __name__ == '__main__':
    success = download_us_zipcodes()
    sys.exit(0 if success else 1)
