import os, json, hashlib, datetime
import pandas as pd
from rapidfuzz import fuzz
from dateutil import parser

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(ROOT, "data", "raw")
MASTER_CSV = os.path.join(ROOT, "data", "master_incidents.csv")
STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia"
}
STATE_NORMALIZE = {name.lower(): abbrev.lower() for abbrev, name in STATES.items()}

def normalize_state_name(state_input):
    if not state_input:
        return ""
    state = str(state_input).strip()
    return STATES.get(state.upper(), state.title())

def normalize_state_for_matching(state_input):
    if not state_input:
        return ""
    state = str(state_input).lower().strip()
    return STATE_NORMALIZE.get(state, state)

def safe_date(val):
    if not val or pd.isna(val):
        return None
    try:
        return parser.parse(str(val)).date().isoformat()
    except:
        return None

def safe_int(val):
    if not val or pd.isna(val):
        return 0
    try:
        return int(float(str(val)))
    except:
        return 0

def canonical_from_row(src_name, row):
    canonical = {
        "date": None, "city": None, "state": None, "latitude": None, "longitude": None,
        "fatalities": 0, "injuries": 0, "shooter_name": None, "shooter_age": None,
        "sources": [], "notes": ""
    }
    
    if src_name == "violence_project":
        canonical["date"] = safe_date(row.get("Full Date"))
        canonical["city"] = row.get("City")
        canonical["state"] = normalize_state_name(row.get("State"))
        canonical["latitude"] = row.get("Latitude")
        canonical["longitude"] = row.get("Longitude")
        canonical["fatalities"] = safe_int(row.get("Number Killed"))
        canonical["injuries"] = safe_int(row.get("Total Injured"))
        
        first = row.get("Shooter First Name", "")
        last = row.get("Shooter Last Name", "")
        if first or last:
            canonical["shooter_name"] = f"{first} {last}".strip()
        
        canonical["shooter_age"] = safe_int(row.get("Age"))
        
    elif src_name == "motherjones":
        canonical["date"] = safe_date(row.get("date"))
        location = row.get("location", "")
        if ", " in location:
            city, state = location.split(", ", 1)
            canonical["city"] = city
            canonical["state"] = normalize_state_name(state)
        else:
            canonical["city"] = location
            canonical["state"] = ""
        canonical["fatalities"] = safe_int(row.get("fatalities"))
        canonical["injuries"] = safe_int(row.get("injured"))
        canonical["shooter_age"] = safe_int(row.get("age_of_shooter"))
        
        summary = row.get("summary", "")
        if summary and not pd.isna(summary):
            import re
            name_match = re.match(r'^([A-Z][a-z]+ (?:[A-Z]\. )?[A-Z][a-z]+)', summary)
            if name_match:
                canonical["shooter_name"] = name_match.group(1)
        
        canonical["notes"] = summary
        
    elif src_name == "stanford_msa":
        canonical["date"] = safe_date(row.get("Date"))
        canonical["city"] = row.get("City")
        canonical["state"] = normalize_state_name(row.get("State"))
        canonical["latitude"] = row.get("Latitude") 
        canonical["longitude"] = row.get("Longitude")
        canonical["fatalities"] = safe_int(row.get("Number of Civilian Fatalities"))
        canonical["injuries"] = safe_int(row.get("Number of Civilian Injured"))
        canonical["shooter_name"] = row.get("Shooter Name")
        
    elif src_name == "gva":
        canonical["date"] = safe_date(row.get("incident_date"))
        canonical["city"] = row.get("city_or_county")
        canonical["state"] = normalize_state_name(row.get("state"))
        canonical["latitude"] = row.get("latitude")
        canonical["longitude"] = row.get("longitude")
        canonical["fatalities"] = safe_int(row.get("victims_killed"))
        canonical["injuries"] = safe_int(row.get("victims_injured"))
        canonical["notes"] = f'Address: {row.get("address", "")}'
        canonical["total_killed"] = safe_int(row.get("killed"))
        
    canonical["sources"].append({
        "source": src_name,
        "row_index": getattr(row, 'name', 0)
    })
    
    return canonical

def are_coords_close(lat1, lon1, lat2, lon2, max_miles=20):
    if not all([lat1, lon1, lat2, lon2]):
        return False
    try:
        from math import radians, cos, sin, asin, sqrt
        lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
        
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        miles = 3956 * c
        return miles <= max_miles
    except:
        return False

def make_fingerprint(canonical):
    date = canonical.get("date") or ""
    
    fatalities = int(canonical.get("fatalities") or 0)
    if fatalities == 0:
        fat_range = "0"
    elif fatalities <= 3:
        fat_range = "1-3"
    elif fatalities <= 7:
        fat_range = "4-7"
    elif fatalities <= 15:
        fat_range = "8-15"
    else:
        fat_range = "15+"
    
    state = normalize_state_for_matching(canonical.get("state"))
    key = f"{date}|{state}|{fat_range}"
    return hashlib.sha256(key.encode()).hexdigest()

def read_and_normalize():
    canonical_rows = []
    
    for filename in os.listdir(RAW_DIR):
        if not filename.endswith('.csv'):
            continue
            
        filepath = os.path.join(RAW_DIR, filename)
        src_name = filename.replace('.csv', '').lower()
        
        print(f"Processing {filename}...")
        
        try:
            df = pd.read_csv(filepath, dtype=str, low_memory=False)
        except:
            try:
                df = pd.read_csv(filepath, dtype=str, encoding='latin-1', low_memory=False)
            except Exception as e:
                print(f"Failed to read {filename}: {e}")
                continue
        
        for _, row in df.iterrows():
            canonical = canonical_from_row(src_name, row)
            canonical["fingerprint"] = make_fingerprint(canonical)
            canonical_rows.append(canonical)
    
    return canonical_rows

def deduplicate_and_merge(rows):
    by_fingerprint = {}
    for row in rows:
        fp = row["fingerprint"]
        if fp not in by_fingerprint:
            row["merged_from"] = [row["sources"]]
            by_fingerprint[fp] = row
        else:
            existing = by_fingerprint[fp]
            for key in ["date", "city", "state", "latitude", "longitude", "shooter_name", "shooter_age"]:
                if not existing.get(key) and row.get(key):
                    existing[key] = row[key]
            for key in ["fatalities", "injuries", "total_killed"]:
                if key in row and (not existing.get(key) or row.get(key, 0) > existing.get(key, 0)):
                    existing[key] = row[key]
            notes_set = set(filter(None, [existing.get("notes", ""), row.get("notes", "")]))
            existing["notes"] = " | ".join(notes_set)
            existing["sources"].extend(row["sources"])
            existing["merged_from"].append(row["sources"])
    return list(by_fingerprint.values())

def fuzzy_merge_pass(rows):
    merged = []
    used = set()
    
    for i, row_a in enumerate(rows):
        if i in used:
            continue
        group = row_a.copy()
        
        for j, row_b in enumerate(rows[i+1:], i+1):
            if j in used:
                continue
                
            date_a = row_a.get("date")
            date_b = row_b.get("date")
            if not date_a or not date_b:
                continue
            
            date_match = False
            if date_a == date_b:
                date_match = True
            else:
                try:
                    from datetime import datetime
                    dt_a = datetime.fromisoformat(date_a)
                    dt_b = datetime.fromisoformat(date_b)
                    days_diff = abs((dt_a - dt_b).days)
                    date_match = days_diff <= 1
                except:
                    date_match = False
            
            if not date_match:
                continue
                
            city_a = (row_a.get("city") or "").lower().strip()
            city_b = (row_b.get("city") or "").lower().strip()
            
            city_a_parts = set(city_a.replace("-", " ").replace(",", " ").split())
            city_b_parts = set(city_b.replace("-", " ").replace(",", " ").split())
            city_overlap = len(city_a_parts & city_b_parts) > 0
            
            state_a = normalize_state_for_matching(row_a.get("state"))
            state_b = normalize_state_for_matching(row_b.get("state"))
            state_match = state_a == state_b or fuzz.ratio(state_a, state_b) > 80
            
            fat_a, fat_b = int(row_a.get("fatalities", 0)), int(row_b.get("fatalities", 0))
            inj_a, inj_b = int(row_a.get("injuries", 0)), int(row_b.get("injuries", 0))
            
            fat_close = fat_a == fat_b or abs(fat_a - fat_b) <= 2
            inj_close = abs(inj_a - inj_b) <= 5 or min(inj_a, inj_b) == 0
            casualties_close = fat_close and inj_close
            
            coords_close = are_coords_close(
                row_a.get("latitude"), row_a.get("longitude"),
                row_b.get("latitude"), row_b.get("longitude"), 
                max_miles=30
            )
            
            name_a = (row_a.get("shooter_name") or "").lower().strip()
            name_b = (row_b.get("shooter_name") or "").lower().strip()
            name_match = False
            if name_a and name_b:
                name_sim = fuzz.ratio(name_a, name_b)
                name_parts_a = set(name_a.split())
                name_parts_b = set(name_b.split())
                name_overlap = len(name_parts_a & name_parts_b) >= 2
                name_contained = name_a in name_b or name_b in name_a
                name_match = name_sim > 70 or name_overlap or name_contained
            
            condition1 = (state_match and casualties_close and 
                         (city_overlap or coords_close or name_match))
            condition2 = (state_match and name_match and name_a and name_b)
            
            if condition1 or condition2:
                for key in ["city", "state", "latitude", "longitude", "shooter_name", "shooter_age"]:
                    if not group.get(key) and row_b.get(key):
                        group[key] = row_b[key]
                
                group["fatalities"] = max(fat_a, fat_b)
                group["injuries"] = max(inj_a, inj_b)
                if "total_killed" in group or "total_killed" in row_b:
                    group["total_killed"] = max(
                        group.get("total_killed", 0), 
                        row_b.get("total_killed", 0)
                    )
                
                notes_set = set(filter(None, [group.get("notes", ""), row_b.get("notes", "")]))
                group["notes"] = " | ".join(notes_set)
                group["sources"].extend(row_b["sources"])
                group["merged_from"].extend(row_b.get("merged_from", []))
                used.add(j)
        
        merged.append(group)
        used.add(i)
    
    return merged

def save_master_csv(rows):
    if not rows:
        print("No data to save")
        return
    
    for i, row in enumerate(rows, 1):
        row["master_id"] = i
        row["created_at"] = datetime.datetime.utcnow().isoformat()
        row["sources"] = json.dumps(row["sources"])
        row["merged_from"] = json.dumps(row["merged_from"])
    
    df = pd.DataFrame(rows)
    
    schema_cols = [
        "master_id", "fingerprint", "date", "city", "state", 
        "latitude", "longitude", "fatalities", "injuries",
        "shooter_name", "shooter_age",
        "sources", "merged_from", "notes", "created_at"
    ]
    
    for col in schema_cols:
        if col not in df.columns:
            df[col] = ""
    
    df[schema_cols].to_csv(MASTER_CSV, index=False)
    print(f"Saved {len(df)} incidents to {MASTER_CSV}")

def main():
    print("Reading and normalizing source files...")
    canonical_rows = read_and_normalize()
    print(f"Normalized {len(canonical_rows)} total rows")
    
    print("Deduplicating by fingerprint...")
    deduplicated = deduplicate_and_merge(canonical_rows)
    print(f"After deduplication: {len(deduplicated)} incidents")
    
    print("Fuzzy merging similar incidents...")
    final_merged = fuzzy_merge_pass(deduplicated)
    print(f"After fuzzy merge: {len(final_merged)} incidents")
    
    print("Saving master CSV...")
    save_master_csv(final_merged)
    print("Done!")

if __name__ == "__main__":
    main()
