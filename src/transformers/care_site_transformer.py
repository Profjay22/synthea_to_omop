import pandas as pd
import hashlib
from typing import Optional

class CareSiteTransformer:
    """Transform provider data to extract unique care sites"""
    
    def __init__(self):
        pass
    
    def transform(self, providers_df: pd.DataFrame) -> pd.DataFrame:
        """Transform provider data to extract unique care sites"""
        
        print(f"🔄 Extracting care sites from {len(providers_df)} provider records...")
        
        # Get unique organizations
        unique_orgs = providers_df[["ORGANIZATION"]].drop_duplicates()
        unique_orgs = unique_orgs.dropna(subset=["ORGANIZATION"])
        
        if unique_orgs.empty:
            print("❌ No organizations found")
            return pd.DataFrame()
        
        print(f"✅ Found {len(unique_orgs)} unique organizations")
        
        care_sites = []
        
        for idx, row in unique_orgs.iterrows():
            try:
                care_site_record = self._transform_organization(row)
                if care_site_record:
                    care_sites.append(care_site_record)
            except Exception as e:
                print(f"⚠️ Error with organization {row['ORGANIZATION']}: {e}")
                continue
        
        if not care_sites:
            print("❌ No valid care site records created")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(care_sites)
        print(f"✅ Successfully transformed {len(result_df)} care sites")
        return result_df
    
    def _transform_organization(self, org_row: pd.Series) -> Optional[dict]:
        """Transform single organization to OMOP care_site record"""
        
        org_uuid = str(org_row["ORGANIZATION"])
        
        # Generate care_site_id from organization UUID
        care_site_id = self._uuid_to_int(org_uuid)
        
        # Create OMOP care_site record
        return {
            'care_site_id': care_site_id,
            'care_site_name': f"Healthcare Organization {org_uuid[:8]}",
            'place_of_service_concept_id': 0,  # Unknown
            'location_id': None,  # Could be set if you have org location data
            'care_site_source_value': org_uuid,
            'place_of_service_source_value': None
        }
    
    def _uuid_to_int(self, uuid_str: str) -> int:
        """Convert UUID string to deterministic 32-bit signed integer"""
        # Use first 4 bytes of MD5 hash to create 32-bit integer
        hash_bytes = hashlib.md5(str(uuid_str).encode()).digest()[:4]
        # Convert to signed 32-bit integer to avoid database issues
        unsigned_int = int.from_bytes(hash_bytes, byteorder='big', signed=False)
        # Convert to signed 32-bit range (0 to 2^31-1)
        return unsigned_int % 2147483647 + 1