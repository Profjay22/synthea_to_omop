import pandas as pd
import hashlib
from src.database.connection import DatabaseManager


class ProviderTransformer:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset=["Id", "GENDER", "ADDRESS", "CITY", "STATE", "ZIP"])

        # Ensure ZIP is a 5-digit string
        df["ZIP"] = df["ZIP"].astype(str).str.zfill(5)

        # Convert UUID to deterministic integer using hash
        df["provider_id"] = df["Id"].apply(self._uuid_to_int)
        
        # Keep original UUID as source value
        df["provider_source_value"] = df["Id"].astype(str)
        
        # Store organization UUID for care site conversion
        df["organization_uuid"] = df["ORGANIZATION"].astype(str)

        # Rename columns for OMOP
        df = df.rename(columns={
            "NAME": "provider_name",
            "GENDER": "gender_source_value",
            "SPECIALITY": "specialty_source_value"
        })

        # Gender mapping
        df["gender_concept_id"] = df["gender_source_value"].map({
            "M": 8507,
            "F": 8532
        }).fillna(0).astype(int)

        # Match location (foreign key lookup)
        df["location_id"] = df.apply(self._lookup_location_id, axis=1)

        # Convert organization UUID to care_site_id (direct mapping - no lookup needed)
        df["care_site_id"] = df["organization_uuid"].apply(
            lambda x: self._uuid_to_int(x) if pd.notna(x) and x != "" else None
        )

        df_omop = df[[
            "provider_id",
            "provider_name",
            "provider_source_value",
            "specialty_source_value",
            "gender_concept_id",
            "gender_source_value",
            "care_site_id",
            "location_id"
        ]].copy()

        return df_omop

    def _uuid_to_int(self, uuid_str: str) -> int:
        """Convert UUID string to deterministic 32-bit signed integer"""
        # Use first 4 bytes of MD5 hash to create 32-bit integer
        hash_bytes = hashlib.md5(str(uuid_str).encode()).digest()[:4]
        # Convert to signed 32-bit integer to avoid database issues
        unsigned_int = int.from_bytes(hash_bytes, byteorder='big', signed=False)
        # Convert to signed 32-bit range (0 to 2^31-1)
        return unsigned_int % 2147483647 + 1

    def _lookup_location_id(self, row):
        """Lookup location_id using address information (foreign key)"""
        query = f"""
            SELECT location_id FROM {self.db_manager.config.schema_cdm}.location
            WHERE address_1 = %(address)s
              AND city = %(city)s
              AND state = %(state)s
              AND zip = %(zip)s
            LIMIT 1
        """
        try:
            result = self.db_manager.execute_query(query, {
                "address": row["ADDRESS"],
                "city": row["CITY"],
                "state": row["STATE"],
                "zip": row["ZIP"]
            })

            if not result.empty:
                return result.iloc[0]["location_id"]
            else:
                return None
        except Exception:
            return None