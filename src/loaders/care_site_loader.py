import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager

OMOP_CARE_SITE_COLUMNS: List[str] = [
    "care_site_id",
    "care_site_name",
    "place_of_service_concept_id",
    "location_id",
    "care_site_source_value",
    "place_of_service_source_value",
]

class CareSiteLoader:
    """Loader for OMOP CDM care_site table using pandas.to_sql (robust, no param-shape issues)."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # keep only columns that exist in OMOP care_site
        cols = [c for c in OMOP_CARE_SITE_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_CARE_SITE_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        # Convert NaT -> None so DB gets NULL
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_care_sites(self, care_sites_df: pd.DataFrame, batch_size: Optional[int] = None) -> bool:
        if care_sites_df is None or care_sites_df.empty:
            print("❌ No data to load")
            return False

        try:
            df = self._align_columns(care_sites_df)

            total = len(df)
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = total
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} care sites via to_sql "
                  f"(schema={self.db_manager.config.schema_cdm}, table=care_site, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                # IMPORTANT: use engine directly; do NOT go through bulk_insert here
                chunk.to_sql(
                    name="care_site",
                    con=self.db_manager.engine,
                    schema=self.db_manager.config.schema_cdm,
                    if_exists="append",
                    index=False,
                    method="multi",        # build batched INSERTs
                    chunksize=len(chunk)   # one SQL per chunk
                )
                print(f"   ✅ Batch {i+1}/{num_batches} inserted ({len(chunk)} rows).")
                start = end

            print("✅ All data loaded successfully!")

            # Post-load count
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.db_manager.config.schema_cdm}.care_site")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total care sites in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        print("\n🔍 Verifying loaded care site data...")
        try:
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_care_sites,
                COUNT(DISTINCT place_of_service_concept_id) as service_concepts,
                COUNT(CASE WHEN location_id IS NOT NULL THEN 1 END) as with_location
            FROM {self.db_manager.config.schema_cdm}.care_site
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total care sites: {summary['total_care_sites']}")
            print(f"  Service concepts: {summary['service_concepts']}")
            print(f"  With location: {summary['with_location']}")

            sample_sql = f"""
            SELECT care_site_id, care_site_name, place_of_service_concept_id, 
                   location_id, care_site_source_value
            FROM {self.db_manager.config.schema_cdm}.care_site 
            LIMIT 5
            """
            samples = self.db_manager.execute_query(sample_sql)
            print("\n📋 Sample care site records:")
            for _, r in samples.iterrows():
                print(f"  ID: {r['care_site_id']} | Name: {r['care_site_name'][:30]}... | "
                      f"Service: {r['place_of_service_concept_id']} | Location: {r['location_id']}")
        except Exception as e:
            print(f"❌ Verification failed: {e}")