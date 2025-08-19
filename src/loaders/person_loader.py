import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager

OMOP_PERSON_COLUMNS: List[str] = [
    "person_id",
    "gender_concept_id",
    "year_of_birth",
    "month_of_birth",
    "day_of_birth",
    "birth_datetime",
    "race_concept_id",
    "ethnicity_concept_id",
    "location_id",
    "provider_id",
    "care_site_id",
    "person_source_value",
    "gender_source_value",
    "gender_source_concept_id",
    "race_source_value",
    "race_source_concept_id",
    "ethnicity_source_value",
    "ethnicity_source_concept_id",
]

class PersonLoader:
    """Loader for OMOP CDM person table using pandas.to_sql (robust, no param-shape issues)."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # keep only columns that exist in OMOP person
        cols = [c for c in OMOP_PERSON_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_PERSON_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        # Convert NaT -> None so DB gets NULL
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_persons(self, persons_df: pd.DataFrame, batch_size: Optional[int] = None) -> bool:
        if persons_df is None or persons_df.empty:
            print("❌ No data to load")
            return False

        try:
            df = self._align_columns(persons_df)

            total = len(df)
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = total
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} persons via to_sql "
                  f"(schema={self.db_manager.config.schema_cdm}, table=person, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                # IMPORTANT: use engine directly; do NOT go through bulk_insert here
                chunk.to_sql(
                    name="person",
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
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.db_manager.config.schema_cdm}.person")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total persons in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        print("\n🔍 Verifying loaded data...")
        try:
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_persons,
                COUNT(DISTINCT gender_concept_id) as gender_concepts,
                COUNT(DISTINCT race_concept_id) as race_concepts,
                MIN(year_of_birth) as min_birth_year,
                MAX(year_of_birth) as max_birth_year
            FROM {self.db_manager.config.schema_cdm}.person
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total persons: {summary['total_persons']}")
            print(f"  Gender concepts: {summary['gender_concepts']}")
            print(f"  Race concepts: {summary['race_concepts']}")
            print(f"  Birth years: {summary['min_birth_year']} - {summary['max_birth_year']}")

            sample_sql = f"""
            SELECT person_id, gender_concept_id, year_of_birth, race_concept_id, 
                   person_source_value, gender_source_value, race_source_value
            FROM {self.db_manager.config.schema_cdm}.person 
            LIMIT 5
            """
            samples = self.db_manager.execute_query(sample_sql)
            print("\n📋 Sample records:")
            for _, r in samples.iterrows():
                print(f"  ID: {r['person_id']} | Gender: {r['gender_concept_id']} | "
                      f"Birth: {r['year_of_birth']} | Race: {r['race_concept_id']}")
        except Exception as e:
            print(f"❌ Verification failed: {e}")
