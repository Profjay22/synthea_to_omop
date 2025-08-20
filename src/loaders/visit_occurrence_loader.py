import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager

OMOP_VISIT_OCCURRENCE_COLUMNS: List[str] = [
    "visit_occurrence_id",
    "person_id",
    "visit_concept_id",
    "visit_start_date",
    "visit_start_datetime",
    "visit_end_date",
    "visit_end_datetime",
    "visit_type_concept_id",
    "provider_id",
    "care_site_id",
    "visit_source_value",
    "visit_source_concept_id",
    "admitted_from_concept_id",
    "admitted_from_source_value",
    "discharged_to_concept_id",
    "discharged_to_source_value",
    "preceding_visit_occurrence_id"
]

class VisitOccurrenceLoader:
    """Loader for OMOP CDM visit_occurrence table using pandas.to_sql"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # keep only columns that exist in OMOP visit_occurrence
        cols = [c for c in OMOP_VISIT_OCCURRENCE_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_VISIT_OCCURRENCE_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        # Convert NaT -> None so DB gets NULL
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_visit_occurrences(self, visit_occurrences_df: pd.DataFrame, batch_size: Optional[int] = None) -> bool:
        if visit_occurrences_df is None or visit_occurrences_df.empty:
            print("❌ No data to load")
            return False

        try:
            df = self._align_columns(visit_occurrences_df)

            total = len(df)
            # Use smaller batch size for visit_occurrence due to many columns
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = min(100, total)  # Smaller default batch size
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} visit occurrences via to_sql "
                  f"(schema={self.db_manager.config.schema_cdm}, table=visit_occurrence, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                try:
                    # Use engine directly with pandas.to_sql
                    chunk.to_sql(
                        name="visit_occurrence",
                        con=self.db_manager.engine,
                        schema=self.db_manager.config.schema_cdm,
                        if_exists="append",
                        index=False,
                        method="multi",        # build batched INSERTs
                        chunksize=50          # Even smaller chunk size for complex inserts
                    )
                    print(f"   ✅ Batch {i+1}/{num_batches} inserted ({len(chunk)} rows).")
                except Exception as e:
                    print(f"   ❌ Batch {i+1}/{num_batches} failed: {str(e)[:200]}...")
                    # Try smaller chunks if batch fails
                    if len(chunk) > 1:
                        print(f"   🔄 Retrying batch {i+1} with smaller chunks...")
                        for j in range(0, len(chunk), 10):  # 10 rows at a time
                            mini_chunk = chunk.iloc[j:j+10]
                            mini_chunk.to_sql(
                                name="visit_occurrence",
                                con=self.db_manager.engine,
                                schema=self.db_manager.config.schema_cdm,
                                if_exists="append",
                                index=False,
                                method="multi",
                                chunksize=1
                            )
                        print(f"   ✅ Batch {i+1} completed with smaller chunks")
                    else:
                        print(f"   ❌ Single row failed, skipping...")
                        
                start = end

            print("✅ All data loaded successfully!")

            # Post-load count
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.db_manager.config.schema_cdm}.visit_occurrence")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total visit occurrences in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        print("\n🔍 Verifying loaded visit occurrence data...")
        try:
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_visits,
                COUNT(DISTINCT person_id) as unique_patients,
                COUNT(DISTINCT visit_concept_id) as visit_types,
                COUNT(DISTINCT provider_id) as unique_providers,
                COUNT(DISTINCT care_site_id) as unique_care_sites,
                MIN(visit_start_date) as earliest_visit,
                MAX(visit_start_date) as latest_visit
            FROM {self.db_manager.config.schema_cdm}.visit_occurrence
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total visits: {summary['total_visits']}")
            print(f"  Unique patients: {summary['unique_patients']}")
            print(f"  Visit types: {summary['visit_types']}")
            print(f"  Unique providers: {summary['unique_providers']}")
            print(f"  Unique care sites: {summary['unique_care_sites']}")
            print(f"  Date range: {summary['earliest_visit']} to {summary['latest_visit']}")

            sample_sql = f"""
            SELECT visit_occurrence_id, person_id, visit_concept_id, 
                   visit_start_date, visit_source_value, provider_id, care_site_id
            FROM {self.db_manager.config.schema_cdm}.visit_occurrence 
            LIMIT 5
            """
            samples = self.db_manager.execute_query(sample_sql)
            print("\n📋 Sample visit occurrence records:")
            for _, r in samples.iterrows():
                print(f"  Visit ID: {r['visit_occurrence_id']} | Person: {r['person_id']} | "
                      f"Type: {r['visit_concept_id']} | Date: {r['visit_start_date']} | "
                      f"Source: {r['visit_source_value']}")
        except Exception as e:
            print(f"❌ Verification failed: {e}")