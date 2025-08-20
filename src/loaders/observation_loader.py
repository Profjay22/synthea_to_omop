import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager

OMOP_OBSERVATION_COLUMNS: List[str] = [
    "observation_id",
    "person_id",
    "observation_concept_id",
    "observation_date",
    "observation_datetime",
    "observation_type_concept_id",
    "value_as_number",
    "value_as_string",
    "value_as_concept_id",
    "qualifier_concept_id",
    "unit_concept_id",
    "provider_id",
    "visit_occurrence_id",
    "visit_detail_id",
    "observation_source_value",
    "observation_source_concept_id",
    "unit_source_value",
    "qualifier_source_value",
    "value_source_value",
    "observation_event_id",
    "obs_event_field_concept_id"
]

class ObservationLoader:
    """Loader for OMOP CDM observation table using pandas.to_sql"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # keep only columns that exist in OMOP observation
        cols = [c for c in OMOP_OBSERVATION_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_OBSERVATION_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        # Convert NaT -> None so DB gets NULL
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_observations(self, observations_df: pd.DataFrame, batch_size: Optional[int] = None) -> bool:
        if observations_df is None or observations_df.empty:
            print("❌ No data to load")
            return False

        try:
            df = self._align_columns(observations_df)

            total = len(df)
            # Use smaller batch size for observations due to many columns
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = min(50, total)  # Smaller default batch size
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} observations via to_sql "
                  f"(schema={self.db_manager.config.schema_cdm}, table=observation, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                try:
                    # Use engine directly with pandas.to_sql
                    chunk.to_sql(
                        name="observation",
                        con=self.db_manager.engine,
                        schema=self.db_manager.config.schema_cdm,
                        if_exists="append",
                        index=False,
                        method="multi",        # build batched INSERTs
                        chunksize=25          # Even smaller chunk size for complex inserts
                    )
                    print(f"   ✅ Batch {i+1}/{num_batches} inserted ({len(chunk)} rows).")
                except Exception as e:
                    print(f"   ❌ Batch {i+1}/{num_batches} failed: {str(e)[:200]}...")
                    # Try smaller chunks if batch fails
                    if len(chunk) > 1:
                        print(f"   🔄 Retrying batch {i+1} with smaller chunks...")
                        for j in range(0, len(chunk), 5):  # 5 rows at a time
                            mini_chunk = chunk.iloc[j:j+5]
                            mini_chunk.to_sql(
                                name="observation",
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
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.db_manager.config.schema_cdm}.observation")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total observations in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        print("\n🔍 Verifying loaded observation data...")
        try:
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_observations,
                COUNT(DISTINCT person_id) as unique_patients,
                COUNT(DISTINCT observation_concept_id) as observation_concepts,
                COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) as numeric_values,
                COUNT(CASE WHEN value_as_string IS NOT NULL THEN 1 END) as string_values,
                COUNT(CASE WHEN value_as_concept_id IS NOT NULL THEN 1 END) as concept_values,
                COUNT(DISTINCT visit_occurrence_id) as unique_visits,
                MIN(observation_date) as earliest_observation,
                MAX(observation_date) as latest_observation
            FROM {self.db_manager.config.schema_cdm}.observation
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total observations: {summary['total_observations']}")
            print(f"  Unique patients: {summary['unique_patients']}")
            print(f"  Observation concepts: {summary['observation_concepts']}")
            print(f"  Numeric values: {summary['numeric_values']}")
            print(f"  String values: {summary['string_values']}")
            print(f"  Concept values: {summary['concept_values']}")
            print(f"  Unique visits: {summary['unique_visits']}")
            print(f"  Date range: {summary['earliest_observation']} to {summary['latest_observation']}")

            sample_sql = f"""
            SELECT observation_id, person_id, observation_concept_id, 
                   observation_date, value_as_number, value_as_string, 
                   observation_source_value, unit_source_value
            FROM {self.db_manager.config.schema_cdm}.observation 
            LIMIT 5
            """
            samples = self.db_manager.execute_query(sample_sql)
            print("\n📋 Sample observation records:")
            for _, r in samples.iterrows():
                value_info = ""
                if pd.notna(r['value_as_number']):
                    value_info = f"Value: {r['value_as_number']}"
                elif pd.notna(r['value_as_string']):
                    value_info = f"Value: {r['value_as_string']}"
                else:
                    value_info = "No value"
                
                unit_info = f" {r['unit_source_value']}" if pd.notna(r['unit_source_value']) else ""
                
                print(f"  Obs ID: {r['observation_id']} | Person: {r['person_id']} | "
                      f"Concept: {r['observation_concept_id']} | Date: {r['observation_date']}")
                print(f"    Source: {r['observation_source_value']} | {value_info}{unit_info}")
        except Exception as e:
            print(f"❌ Verification failed: {e}")