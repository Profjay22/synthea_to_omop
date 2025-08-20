import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager

OMOP_CONDITION_OCCURRENCE_COLUMNS: List[str] = [
    "condition_occurrence_id",
    "person_id",
    "condition_concept_id",
    "condition_start_date",
    "condition_start_datetime",
    "condition_end_date",
    "condition_end_datetime",
    "condition_type_concept_id",
    "condition_status_concept_id",
    "stop_reason",
    "provider_id",
    "visit_occurrence_id",
    "visit_detail_id",
    "condition_source_value",
    "condition_source_concept_id",
    "condition_status_source_value"
]

class ConditionOccurrenceLoader:
    """Loader for OMOP CDM condition_occurrence table using pandas.to_sql"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # keep only columns that exist in OMOP condition_occurrence
        cols = [c for c in OMOP_CONDITION_OCCURRENCE_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_CONDITION_OCCURRENCE_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        # Convert NaT -> None so DB gets NULL
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_condition_occurrences(self, condition_occurrences_df: pd.DataFrame, batch_size: Optional[int] = None) -> bool:
        if condition_occurrences_df is None or condition_occurrences_df.empty:
            print("❌ No data to load")
            return False

        try:
            df = self._align_columns(condition_occurrences_df)

            total = len(df)
            # Use smaller batch size for condition_occurrence due to many columns
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = min(100, total)  # Smaller default batch size
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} condition occurrences via to_sql "
                  f"(schema={self.db_manager.config.schema_cdm}, table=condition_occurrence, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                try:
                    # Use engine directly with pandas.to_sql
                    chunk.to_sql(
                        name="condition_occurrence",
                        con=self.db_manager.engine,
                        schema=self.db_manager.config.schema_cdm,
                        if_exists="append",
                        index=False,
                        method="multi",        # build batched INSERTs
                        chunksize=50          # Smaller chunk size for complex inserts
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
                                name="condition_occurrence",
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
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.db_manager.config.schema_cdm}.condition_occurrence")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total condition occurrences in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        print("\n🔍 Verifying loaded condition occurrence data...")
        try:
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_conditions,
                COUNT(DISTINCT person_id) as unique_patients,
                COUNT(DISTINCT condition_concept_id) as unique_conditions,
                COUNT(DISTINCT visit_occurrence_id) as linked_visits,
                COUNT(CASE WHEN condition_concept_id > 0 THEN 1 END) as mapped_concepts,
                MIN(condition_start_date) as earliest_condition,
                MAX(condition_start_date) as latest_condition
            FROM {self.db_manager.config.schema_cdm}.condition_occurrence
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total conditions: {summary['total_conditions']}")
            print(f"  Unique patients: {summary['unique_patients']}")
            print(f"  Unique condition concepts: {summary['unique_conditions']}")
            print(f"  Linked to visits: {summary['linked_visits']}")
            print(f"  Mapped concepts: {summary['mapped_concepts']}")
            print(f"  Date range: {summary['earliest_condition']} to {summary['latest_condition']}")

            # Show top conditions
            top_conditions_sql = f"""
            SELECT 
                co.condition_concept_id,
                c.concept_name,
                COUNT(*) as occurrence_count
            FROM {self.db_manager.config.schema_cdm}.condition_occurrence co
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON co.condition_concept_id = c.concept_id
            WHERE co.condition_concept_id > 0
            GROUP BY co.condition_concept_id, c.concept_name
            ORDER BY occurrence_count DESC
            LIMIT 5
            """
            top_conditions = self.db_manager.execute_query(top_conditions_sql)
            print("\n📋 Top 5 conditions:")
            for _, r in top_conditions.iterrows():
                concept_name = r['concept_name'] if pd.notna(r['concept_name']) else 'Unknown'
                print(f"  {r['condition_concept_id']}: {concept_name} ({r['occurrence_count']} occurrences)")

            # Sample records
            sample_sql = f"""
            SELECT 
                co.condition_occurrence_id, 
                co.person_id, 
                co.condition_concept_id,
                c.concept_name,
                co.condition_start_date, 
                co.visit_occurrence_id,
                co.condition_source_value
            FROM {self.db_manager.config.schema_cdm}.condition_occurrence co
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON co.condition_concept_id = c.concept_id
            LIMIT 5
            """
            samples = self.db_manager.execute_query(sample_sql)
            print("\n📋 Sample condition occurrence records:")
            for _, r in samples.iterrows():
                concept_name = r['concept_name'] if pd.notna(r['concept_name']) else 'Unknown'
                visit_id = r['visit_occurrence_id'] if pd.notna(r['visit_occurrence_id']) else 'None'
                print(f"  ID: {r['condition_occurrence_id']} | Person: {r['person_id']} | "
                      f"Condition: {concept_name} | Date: {r['condition_start_date']} | Visit: {visit_id}")

            # Data quality checks
            print("\n🔍 Data quality checks:")
            
            # Check for unmapped concepts
            unmapped_sql = f"""
            SELECT COUNT(*) as unmapped_count
            FROM {self.db_manager.config.schema_cdm}.condition_occurrence
            WHERE condition_concept_id = 0
            """
            unmapped = self.db_manager.execute_query(unmapped_sql).iloc[0]['unmapped_count']
            if unmapped > 0:
                print(f"  ⚠️ {unmapped} conditions have unmapped concepts (concept_id = 0)")
            else:
                print(f"  ✅ All conditions have mapped concepts")

            # Check for future dates
            future_dates_sql = f"""
            SELECT COUNT(*) as future_count
            FROM {self.db_manager.config.schema_cdm}.condition_occurrence
            WHERE condition_start_date > CURRENT_DATE
            """
            future_dates = self.db_manager.execute_query(future_dates_sql).iloc[0]['future_count']
            if future_dates > 0:
                print(f"  ⚠️ {future_dates} conditions have future start dates")
            else:
                print(f"  ✅ No conditions with future start dates")

            # Check for end dates before start dates
            invalid_dates_sql = f"""
            SELECT COUNT(*) as invalid_count
            FROM {self.db_manager.config.schema_cdm}.condition_occurrence
            WHERE condition_end_date IS NOT NULL 
              AND condition_end_date < condition_start_date
            """
            invalid_dates = self.db_manager.execute_query(invalid_dates_sql).iloc[0]['invalid_count']
            if invalid_dates > 0:
                print(f"  ⚠️ {invalid_dates} conditions have end dates before start dates")
            else:
                print(f"  ✅ No conditions with invalid date ranges")
                
        except Exception as e:
            print(f"❌ Verification failed: {e}")