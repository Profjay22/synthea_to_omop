import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager

OMOP_OBSERVATION_PERIOD_COLUMNS: List[str] = [
    "observation_period_id",
    "person_id",
    "observation_period_start_date",
    "observation_period_end_date",
    "period_type_concept_id"
]

class ObservationPeriodLoader:
    """Loader for OMOP CDM observation_period table using pandas.to_sql"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # keep only columns that exist in OMOP observation_period
        cols = [c for c in OMOP_OBSERVATION_PERIOD_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_OBSERVATION_PERIOD_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        # Convert NaT -> None so DB gets NULL
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_observation_periods(self, periods_df: pd.DataFrame, batch_size: Optional[int] = None) -> bool:
        if periods_df is None or periods_df.empty:
            print("❌ No data to load")
            return False

        try:
            df = self._align_columns(periods_df)

            total = len(df)
            # Observation periods are typically small dataset, use reasonable batch size
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = min(500, total)
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} observation periods via to_sql "
                  f"(schema={self.db_manager.config.schema_cdm}, table=observation_period, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                try:
                    # Use engine directly with pandas.to_sql
                    chunk.to_sql(
                        name="observation_period",
                        con=self.db_manager.engine,
                        schema=self.db_manager.config.schema_cdm,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=len(chunk)
                    )
                    print(f"   ✅ Batch {i+1}/{num_batches} inserted ({len(chunk)} rows).")
                except Exception as e:
                    print(f"   ❌ Batch {i+1}/{num_batches} failed: {str(e)[:200]}...")
                    return False
                        
                start = end

            print("✅ All data loaded successfully!")

            # Post-load count
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.db_manager.config.schema_cdm}.observation_period")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total observation periods in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        print("\n🔍 Verifying loaded observation period data...")
        try:
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_periods,
                COUNT(DISTINCT person_id) as unique_patients,
                MIN(observation_period_start_date) as earliest_start,
                MAX(observation_period_end_date) as latest_end,
                AVG(observation_period_end_date - observation_period_start_date) as avg_period_length,
                COUNT(CASE WHEN observation_period_start_date = observation_period_end_date THEN 1 END) as single_day_periods,
                COUNT(DISTINCT period_type_concept_id) as period_types
            FROM {self.db_manager.config.schema_cdm}.observation_period
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total periods: {summary['total_periods']}")
            print(f"  Unique patients: {summary['unique_patients']}")
            print(f"  Date range: {summary['earliest_start']} to {summary['latest_end']}")
            print(f"  Average period length: {summary['avg_period_length']}")
            print(f"  Single-day periods: {summary['single_day_periods']}")
            print(f"  Period types: {summary['period_types']}")

            # Check for data quality issues
            quality_sql = f"""
            SELECT 
                COUNT(CASE WHEN observation_period_start_date > observation_period_end_date THEN 1 END) as invalid_date_ranges,
                COUNT(CASE WHEN observation_period_start_date IS NULL OR observation_period_end_date IS NULL THEN 1 END) as null_dates
            FROM {self.db_manager.config.schema_cdm}.observation_period
            """
            quality = self.db_manager.execute_query(quality_sql).iloc[0]
            
            if quality['invalid_date_ranges'] > 0:
                print(f"  ⚠️ Invalid date ranges: {quality['invalid_date_ranges']}")
            if quality['null_dates'] > 0:
                print(f"  ⚠️ NULL dates: {quality['null_dates']}")

            sample_sql = f"""
            SELECT observation_period_id, person_id, 
                   observation_period_start_date, observation_period_end_date,
                   (observation_period_end_date - observation_period_start_date) as period_length
            FROM {self.db_manager.config.schema_cdm}.observation_period 
            ORDER BY observation_period_start_date
            LIMIT 5
            """
            samples = self.db_manager.execute_query(sample_sql)
            print("\n📋 Sample observation period records:")
            for _, r in samples.iterrows():
                print(f"  Period ID: {r['observation_period_id']} | Person: {r['person_id']} | "
                      f"{r['observation_period_start_date']} to {r['observation_period_end_date']} | "
                      f"Length: {r['period_length']}")
                      
            # Check for multiple periods per person (should be 1:1)
            duplicate_sql = f"""
            SELECT person_id, COUNT(*) as period_count
            FROM {self.db_manager.config.schema_cdm}.observation_period
            GROUP BY person_id
            HAVING COUNT(*) > 1
            LIMIT 5
            """
            duplicates = self.db_manager.execute_query(duplicate_sql)
            if not duplicates.empty:
                print(f"\n⚠️ Found {len(duplicates)} persons with multiple observation periods:")
                for _, r in duplicates.iterrows():
                    print(f"  Person {r['person_id']}: {r['period_count']} periods")
            else:
                print("\n✅ Confirmed: One observation period per person")
                
        except Exception as e:
            print(f"❌ Verification failed: {e}")