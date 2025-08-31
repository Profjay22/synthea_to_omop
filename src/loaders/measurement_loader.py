import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager

OMOP_MEASUREMENT_COLUMNS: List[str] = [
    "measurement_id",
    "person_id",
    "measurement_concept_id",
    "measurement_date",
    "measurement_datetime",
    "measurement_time",
    "measurement_type_concept_id",
    "operator_concept_id",
    "value_as_number",
    "value_as_concept_id",
    "unit_concept_id",
    "range_low",
    "range_high",
    "provider_id",
    "visit_occurrence_id",
    "visit_detail_id",
    "measurement_source_value",
    "measurement_source_concept_id",
    "unit_source_value",
    "unit_source_concept_id",
    "value_source_value",
    "measurement_event_id",
    "meas_event_field_concept_id"
]

class MeasurementLoader:
    """Loader for OMOP CDM measurement table using pandas.to_sql"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Keep only columns that exist in OMOP measurement table
        cols = [c for c in OMOP_MEASUREMENT_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_MEASUREMENT_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        # Convert NaT -> None so DB gets NULL
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_measurements(self, measurements_df: pd.DataFrame, batch_size: Optional[int] = None) -> bool:
        if measurements_df is None or measurements_df.empty:
            print("❌ No data to load")
            return False

        try:
            df = self._align_columns(measurements_df)

            total = len(df)
            # Use appropriate batch size for measurements
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = min(200, total)  # Good batch size for measurements
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} measurements via to_sql "
                  f"(schema={self.db_manager.config.schema_cdm}, table=measurement, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                try:
                    # Use engine directly with pandas.to_sql
                    chunk.to_sql(
                        name="measurement",
                        con=self.db_manager.engine,
                        schema=self.db_manager.config.schema_cdm,
                        if_exists="append",
                        index=False,
                        method="multi",        # Build batched INSERTs
                        chunksize=75          # Good chunk size for measurements
                    )
                    print(f"   ✅ Batch {i+1}/{num_batches} inserted ({len(chunk)} rows).")
                except Exception as e:
                    print(f"   ❌ Batch {i+1}/{num_batches} failed: {str(e)[:200]}...")
                    # Try smaller chunks if batch fails
                    if len(chunk) > 1:
                        print(f"   🔄 Retrying batch {i+1} with smaller chunks...")
                        for j in range(0, len(chunk), 25):  # 25 rows at a time
                            mini_chunk = chunk.iloc[j:j+25]
                            mini_chunk.to_sql(
                                name="measurement",
                                con=self.db_manager.engine,
                                schema=self.db_manager.config.schema_cdm,
                                if_exists="append",
                                index=False,
                                method="multi",
                                chunksize=10
                            )
                        print(f"   ✅ Batch {i+1} completed with smaller chunks")
                    else:
                        print(f"   ❌ Single row failed, skipping...")
                        
                start = end

            print("✅ All data loaded successfully!")

            # Post-load count
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.db_manager.config.schema_cdm}.measurement")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total measurements in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        print("\n🔍 Verifying loaded measurement data...")
        try:
            # Enhanced summary with measurement analysis
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_measurements,
                COUNT(DISTINCT person_id) as unique_patients,
                COUNT(DISTINCT measurement_concept_id) as measurement_concepts,
                COUNT(DISTINCT visit_occurrence_id) as unique_visits,
                COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) as numeric_values,
                COUNT(CASE WHEN value_as_concept_id IS NOT NULL THEN 1 END) as concept_values,
                COUNT(CASE WHEN operator_concept_id IS NOT NULL THEN 1 END) as with_operators,
                COUNT(CASE WHEN unit_concept_id IS NOT NULL THEN 1 END) as with_units,
                AVG(value_as_number) as avg_numeric_value,
                MIN(measurement_date) as earliest_measurement,
                MAX(measurement_date) as latest_measurement
            FROM {self.db_manager.config.schema_cdm}.measurement
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total measurements: {summary['total_measurements']}")
            print(f"  Unique patients: {summary['unique_patients']}")
            print(f"  Measurement concepts: {summary['measurement_concepts']}")
            print(f"  Unique visits: {summary['unique_visits']}")
            print(f"  Numeric values: {summary['numeric_values']}")
            print(f"  Concept values: {summary['concept_values']}")
            print(f"  With operators: {summary['with_operators']}")
            print(f"  With units: {summary['with_units']}")
            if pd.notna(summary['avg_numeric_value']):
                print(f"  Average numeric value: {summary['avg_numeric_value']:.2f}")
            print(f"  Date range: {summary['earliest_measurement']} to {summary['latest_measurement']}")

            # Vocabulary breakdown for measurements
            vocab_sql = f"""
            SELECT 
                c.vocabulary_id,
                COUNT(*) as measurement_count,
                COUNT(DISTINCT m.person_id) as unique_patients
            FROM {self.db_manager.config.schema_cdm}.measurement m
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON m.measurement_concept_id = c.concept_id
            WHERE c.vocabulary_id IS NOT NULL
            GROUP BY c.vocabulary_id
            ORDER BY measurement_count DESC
            """
            vocab_breakdown = self.db_manager.execute_query(vocab_sql)
            if not vocab_breakdown.empty:
                print("\n📊 Vocabulary breakdown:")
                for _, row in vocab_breakdown.iterrows():
                    print(f"  {row['vocabulary_id']}: {row['measurement_count']} measurements ({row['unique_patients']} patients)")

            # Top measurement types
            measurement_types_sql = f"""
            SELECT 
                m.measurement_concept_id,
                c.concept_name,
                c.vocabulary_id,
                COUNT(*) as measurement_count,
                COUNT(DISTINCT m.person_id) as unique_patients,
                AVG(CASE WHEN m.value_as_number IS NOT NULL THEN m.value_as_number END) as avg_value
            FROM {self.db_manager.config.schema_cdm}.measurement m
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON m.measurement_concept_id = c.concept_id
            WHERE m.measurement_concept_id > 0
            GROUP BY m.measurement_concept_id, c.concept_name, c.vocabulary_id
            ORDER BY measurement_count DESC
            LIMIT 10
            """
            top_measurements = self.db_manager.execute_query(measurement_types_sql)
            if not top_measurements.empty:
                print("\n📊 Top 10 measurement types:")
                for _, row in top_measurements.iterrows():
                    vocab_info = f" ({row['vocabulary_id']})" if pd.notna(row['vocabulary_id']) else ""
                    avg_info = f" | Avg: {row['avg_value']:.2f}" if pd.notna(row['avg_value']) else ""
                    print(f"  {row['concept_name']}{vocab_info} - {row['measurement_count']} measurements ({row['unique_patients']} patients){avg_info}")

            # Unit distribution
            unit_sql = f"""
            SELECT 
                uc.concept_name as unit_name,
                COUNT(*) as measurement_count
            FROM {self.db_manager.config.schema_cdm}.measurement m
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept uc 
                ON m.unit_concept_id = uc.concept_id
            WHERE m.unit_concept_id IS NOT NULL
            GROUP BY uc.concept_name
            ORDER BY measurement_count DESC
            LIMIT 5
            """
            unit_dist = self.db_manager.execute_query(unit_sql)
            if not unit_dist.empty:
                print("\n📊 Top 5 units of measurement:")
                for _, row in unit_dist.iterrows():
                    print(f"  {row['unit_name']}: {row['measurement_count']} measurements")

            # Sample measurement records with enhanced details
            sample_sql = f"""
            SELECT 
                m.measurement_id,
                m.person_id,
                m.measurement_concept_id,
                mc.concept_name as measurement_name,
                mc.vocabulary_id,
                m.measurement_date,
                m.value_as_number,
                m.value_as_concept_id,
                vc.concept_name as value_concept_name,
                m.unit_concept_id,
                uc.concept_name as unit_name,
                m.operator_concept_id,
                oc.concept_name as operator_name,
                m.measurement_source_value,
                m.visit_occurrence_id
            FROM {self.db_manager.config.schema_cdm}.measurement m
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept mc 
                ON m.measurement_concept_id = mc.concept_id
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept vc 
                ON m.value_as_concept_id = vc.concept_id
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept uc 
                ON m.unit_concept_id = uc.concept_id
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept oc 
                ON m.operator_concept_id = oc.concept_id
            ORDER BY m.measurement_date
            LIMIT 5
            """
            samples = self.db_manager.execute_query(sample_sql)
            print("\n📋 Sample measurement records:")
            for _, r in samples.iterrows():
                # Format measurement info
                measurement_info = f"{r['measurement_name']}" if pd.notna(r['measurement_name']) else f"Concept {r['measurement_concept_id']}"
                vocab_info = f" ({r['vocabulary_id']})" if pd.notna(r['vocabulary_id']) else ""
                
                # Format value info
                value_info = ""
                if pd.notna(r['value_as_number']):
                    operator_info = f"{r['operator_name']} " if pd.notna(r['operator_name']) else ""
                    unit_info = f" {r['unit_name']}" if pd.notna(r['unit_name']) else ""
                    value_info = f" | Value: {operator_info}{r['value_as_number']}{unit_info}"
                elif pd.notna(r['value_concept_name']):
                    value_info = f" | Value: {r['value_concept_name']}"
                
                print(f"  Measurement ID: {r['measurement_id']} | Person: {r['person_id']}")
                print(f"    {measurement_info}{vocab_info} | Date: {r['measurement_date']}{value_info}")
                print(f"    Source: {r['measurement_source_value']} | Visit: {r['visit_occurrence_id']}")
                
        except Exception as e:
            print(f"❌ Verification failed: {e}")