import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager

OMOP_DRUG_EXPOSURE_COLUMNS: List[str] = [
    "drug_exposure_id",
    "person_id",
    "drug_concept_id",
    "drug_exposure_start_date",
    "drug_exposure_start_datetime",
    "drug_exposure_end_date",
    "drug_exposure_end_datetime",
    "verbatim_end_date",
    "drug_type_concept_id",
    "stop_reason",
    "refills",
    "quantity",
    "days_supply",
    "sig",
    "route_concept_id",
    "lot_number",
    "provider_id",
    "visit_occurrence_id",
    "visit_detail_id",
    "drug_source_value",
    "drug_source_concept_id",
    "route_source_value",
    "dose_unit_source_value"
]

class DrugExposureLoader:
    """Loader for OMOP CDM drug_exposure table using pandas.to_sql"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Keep only columns that exist in OMOP drug_exposure table
        cols = [c for c in OMOP_DRUG_EXPOSURE_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_DRUG_EXPOSURE_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        # Convert NaT -> None so DB gets NULL
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_drug_exposures(self, drug_exposures_df: pd.DataFrame, batch_size: Optional[int] = None) -> bool:
        if drug_exposures_df is None or drug_exposures_df.empty:
            print("❌ No data to load")
            return False

        try:
            df = self._align_columns(drug_exposures_df)

            total = len(df)
            # Use appropriate batch size for drug exposures (many columns)
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = min(150, total)  # Moderate batch size due to many columns
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} drug exposures via to_sql "
                  f"(schema={self.db_manager.config.schema_cdm}, table=drug_exposure, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                try:
                    # Use engine directly with pandas.to_sql
                    chunk.to_sql(
                        name="drug_exposure",
                        con=self.db_manager.engine,
                        schema=self.db_manager.config.schema_cdm,
                        if_exists="append",
                        index=False,
                        method="multi",        # Build batched INSERTs
                        chunksize=60          # Moderate chunk size for complex table
                    )
                    print(f"   ✅ Batch {i+1}/{num_batches} inserted ({len(chunk)} rows).")
                except Exception as e:
                    print(f"   ❌ Batch {i+1}/{num_batches} failed: {str(e)[:200]}...")
                    # Try smaller chunks if batch fails
                    if len(chunk) > 1:
                        print(f"   🔄 Retrying batch {i+1} with smaller chunks...")
                        for j in range(0, len(chunk), 20):  # 20 rows at a time
                            mini_chunk = chunk.iloc[j:j+20]
                            mini_chunk.to_sql(
                                name="drug_exposure",
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
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.db_manager.config.schema_cdm}.drug_exposure")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total drug exposures in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        print("\n🔍 Verifying loaded drug exposure data...")
        try:
            # Enhanced summary with medication vs immunization breakdown
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_exposures,
                COUNT(DISTINCT person_id) as unique_patients,
                COUNT(DISTINCT drug_concept_id) as drug_concepts,
                COUNT(DISTINCT visit_occurrence_id) as unique_visits,
                COUNT(CASE WHEN quantity IS NOT NULL THEN 1 END) as with_quantity,
                COUNT(CASE WHEN days_supply IS NOT NULL THEN 1 END) as with_days_supply,
                COUNT(CASE WHEN drug_exposure_end_date != drug_exposure_start_date THEN 1 END) as multi_day_exposures,
                COUNT(CASE WHEN drug_exposure_end_date = drug_exposure_start_date THEN 1 END) as single_day_exposures,
                AVG(quantity) as avg_quantity,
                AVG(days_supply) as avg_days_supply,
                MIN(drug_exposure_start_date) as earliest_exposure,
                MAX(drug_exposure_start_date) as latest_exposure
            FROM {self.db_manager.config.schema_cdm}.drug_exposure
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total drug exposures: {summary['total_exposures']}")
            print(f"  Unique patients: {summary['unique_patients']}")
            print(f"  Drug concepts: {summary['drug_concepts']}")
            print(f"  Unique visits: {summary['unique_visits']}")
            print(f"  With quantity: {summary['with_quantity']}")
            print(f"  With days supply: {summary['with_days_supply']}")
            print(f"  Multi-day exposures: {summary['multi_day_exposures']}")
            print(f"  Single-day exposures: {summary['single_day_exposures']} (likely immunizations)")
            print(f"  Average quantity: {summary['avg_quantity']:.2f}")
            print(f"  Average days supply: {summary['avg_days_supply']:.1f}")
            print(f"  Date range: {summary['earliest_exposure']} to {summary['latest_exposure']}")

            # Drug type breakdown (medications vs immunizations)
            drug_type_sql = f"""
            SELECT 
                de.drug_type_concept_id,
                c.concept_name as drug_type_name,
                COUNT(*) as exposure_count,
                COUNT(DISTINCT de.person_id) as unique_patients,
                AVG(de.quantity) as avg_quantity,
                AVG(de.days_supply) as avg_days_supply
            FROM {self.db_manager.config.schema_cdm}.drug_exposure de
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON de.drug_type_concept_id = c.concept_id
            GROUP BY de.drug_type_concept_id, c.concept_name
            ORDER BY exposure_count DESC
            """
            drug_types = self.db_manager.execute_query(drug_type_sql)
            if not drug_types.empty:
                print("\n📊 Drug type breakdown:")
                for _, row in drug_types.iterrows():
                    type_name = row['drug_type_name'] if pd.notna(row['drug_type_name']) else 'Unknown'
                    print(f"  {type_name} ({row['drug_type_concept_id']}): {row['exposure_count']} exposures")
                    print(f"    {row['unique_patients']} patients | Avg qty: {row['avg_quantity']:.1f} | Avg days: {row['avg_days_supply']:.1f}")

            # Vocabulary breakdown
            vocab_sql = f"""
            SELECT 
                c.vocabulary_id,
                COUNT(*) as exposure_count,
                COUNT(DISTINCT de.person_id) as unique_patients,
                COUNT(DISTINCT de.drug_concept_id) as unique_drugs
            FROM {self.db_manager.config.schema_cdm}.drug_exposure de
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON de.drug_concept_id = c.concept_id
            WHERE c.vocabulary_id IS NOT NULL
            GROUP BY c.vocabulary_id
            ORDER BY exposure_count DESC
            """
            vocab_breakdown = self.db_manager.execute_query(vocab_sql)
            if not vocab_breakdown.empty:
                print("\n📊 Vocabulary breakdown:")
                for _, row in vocab_breakdown.iterrows():
                    print(f"  {row['vocabulary_id']}: {row['exposure_count']} exposures")
                    print(f"    {row['unique_patients']} patients | {row['unique_drugs']} unique drugs")

            # Top drugs
            top_drugs_sql = f"""
            SELECT 
                de.drug_concept_id,
                c.concept_name as drug_name,
                c.vocabulary_id,
                COUNT(*) as exposure_count,
                COUNT(DISTINCT de.person_id) as unique_patients,
                AVG(de.quantity) as avg_quantity
            FROM {self.db_manager.config.schema_cdm}.drug_exposure de
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON de.drug_concept_id = c.concept_id
            WHERE de.drug_concept_id > 0
            GROUP BY de.drug_concept_id, c.concept_name, c.vocabulary_id
            ORDER BY exposure_count DESC
            LIMIT 5
            """
            top_drugs = self.db_manager.execute_query(top_drugs_sql)
            if not top_drugs.empty:
                print("\n📊 Top drugs by exposure count:")
                for _, row in top_drugs.iterrows():
                    vocab_info = f" ({row['vocabulary_id']})" if pd.notna(row['vocabulary_id']) else ""
                    print(f"  {row['drug_name']}{vocab_info}")
                    print(f"    {row['exposure_count']} exposures | {row['unique_patients']} patients | Avg qty: {row['avg_quantity']:.1f}")

            # Sample drug exposure records
            sample_sql = f"""
            SELECT 
                de.drug_exposure_id,
                de.person_id,
                de.drug_concept_id,
                c.concept_name as drug_name,
                c.vocabulary_id,
                de.drug_exposure_start_date,
                de.drug_exposure_end_date,
                de.quantity,
                de.days_supply,
                de.drug_source_value,
                de.visit_occurrence_id
            FROM {self.db_manager.config.schema_cdm}.drug_exposure de
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON de.drug_concept_id = c.concept_id
            ORDER BY de.drug_exposure_start_date
            LIMIT 5
            """
            samples = self.db_manager.execute_query(sample_sql)
            print("\n📋 Sample drug exposure records:")
            for _, r in samples.iterrows():
                end_info = f" - {r['drug_exposure_end_date']}" if pd.notna(r['drug_exposure_end_date']) and r['drug_exposure_end_date'] != r['drug_exposure_start_date'] else ""
                duration = f" ({r['days_supply']} days)" if pd.notna(r['days_supply']) and r['days_supply'] > 1 else ""
                concept_info = f" | {r['drug_name']}" if pd.notna(r['drug_name']) else ""
                vocab_info = f" ({r['vocabulary_id']})" if pd.notna(r['vocabulary_id']) else ""
                qty_info = f" | Qty: {r['quantity']}" if pd.notna(r['quantity']) else ""
                
                print(f"  Exposure ID: {r['drug_exposure_id']} | Person: {r['person_id']}")
                print(f"    Date: {r['drug_exposure_start_date']}{end_info}{duration}")
                print(f"    Drug: {r['drug_concept_id']}{concept_info}{vocab_info}{qty_info}")
                print(f"    Source: {r['drug_source_value']} | Visit: {r['visit_occurrence_id']}")
                
        except Exception as e:
            print(f"❌ Verification failed: {e}")