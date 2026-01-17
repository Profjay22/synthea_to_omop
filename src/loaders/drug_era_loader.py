# src/loaders/drug_era_loader.py
"""
Drug Era Loader

Loads drug_era records into the OMOP CDM database.
"""

import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager


OMOP_DRUG_ERA_COLUMNS: List[str] = [
    "drug_era_id",
    "person_id",
    "drug_concept_id",
    "drug_era_start_date",
    "drug_era_end_date",
    "drug_exposure_count",
    "gap_days"
]


class DrugEraLoader:
    """Loader for OMOP CDM drug_era table."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.schema = db_manager.config.schema_cdm

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Align DataFrame columns to match OMOP schema."""
        cols = [c for c in OMOP_DRUG_ERA_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_DRUG_ERA_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_drug_eras(self, eras_df: pd.DataFrame, batch_size: int = 500) -> bool:
        """
        Load drug era records to database.

        Args:
            eras_df: DataFrame with drug era records
            batch_size: Number of records per batch

        Returns:
            True if successful, False otherwise
        """
        if eras_df is None or eras_df.empty:
            print("❌ No drug era data to load")
            return False

        try:
            df = self._align_columns(eras_df)

            total = len(df)
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = total
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} drug eras via to_sql "
                  f"(schema={self.schema}, table=drug_era, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                chunk.to_sql(
                    name="drug_era",
                    con=self.db_manager.engine,
                    schema=self.schema,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=len(chunk)
                )
                print(f"   ✅ Batch {i+1}/{num_batches} inserted ({len(chunk)} rows).")
                start = end

            print("✅ All drug era data loaded successfully!")

            # Post-load count
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.schema}.drug_era")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total drug eras in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        """Verify loaded drug era data."""
        print("\n🔍 Verifying loaded drug era data...")
        try:
            summary_sql = f"""
            SELECT
                COUNT(*) as total_eras,
                COUNT(DISTINCT person_id) as unique_persons,
                COUNT(DISTINCT drug_concept_id) as unique_drugs,
                SUM(drug_exposure_count) as total_exposures,
                AVG(drug_exposure_count) as avg_exposures_per_era,
                SUM(gap_days) as total_gap_days,
                MIN(drug_era_start_date) as earliest_era,
                MAX(drug_era_end_date) as latest_era
            FROM {self.schema}.drug_era
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total eras: {summary['total_eras']}")
            print(f"  Unique persons: {summary['unique_persons']}")
            print(f"  Unique drugs: {summary['unique_drugs']}")
            print(f"  Total exposures covered: {summary['total_exposures']}")
            print(f"  Avg exposures per era: {summary['avg_exposures_per_era']:.2f}")
            print(f"  Total gap days: {summary['total_gap_days']}")
            print(f"  Date range: {summary['earliest_era']} to {summary['latest_era']}")

            # Show top drugs
            top_drugs_sql = f"""
            SELECT
                de.drug_concept_id,
                c.concept_name,
                COUNT(*) as era_count,
                SUM(de.drug_exposure_count) as total_exposures
            FROM {self.schema}.drug_era de
            LEFT JOIN {self.schema}.concept c ON de.drug_concept_id = c.concept_id
            GROUP BY de.drug_concept_id, c.concept_name
            ORDER BY era_count DESC
            LIMIT 5
            """
            top = self.db_manager.execute_query(top_drugs_sql)
            print("\n📋 Top 5 drugs by era count:")
            for _, r in top.iterrows():
                name = r['concept_name'] or f"Concept {r['drug_concept_id']}"
                print(f"  {name[:50]}: {r['era_count']} eras, {r['total_exposures']} exposures")

        except Exception as e:
            print(f"❌ Verification failed: {e}")
