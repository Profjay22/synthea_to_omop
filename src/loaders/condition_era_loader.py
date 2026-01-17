# src/loaders/condition_era_loader.py
"""
Condition Era Loader

Loads condition_era records into the OMOP CDM database.
"""

import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager


OMOP_CONDITION_ERA_COLUMNS: List[str] = [
    "condition_era_id",
    "person_id",
    "condition_concept_id",
    "condition_era_start_date",
    "condition_era_end_date",
    "condition_occurrence_count"
]


class ConditionEraLoader:
    """Loader for OMOP CDM condition_era table."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.schema = db_manager.config.schema_cdm

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Align DataFrame columns to match OMOP schema."""
        cols = [c for c in OMOP_CONDITION_ERA_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_CONDITION_ERA_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_condition_eras(self, eras_df: pd.DataFrame, batch_size: int = 500) -> bool:
        """
        Load condition era records to database.

        Args:
            eras_df: DataFrame with condition era records
            batch_size: Number of records per batch

        Returns:
            True if successful, False otherwise
        """
        if eras_df is None or eras_df.empty:
            print("❌ No condition era data to load")
            return False

        try:
            df = self._align_columns(eras_df)

            total = len(df)
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = total
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} condition eras via to_sql "
                  f"(schema={self.schema}, table=condition_era, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                chunk.to_sql(
                    name="condition_era",
                    con=self.db_manager.engine,
                    schema=self.schema,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=len(chunk)
                )
                print(f"   ✅ Batch {i+1}/{num_batches} inserted ({len(chunk)} rows).")
                start = end

            print("✅ All condition era data loaded successfully!")

            # Post-load count
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.schema}.condition_era")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total condition eras in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        """Verify loaded condition era data."""
        print("\n🔍 Verifying loaded condition era data...")
        try:
            summary_sql = f"""
            SELECT
                COUNT(*) as total_eras,
                COUNT(DISTINCT person_id) as unique_persons,
                COUNT(DISTINCT condition_concept_id) as unique_conditions,
                SUM(condition_occurrence_count) as total_occurrences,
                AVG(condition_occurrence_count) as avg_occurrences_per_era,
                MIN(condition_era_start_date) as earliest_era,
                MAX(condition_era_end_date) as latest_era
            FROM {self.schema}.condition_era
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total eras: {summary['total_eras']}")
            print(f"  Unique persons: {summary['unique_persons']}")
            print(f"  Unique conditions: {summary['unique_conditions']}")
            print(f"  Total occurrences covered: {summary['total_occurrences']}")
            print(f"  Avg occurrences per era: {summary['avg_occurrences_per_era']:.2f}")
            print(f"  Date range: {summary['earliest_era']} to {summary['latest_era']}")

            # Show top conditions
            top_conditions_sql = f"""
            SELECT
                ce.condition_concept_id,
                c.concept_name,
                COUNT(*) as era_count,
                SUM(ce.condition_occurrence_count) as total_occurrences
            FROM {self.schema}.condition_era ce
            LEFT JOIN {self.schema}.concept c ON ce.condition_concept_id = c.concept_id
            GROUP BY ce.condition_concept_id, c.concept_name
            ORDER BY era_count DESC
            LIMIT 5
            """
            top = self.db_manager.execute_query(top_conditions_sql)
            print("\n📋 Top 5 conditions by era count:")
            for _, r in top.iterrows():
                name = r['concept_name'] or f"Concept {r['condition_concept_id']}"
                print(f"  {name[:50]}: {r['era_count']} eras, {r['total_occurrences']} occurrences")

        except Exception as e:
            print(f"❌ Verification failed: {e}")
