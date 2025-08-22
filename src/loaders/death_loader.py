import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager

OMOP_DEATH_COLUMNS: List[str] = [
    "person_id",
    "death_date",
    "death_datetime",
    "death_type_concept_id",
    "cause_concept_id",
    "cause_source_value",
    "cause_source_concept_id"
]

class DeathLoader:
    """Loader for OMOP CDM death table using pandas.to_sql"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Keep only columns that exist in OMOP death table
        cols = [c for c in OMOP_DEATH_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_DEATH_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        # Convert NaT -> None so DB gets NULL
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_deaths(self, deaths_df: pd.DataFrame, batch_size: Optional[int] = None) -> bool:
        if deaths_df is None or deaths_df.empty:
            print("❌ No data to load")
            return False

        try:
            df = self._align_columns(deaths_df)

            total = len(df)
            # Use appropriate batch size for deaths (simple table)
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = min(500, total)  # Standard batch size
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} death records via to_sql "
                  f"(schema={self.db_manager.config.schema_cdm}, table=death, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                try:
                    # Use engine directly with pandas.to_sql
                    chunk.to_sql(
                        name="death",
                        con=self.db_manager.engine,
                        schema=self.db_manager.config.schema_cdm,
                        if_exists="append",
                        index=False,
                        method="multi",        # Build batched INSERTs
                        chunksize=100         # Good chunk size for death records
                    )
                    print(f"   ✅ Batch {i+1}/{num_batches} inserted ({len(chunk)} rows).")
                except Exception as e:
                    print(f"   ❌ Batch {i+1}/{num_batches} failed: {str(e)[:200]}...")
                    # Try smaller chunks if batch fails
                    if len(chunk) > 1:
                        print(f"   🔄 Retrying batch {i+1} with smaller chunks...")
                        for j in range(0, len(chunk), 50):  # 50 rows at a time
                            mini_chunk = chunk.iloc[j:j+50]
                            mini_chunk.to_sql(
                                name="death",
                                con=self.db_manager.engine,
                                schema=self.db_manager.config.schema_cdm,
                                if_exists="append",
                                index=False,
                                method="multi",
                                chunksize=25
                            )
                        print(f"   ✅ Batch {i+1} completed with smaller chunks")
                    else:
                        print(f"   ❌ Single row failed, skipping...")
                        
                start = end

            print("✅ All data loaded successfully!")

            # Post-load count
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.db_manager.config.schema_cdm}.death")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total death records in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        print("\n🔍 Verifying loaded death data...")
        try:
            # Enhanced summary with cause analysis
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_deaths,
                COUNT(DISTINCT person_id) as unique_persons,
                COUNT(CASE WHEN death_type_concept_id > 0 THEN 1 END) as with_death_type,
                COUNT(CASE WHEN cause_concept_id > 0 THEN 1 END) as with_cause_concept,
                COUNT(CASE WHEN cause_source_value IS NOT NULL THEN 1 END) as with_cause_value,
                MIN(death_date) as earliest_death,
                MAX(death_date) as latest_death,
                COUNT(DISTINCT death_type_concept_id) as unique_death_types,
                COUNT(DISTINCT cause_concept_id) as unique_cause_concepts
            FROM {self.db_manager.config.schema_cdm}.death
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total deaths: {summary['total_deaths']}")
            print(f"  Unique persons: {summary['unique_persons']}")
            print(f"  With death type: {summary['with_death_type']}")
            print(f"  With cause concept: {summary['with_cause_concept']}")
            print(f"  With cause description: {summary['with_cause_value']}")
            print(f"  Date range: {summary['earliest_death']} to {summary['latest_death']}")
            print(f"  Unique death types: {summary['unique_death_types']}")
            print(f"  Unique cause concepts: {summary['unique_cause_concepts']}")

            # Death type breakdown
            death_type_sql = f"""
            SELECT 
                dt.death_type_concept_id,
                c.concept_name as death_type_name,
                c.vocabulary_id,
                COUNT(*) as death_count
            FROM {self.db_manager.config.schema_cdm}.death dt
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON dt.death_type_concept_id = c.concept_id
            WHERE dt.death_type_concept_id > 0
            GROUP BY dt.death_type_concept_id, c.concept_name, c.vocabulary_id
            ORDER BY death_count DESC
            """
            death_types = self.db_manager.execute_query(death_type_sql)
            if not death_types.empty:
                print("\n📊 Death type breakdown:")
                for _, row in death_types.iterrows():
                    vocab_info = f" ({row['vocabulary_id']})" if pd.notna(row['vocabulary_id']) else ""
                    print(f"  {row['death_type_concept_id']}: {row['death_type_name']}{vocab_info} - {row['death_count']} deaths")

            # Top causes of death
            cause_sql = f"""
            SELECT 
                d.cause_concept_id,
                c.concept_name as cause_name,
                c.vocabulary_id,
                COUNT(*) as death_count,
                d.cause_source_value
            FROM {self.db_manager.config.schema_cdm}.death d
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON d.cause_concept_id = c.concept_id
            WHERE d.cause_concept_id > 0
            GROUP BY d.cause_concept_id, c.concept_name, c.vocabulary_id, d.cause_source_value
            ORDER BY death_count DESC
            LIMIT 5
            """
            top_causes = self.db_manager.execute_query(cause_sql)
            if not top_causes.empty:
                print("\n📊 Top causes of death:")
                for _, row in top_causes.iterrows():
                    vocab_info = f" ({row['vocabulary_id']})" if pd.notna(row['vocabulary_id']) else ""
                    source_info = f" | Source: {row['cause_source_value']}" if pd.notna(row['cause_source_value']) else ""
                    print(f"  {row['cause_name']}{vocab_info} - {row['death_count']} deaths{source_info}")

            # Sample death records
            sample_sql = f"""
            SELECT 
                d.person_id,
                d.death_date,
                d.death_type_concept_id,
                dt.concept_name as death_type_name,
                d.cause_concept_id,
                c.concept_name as cause_name,
                d.cause_source_value
            FROM {self.db_manager.config.schema_cdm}.death d
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept dt 
                ON d.death_type_concept_id = dt.concept_id
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON d.cause_concept_id = c.concept_id
            ORDER BY d.death_date
            LIMIT 5
            """
            samples = self.db_manager.execute_query(sample_sql)
            print("\n📋 Sample death records:")
            for _, r in samples.iterrows():
                death_type_info = f" | Type: {r['death_type_name']}" if pd.notna(r['death_type_name']) else ""
                cause_info = f" | Cause: {r['cause_name']}" if pd.notna(r['cause_name']) else ""
                source_info = f" | Source: {r['cause_source_value']}" if pd.notna(r['cause_source_value']) else ""
                
                print(f"  Person: {r['person_id']} | Date: {r['death_date']}{death_type_info}{cause_info}{source_info}")
                
        except Exception as e:
            print(f"❌ Verification failed: {e}")