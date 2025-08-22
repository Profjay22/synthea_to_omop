import math
import pandas as pd
from typing import Optional, List
from sqlalchemy import text
from src.database.connection import DatabaseManager

OMOP_PROCEDURE_OCCURRENCE_COLUMNS: List[str] = [
    "procedure_occurrence_id",
    "person_id",
    "procedure_concept_id",
    "procedure_date",
    "procedure_datetime",
    "procedure_end_date",
    "procedure_end_datetime",
    "procedure_type_concept_id",
    "modifier_concept_id",
    "quantity",
    "provider_id",
    "visit_occurrence_id",
    "visit_detail_id",
    "procedure_source_value",
    "procedure_source_concept_id",
    "modifier_source_value"
]

class ProcedureOccurrenceLoader:
    """Loader for OMOP CDM procedure_occurrence table using pandas.to_sql"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # keep only columns that exist in OMOP procedure_occurrence
        cols = [c for c in OMOP_PROCEDURE_OCCURRENCE_COLUMNS if c in df.columns]
        missing = [c for c in OMOP_PROCEDURE_OCCURRENCE_COLUMNS if c not in df.columns]
        if missing:
            print(f"ℹ️ Missing columns will be NULL in DB: {missing}")
        aligned = df[cols].copy()
        # Convert NaT -> None so DB gets NULL
        aligned = aligned.replace({pd.NaT: None})
        return aligned

    def load_procedure_occurrences(self, procedures_df: pd.DataFrame, batch_size: Optional[int] = None) -> bool:
        if procedures_df is None or procedures_df.empty:
            print("❌ No data to load")
            return False

        try:
            df = self._align_columns(procedures_df)

            total = len(df)
            # Use appropriate batch size for procedures (increased from 100)
            if not batch_size or batch_size <= 0 or batch_size > total:
                batch_size = min(200, total)  # Increased default batch size
            num_batches = math.ceil(total / batch_size)

            print(f"🚀 Loading {total} procedure occurrences via to_sql "
                  f"(schema={self.db_manager.config.schema_cdm}, table=procedure_occurrence, "
                  f"batches={num_batches}, batch_size={batch_size})...")

            start = 0
            for i in range(num_batches):
                end = min(start + batch_size, total)
                chunk = df.iloc[start:end]

                try:
                    # Use engine directly with pandas.to_sql
                    chunk.to_sql(
                        name="procedure_occurrence",
                        con=self.db_manager.engine,
                        schema=self.db_manager.config.schema_cdm,
                        if_exists="append",
                        index=False,
                        method="multi",        # build batched INSERTs
                        chunksize=75          # Increased chunk size for better performance
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
                                name="procedure_occurrence",
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
            count_sql = text(f"SELECT COUNT(*) AS count FROM {self.db_manager.config.schema_cdm}.procedure_occurrence")
            with self.db_manager.engine.connect() as conn:
                res = conn.execute(count_sql).mappings().first()
            print(f"📊 Total procedure occurrences in database: {int(res['count'])}")
            return True

        except Exception as e:
            print(f"❌ Loading failed: {e}")
            return False

    def verify_data(self) -> None:
        print("\n🔍 Verifying loaded procedure occurrence data...")
        try:
            # Enhanced summary with vocabulary breakdown
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_procedures,
                COUNT(DISTINCT person_id) as unique_patients,
                COUNT(DISTINCT procedure_concept_id) as procedure_concepts,
                COUNT(DISTINCT visit_occurrence_id) as unique_visits,
                COUNT(CASE WHEN procedure_end_date IS NOT NULL THEN 1 END) as with_end_date,
                COUNT(CASE WHEN procedure_end_date IS NULL THEN 1 END) as single_date,
                AVG(quantity) as avg_quantity,
                MIN(procedure_date) as earliest_procedure,
                MAX(procedure_date) as latest_procedure,
                AVG(CASE WHEN procedure_end_date IS NOT NULL 
                    THEN procedure_end_date - procedure_date 
                    ELSE 0 END) as avg_duration_days
            FROM {self.db_manager.config.schema_cdm}.procedure_occurrence
            """
            summary = self.db_manager.execute_query(summary_sql).iloc[0]
            print(f"  Total procedures: {summary['total_procedures']}")
            print(f"  Unique patients: {summary['unique_patients']}")
            print(f"  Procedure concepts: {summary['procedure_concepts']}")
            print(f"  Unique visits: {summary['unique_visits']}")
            print(f"  With end date: {summary['with_end_date']}")
            print(f"  Single date procedures: {summary['single_date']}")
            print(f"  Average quantity: {summary['avg_quantity']:.1f}")
            print(f"  Average duration: {summary['avg_duration_days']:.1f} days")
            print(f"  Date range: {summary['earliest_procedure']} to {summary['latest_procedure']}")

            # Vocabulary breakdown
            vocab_sql = f"""
            SELECT 
                c.vocabulary_id,
                COUNT(*) as procedure_count,
                COUNT(DISTINCT po.person_id) as unique_patients
            FROM {self.db_manager.config.schema_cdm}.procedure_occurrence po
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON po.procedure_concept_id = c.concept_id
            WHERE c.vocabulary_id IS NOT NULL
            GROUP BY c.vocabulary_id
            ORDER BY procedure_count DESC
            """
            vocab_breakdown = self.db_manager.execute_query(vocab_sql)
            if not vocab_breakdown.empty:
                print("\n📊 Vocabulary breakdown:")
                for _, row in vocab_breakdown.iterrows():
                    print(f"  {row['vocabulary_id']}: {row['procedure_count']} procedures ({row['unique_patients']} patients)")

            # Enhanced sample with duration calculation
            sample_sql = f"""
            SELECT 
                po.procedure_occurrence_id, 
                po.person_id, 
                po.procedure_concept_id,
                c.concept_name,
                c.vocabulary_id,
                po.procedure_date, 
                po.procedure_end_date, 
                CASE 
                    WHEN po.procedure_end_date IS NOT NULL 
                    THEN po.procedure_end_date - po.procedure_date 
                    ELSE 0 
                END as duration_days,
                po.quantity,
                po.procedure_source_value, 
                po.visit_occurrence_id
            FROM {self.db_manager.config.schema_cdm}.procedure_occurrence po
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept c 
                ON po.procedure_concept_id = c.concept_id
            ORDER BY po.procedure_date
            LIMIT 5
            """
            samples = self.db_manager.execute_query(sample_sql)
            print("\n📋 Sample procedure occurrence records:")
            for _, r in samples.iterrows():
                end_info = f" - {r['procedure_end_date']}" if pd.notna(r['procedure_end_date']) else ""
                duration_info = f" ({r['duration_days']} days)" if r['duration_days'] > 0 else ""
                concept_info = f" | {r['concept_name']}" if pd.notna(r['concept_name']) else ""
                vocab_info = f" ({r['vocabulary_id']})" if pd.notna(r['vocabulary_id']) else ""
                
                print(f"  Proc ID: {r['procedure_occurrence_id']} | Person: {r['person_id']}")
                print(f"    Date: {r['procedure_date']}{end_info}{duration_info}")
                print(f"    Concept: {r['procedure_concept_id']}{concept_info}{vocab_info}")
                print(f"    Source: {r['procedure_source_value']} | Qty: {r['quantity']} | Visit: {r['visit_occurrence_id']}")
                
        except Exception as e:
            print(f"❌ Verification failed: {e}")