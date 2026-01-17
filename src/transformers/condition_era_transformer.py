# src/transformers/condition_era_transformer.py
"""
Condition Era Transformer

Derives condition_era records from condition_occurrence data.
Groups consecutive conditions of the same type into eras using a persistence window.
"""

import pandas as pd
import hashlib
from datetime import timedelta
from src.database.connection import DatabaseManager


class ConditionEraTransformer:
    """Transform condition_occurrence data into condition_era records."""

    def __init__(self, db_manager: DatabaseManager, gap_days: int = 30):
        """
        Initialize transformer.

        Args:
            db_manager: Database connection manager
            gap_days: Maximum gap between conditions to be considered same era (default 30)
        """
        self.db_manager = db_manager
        self.gap_days = gap_days
        self.schema = db_manager.config.schema_cdm

    def transform(self) -> pd.DataFrame:
        """
        Transform condition_occurrence data into condition_era records.

        Returns:
            DataFrame with condition_era records
        """
        print(f"🔄 Building condition eras from condition_occurrence (gap={self.gap_days} days)...")

        # Get condition occurrence data
        conditions_df = self._get_condition_occurrences()

        if conditions_df.empty:
            print("❌ No condition occurrence data found")
            return pd.DataFrame()

        print(f"✅ Found {len(conditions_df)} condition occurrences for {conditions_df['person_id'].nunique()} persons")

        # Build eras
        eras = self._build_eras(conditions_df)

        if eras.empty:
            print("❌ No condition eras generated")
            return pd.DataFrame()

        print(f"✅ Generated {len(eras)} condition eras")

        return eras

    def _get_condition_occurrences(self) -> pd.DataFrame:
        """Get condition occurrence data from database."""
        query = f"""
        SELECT
            person_id,
            condition_concept_id,
            condition_start_date,
            condition_end_date
        FROM {self.schema}.condition_occurrence
        WHERE condition_concept_id != 0
        ORDER BY person_id, condition_concept_id, condition_start_date
        """
        return self.db_manager.execute_query(query)

    def _build_eras(self, conditions_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build condition eras from condition occurrences.

        Uses the standard OMOP era-building algorithm:
        1. Sort by person, concept, start_date
        2. If gap between end of one condition and start of next <= gap_days,
           they belong to the same era
        3. Calculate era start (min start_date), era end (max end_date),
           and occurrence count
        """
        eras = []

        # Group by person and condition concept
        grouped = conditions_df.groupby(['person_id', 'condition_concept_id'])

        for (person_id, concept_id), group in grouped:
            # Sort by start date
            group = group.sort_values('condition_start_date').reset_index(drop=True)

            # Handle null end dates - use start date as end date
            group['condition_end_date'] = group.apply(
                lambda row: row['condition_end_date'] if pd.notna(row['condition_end_date'])
                else row['condition_start_date'],
                axis=1
            )

            # Build eras for this person/concept combination
            era_start = group.iloc[0]['condition_start_date']
            era_end = group.iloc[0]['condition_end_date']
            occurrence_count = 1

            for i in range(1, len(group)):
                current_start = group.iloc[i]['condition_start_date']
                current_end = group.iloc[i]['condition_end_date']

                # Calculate gap between previous era end and current start
                gap = (current_start - era_end).days if pd.notna(era_end) and pd.notna(current_start) else 0

                if gap <= self.gap_days:
                    # Extend current era
                    era_end = max(era_end, current_end) if pd.notna(current_end) else era_end
                    occurrence_count += 1
                else:
                    # Save current era and start new one
                    eras.append({
                        'person_id': person_id,
                        'condition_concept_id': concept_id,
                        'condition_era_start_date': era_start,
                        'condition_era_end_date': era_end,
                        'condition_occurrence_count': occurrence_count
                    })

                    # Start new era
                    era_start = current_start
                    era_end = current_end
                    occurrence_count = 1

            # Don't forget the last era
            eras.append({
                'person_id': person_id,
                'condition_concept_id': concept_id,
                'condition_era_start_date': era_start,
                'condition_era_end_date': era_end,
                'condition_occurrence_count': occurrence_count
            })

        # Create DataFrame
        eras_df = pd.DataFrame(eras)

        if not eras_df.empty:
            # Generate unique era IDs
            eras_df['condition_era_id'] = eras_df.apply(
                lambda row: self._generate_era_id(
                    row['person_id'],
                    row['condition_concept_id'],
                    row['condition_era_start_date']
                ),
                axis=1
            )

            # Reorder columns
            eras_df = eras_df[[
                'condition_era_id',
                'person_id',
                'condition_concept_id',
                'condition_era_start_date',
                'condition_era_end_date',
                'condition_occurrence_count'
            ]]

        return eras_df

    def _generate_era_id(self, person_id: int, concept_id: int, start_date) -> int:
        """Generate unique era ID from components."""
        key = f"condition_era_{person_id}_{concept_id}_{start_date}"
        hash_bytes = hashlib.md5(key.encode()).digest()[:4]
        return int.from_bytes(hash_bytes, byteorder='big', signed=False) % 2147483647 + 1
