# src/transformers/drug_era_transformer.py
"""
Drug Era Transformer

Derives drug_era records from drug_exposure data.
Groups consecutive drug exposures of the same drug (at ingredient level) into eras.
"""

import pandas as pd
import hashlib
from datetime import timedelta
from src.database.connection import DatabaseManager


class DrugEraTransformer:
    """Transform drug_exposure data into drug_era records."""

    def __init__(self, db_manager: DatabaseManager, gap_days: int = 30):
        """
        Initialize transformer.

        Args:
            db_manager: Database connection manager
            gap_days: Maximum gap between exposures to be considered same era (default 30)
        """
        self.db_manager = db_manager
        self.gap_days = gap_days
        self.schema = db_manager.config.schema_cdm

    def transform(self) -> pd.DataFrame:
        """
        Transform drug_exposure data into drug_era records.

        Returns:
            DataFrame with drug_era records
        """
        print(f"🔄 Building drug eras from drug_exposure (gap={self.gap_days} days)...")

        # Get drug exposure data
        exposures_df = self._get_drug_exposures()

        if exposures_df.empty:
            print("❌ No drug exposure data found")
            return pd.DataFrame()

        print(f"✅ Found {len(exposures_df)} drug exposures for {exposures_df['person_id'].nunique()} persons")

        # Build eras
        eras = self._build_eras(exposures_df)

        if eras.empty:
            print("❌ No drug eras generated")
            return pd.DataFrame()

        print(f"✅ Generated {len(eras)} drug eras")

        return eras

    def _get_drug_exposures(self) -> pd.DataFrame:
        """Get drug exposure data from database with ingredient-level concept mapping."""
        # Get drug exposures and map to ingredient level using concept_ancestor
        # This ensures we group by ingredient, not by specific drug product
        query = f"""
        SELECT
            de.person_id,
            COALESCE(ca.ancestor_concept_id, de.drug_concept_id) as drug_concept_id,
            de.drug_exposure_start_date,
            de.drug_exposure_end_date
        FROM {self.schema}.drug_exposure de
        LEFT JOIN {self.schema}.concept_ancestor ca
            ON de.drug_concept_id = ca.descendant_concept_id
            AND ca.ancestor_concept_id IN (
                SELECT concept_id
                FROM {self.schema}.concept
                WHERE concept_class_id = 'Ingredient'
                AND standard_concept = 'S'
            )
        WHERE de.drug_concept_id != 0
        ORDER BY de.person_id, drug_concept_id, de.drug_exposure_start_date
        """
        try:
            return self.db_manager.execute_query(query)
        except Exception as e:
            # If concept_ancestor doesn't exist or query fails, use drug_concept_id directly
            print(f"⚠️ Using drug_concept_id directly (ingredient mapping failed): {e}")
            fallback_query = f"""
            SELECT
                person_id,
                drug_concept_id,
                drug_exposure_start_date,
                drug_exposure_end_date
            FROM {self.schema}.drug_exposure
            WHERE drug_concept_id != 0
            ORDER BY person_id, drug_concept_id, drug_exposure_start_date
            """
            return self.db_manager.execute_query(fallback_query)

    def _build_eras(self, exposures_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build drug eras from drug exposures.

        Uses the standard OMOP era-building algorithm:
        1. Sort by person, concept, start_date
        2. If gap between end of one exposure and start of next <= gap_days,
           they belong to the same era
        3. Calculate era start (min start_date), era end (max end_date),
           exposure count, and gap days
        """
        eras = []

        # Group by person and drug concept
        grouped = exposures_df.groupby(['person_id', 'drug_concept_id'])

        for (person_id, concept_id), group in grouped:
            # Sort by start date
            group = group.sort_values('drug_exposure_start_date').reset_index(drop=True)

            # Handle null end dates - use start date + 1 day as end date
            group['drug_exposure_end_date'] = group.apply(
                lambda row: row['drug_exposure_end_date'] if pd.notna(row['drug_exposure_end_date'])
                else row['drug_exposure_start_date'] + timedelta(days=1),
                axis=1
            )

            # Build eras for this person/concept combination
            era_start = group.iloc[0]['drug_exposure_start_date']
            era_end = group.iloc[0]['drug_exposure_end_date']
            exposure_count = 1
            total_gap_days = 0

            for i in range(1, len(group)):
                current_start = group.iloc[i]['drug_exposure_start_date']
                current_end = group.iloc[i]['drug_exposure_end_date']

                # Calculate gap between previous era end and current start
                if pd.notna(era_end) and pd.notna(current_start):
                    gap = (current_start - era_end).days
                else:
                    gap = 0

                if gap <= self.gap_days:
                    # Extend current era
                    if gap > 0:
                        total_gap_days += gap
                    era_end = max(era_end, current_end) if pd.notna(current_end) else era_end
                    exposure_count += 1
                else:
                    # Save current era and start new one
                    eras.append({
                        'person_id': person_id,
                        'drug_concept_id': concept_id,
                        'drug_era_start_date': era_start,
                        'drug_era_end_date': era_end,
                        'drug_exposure_count': exposure_count,
                        'gap_days': total_gap_days
                    })

                    # Start new era
                    era_start = current_start
                    era_end = current_end
                    exposure_count = 1
                    total_gap_days = 0

            # Don't forget the last era
            eras.append({
                'person_id': person_id,
                'drug_concept_id': concept_id,
                'drug_era_start_date': era_start,
                'drug_era_end_date': era_end,
                'drug_exposure_count': exposure_count,
                'gap_days': total_gap_days
            })

        # Create DataFrame
        eras_df = pd.DataFrame(eras)

        if not eras_df.empty:
            # Generate unique era IDs
            eras_df['drug_era_id'] = eras_df.apply(
                lambda row: self._generate_era_id(
                    row['person_id'],
                    row['drug_concept_id'],
                    row['drug_era_start_date']
                ),
                axis=1
            )

            # Reorder columns to match OMOP schema
            eras_df = eras_df[[
                'drug_era_id',
                'person_id',
                'drug_concept_id',
                'drug_era_start_date',
                'drug_era_end_date',
                'drug_exposure_count',
                'gap_days'
            ]]

        return eras_df

    def _generate_era_id(self, person_id: int, concept_id: int, start_date) -> int:
        """Generate unique era ID from components."""
        key = f"drug_era_{person_id}_{concept_id}_{start_date}"
        hash_bytes = hashlib.md5(key.encode()).digest()[:4]
        return int.from_bytes(hash_bytes, byteorder='big', signed=False) % 2147483647 + 1
