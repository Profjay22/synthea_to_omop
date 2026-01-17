# src/transformers/dose_era_transformer.py
"""
Dose Era Transformer

Derives dose_era records from drug_exposure data.
Groups consecutive drug exposures with the same drug and dose into eras.
"""

import pandas as pd
import hashlib
from datetime import timedelta
from src.database.connection import DatabaseManager


class DoseEraTransformer:
    """Transform drug_exposure data into dose_era records."""

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
        Transform drug_exposure data into dose_era records.

        Returns:
            DataFrame with dose_era records
        """
        print(f"🔄 Building dose eras from drug_exposure (gap={self.gap_days} days)...")

        # Get drug exposure data with dose information
        exposures_df = self._get_drug_exposures_with_dose()

        if exposures_df.empty:
            print("❌ No drug exposure data with dose information found")
            return pd.DataFrame()

        print(f"✅ Found {len(exposures_df)} drug exposures with dose info for {exposures_df['person_id'].nunique()} persons")

        # Build eras
        eras = self._build_eras(exposures_df)

        if eras.empty:
            print("❌ No dose eras generated")
            return pd.DataFrame()

        print(f"✅ Generated {len(eras)} dose eras")

        return eras

    def _get_drug_exposures_with_dose(self) -> pd.DataFrame:
        """Get drug exposure data with dose information from database."""
        # Get drug exposures that have dose information
        # Note: dose_unit_source_value is available but not dose_unit_concept_id
        # We'll set unit_concept_id to 0 (unknown) since mapping isn't available
        query = f"""
        SELECT
            de.person_id,
            de.drug_concept_id,
            de.drug_exposure_start_date,
            de.drug_exposure_end_date,
            de.quantity as dose_value,
            0 as unit_concept_id
        FROM {self.schema}.drug_exposure de
        WHERE de.drug_concept_id != 0
          AND de.quantity IS NOT NULL
          AND de.quantity > 0
        ORDER BY de.person_id, de.drug_concept_id, de.quantity, de.drug_exposure_start_date
        """
        return self.db_manager.execute_query(query)

    def _build_eras(self, exposures_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build dose eras from drug exposures.

        Groups consecutive exposures with the same drug AND dose into eras.
        """
        eras = []

        # Fill null unit_concept_id with 0
        exposures_df['unit_concept_id'] = exposures_df['unit_concept_id'].fillna(0).astype(int)

        # Group by person, drug concept, dose value, and unit
        grouped = exposures_df.groupby(['person_id', 'drug_concept_id', 'dose_value', 'unit_concept_id'])

        for (person_id, concept_id, dose_value, unit_concept_id), group in grouped:
            # Sort by start date
            group = group.sort_values('drug_exposure_start_date').reset_index(drop=True)

            # Handle null end dates - use start date + 1 day as end date
            group['drug_exposure_end_date'] = group.apply(
                lambda row: row['drug_exposure_end_date'] if pd.notna(row['drug_exposure_end_date'])
                else row['drug_exposure_start_date'] + timedelta(days=1),
                axis=1
            )

            # Build eras for this person/concept/dose combination
            era_start = group.iloc[0]['drug_exposure_start_date']
            era_end = group.iloc[0]['drug_exposure_end_date']

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
                    era_end = max(era_end, current_end) if pd.notna(current_end) else era_end
                else:
                    # Save current era and start new one
                    eras.append({
                        'person_id': person_id,
                        'drug_concept_id': concept_id,
                        'unit_concept_id': unit_concept_id,
                        'dose_value': dose_value,
                        'dose_era_start_date': era_start,
                        'dose_era_end_date': era_end
                    })

                    # Start new era
                    era_start = current_start
                    era_end = current_end

            # Don't forget the last era
            eras.append({
                'person_id': person_id,
                'drug_concept_id': concept_id,
                'unit_concept_id': unit_concept_id,
                'dose_value': dose_value,
                'dose_era_start_date': era_start,
                'dose_era_end_date': era_end
            })

        # Create DataFrame
        eras_df = pd.DataFrame(eras)

        if not eras_df.empty:
            # Generate unique era IDs using row index to guarantee uniqueness
            eras_df = eras_df.reset_index(drop=True)
            eras_df['dose_era_id'] = eras_df.apply(
                lambda row: self._generate_era_id(
                    row['person_id'],
                    row['drug_concept_id'],
                    row['dose_value'],
                    row['dose_era_start_date'],
                    row['unit_concept_id'],
                    row.name  # row index for uniqueness
                ),
                axis=1
            )

            # Verify uniqueness
            if eras_df['dose_era_id'].duplicated().any():
                # Fall back to simple sequential IDs if still have duplicates
                eras_df['dose_era_id'] = range(1, len(eras_df) + 1)

            # Reorder columns to match OMOP schema
            eras_df = eras_df[[
                'dose_era_id',
                'person_id',
                'drug_concept_id',
                'unit_concept_id',
                'dose_value',
                'dose_era_start_date',
                'dose_era_end_date'
            ]]

        return eras_df

    def _generate_era_id(self, person_id: int, concept_id: int, dose_value: float, start_date, unit_concept_id: int, row_idx: int) -> int:
        """Generate unique era ID from components."""
        # Include row index to guarantee uniqueness
        key = f"dose_era_{person_id}_{concept_id}_{dose_value}_{unit_concept_id}_{start_date}_{row_idx}"
        hash_bytes = hashlib.md5(key.encode()).digest()[:8]
        return int.from_bytes(hash_bytes, byteorder='big', signed=False) % 2147483647 + 1
