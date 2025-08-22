import pandas as pd
from datetime import datetime
from typing import Optional
from src.utils.uuid_converter import UUIDConverter

class ObservationPeriodTransformer:
    """Transform source data to calculate observation periods for each person"""
    
    def __init__(self, extractor):
        self.extractor = extractor
        self.period_type_concept_id = 32817  # EHR
    
    def transform(self) -> pd.DataFrame:
        """Calculate observation periods from all source tables"""
        
        print("🔄 Calculating observation periods from source data...")
        
        # Collect all evidence dates from source tables
        all_evidence = self._collect_evidence_dates()
        
        if all_evidence.empty:
            print("❌ No evidence dates found")
            return pd.DataFrame()
        
        print(f"📊 Collected {len(all_evidence)} evidence dates from source tables")
        
        # Calculate observation periods per person
        observation_periods = self._calculate_periods(all_evidence)
        
        print(f"✅ Calculated observation periods for {len(observation_periods)} persons")
        return observation_periods
    
    def _collect_evidence_dates(self) -> pd.DataFrame:
        """Collect all evidence dates from trusted source tables"""
        
        all_evidence = []
        
        # 1. Encounters (visit evidence - start/end)
        print("📥 Collecting encounter evidence...")
        encounter_evidence = self._get_encounter_evidence()
        if not encounter_evidence.empty:
            all_evidence.append(encounter_evidence)
            print(f"  ✅ {len(encounter_evidence)} encounter evidence records")
        
        # 2. Conditions (start/end)
        print("📥 Collecting condition evidence...")
        condition_evidence = self._get_condition_evidence()
        if not condition_evidence.empty:
            all_evidence.append(condition_evidence)
            print(f"  ✅ {len(condition_evidence)} condition evidence records")
        
        # 3. Procedures (start/end)
        print("📥 Collecting procedure evidence...")
        procedure_evidence = self._get_procedure_evidence()
        if not procedure_evidence.empty:
            all_evidence.append(procedure_evidence)
            print(f"  ✅ {len(procedure_evidence)} procedure evidence records")
        
        # 4. Medications (start/end)
        print("📥 Collecting medication evidence...")
        medication_evidence = self._get_medication_evidence()
        if not medication_evidence.empty:
            all_evidence.append(medication_evidence)
            print(f"  ✅ {len(medication_evidence)} medication evidence records")
        
        # 5. Observations (date only - normalize to start=end)
        print("📥 Collecting observation evidence...")
        observation_evidence = self._get_observation_evidence()
        if not observation_evidence.empty:
            all_evidence.append(observation_evidence)
            print(f"  ✅ {len(observation_evidence)} observation evidence records")
        
        # Combine all evidence
        if all_evidence:
            combined = pd.concat(all_evidence, ignore_index=True)
            # Remove duplicates
            combined = combined.drop_duplicates(subset=['person_id', 'start_date', 'end_date'])
            return combined
        else:
            return pd.DataFrame()
    
    def _get_encounter_evidence(self) -> pd.DataFrame:
        """Extract evidence from encounters (visit_occurrence equivalent)"""
        try:
            encounters_df = self.extractor.get_encounters()
            if encounters_df.empty:
                return pd.DataFrame()
            
            # Filter required columns
            if not all(col in encounters_df.columns for col in ['PATIENT', 'START', 'STOP']):
                print("⚠️ Missing required columns in encounters")
                return pd.DataFrame()
            
            evidence = []
            for _, row in encounters_df.iterrows():
                if pd.notna(row['PATIENT']) and pd.notna(row['START']):
                    start_date = self._parse_datetime(row['START'])
                    end_date = self._parse_datetime(row['STOP']) if pd.notna(row['STOP']) else start_date
                    
                    if start_date:
                        evidence.append({
                            'person_id': str(row['PATIENT']),
                            'start_date': start_date.date(),
                            'end_date': end_date.date() if end_date else start_date.date(),
                            'source': 'encounter'
                        })
            
            return pd.DataFrame(evidence)
            
        except Exception as e:
            print(f"⚠️ Error collecting encounter evidence: {e}")
            return pd.DataFrame()
    
    def _get_condition_evidence(self) -> pd.DataFrame:
        """Extract evidence from conditions"""
        try:
            conditions_df = self.extractor.get_conditions()
            if conditions_df.empty:
                return pd.DataFrame()
            
            # Filter required columns
            if not all(col in conditions_df.columns for col in ['PATIENT', 'START']):
                print("⚠️ Missing required columns in conditions")
                return pd.DataFrame()
            
            evidence = []
            for _, row in conditions_df.iterrows():
                if pd.notna(row['PATIENT']) and pd.notna(row['START']):
                    start_date = self._parse_datetime_condition_format(row['START'])
                    end_date = self._parse_datetime_condition_format(row['STOP']) if pd.notna(row.get('STOP')) else start_date
                    
                    if start_date:
                        evidence.append({
                            'person_id': str(row['PATIENT']),
                            'start_date': start_date.date(),
                            'end_date': end_date.date() if end_date else start_date.date(),
                            'source': 'condition'
                        })
            
            return pd.DataFrame(evidence)
            
        except Exception as e:
            print(f"⚠️ Error collecting condition evidence: {e}")
            return pd.DataFrame()
    
    def _get_procedure_evidence(self) -> pd.DataFrame:
        """Extract evidence from procedures"""
        try:
            procedures_df = self.extractor.get_procedures()
            if procedures_df.empty:
                return pd.DataFrame()
            
            # Check if procedures data has required columns
            if not all(col in procedures_df.columns for col in ['PATIENT', 'START']):
                print("⚠️ Missing required columns in procedures")
                return pd.DataFrame()
            
            evidence = []
            for _, row in procedures_df.iterrows():
                if pd.notna(row['PATIENT']) and pd.notna(row['START']):
                    start_date = self._parse_datetime(row['START'])
                    end_date = self._parse_datetime(row['STOP']) if pd.notna(row.get('STOP')) else start_date
                    
                    if start_date:
                        evidence.append({
                            'person_id': str(row['PATIENT']),
                            'start_date': start_date.date(),
                            'end_date': end_date.date() if end_date else start_date.date(),
                            'source': 'procedure'
                        })
            
            return pd.DataFrame(evidence)
            
        except Exception as e:
            print(f"⚠️ Error collecting procedure evidence: {e}")
            return pd.DataFrame()
    
    def _get_medication_evidence(self) -> pd.DataFrame:
        """Extract evidence from medications"""
        try:
            medications_df = self.extractor.get_medications()
            if medications_df.empty:
                return pd.DataFrame()
            
            # Check if medications data has required columns
            if not all(col in medications_df.columns for col in ['PATIENT', 'START']):
                print("⚠️ Missing required columns in medications")
                return pd.DataFrame()
            
            evidence = []
            for _, row in medications_df.iterrows():
                if pd.notna(row['PATIENT']) and pd.notna(row['START']):
                    start_date = self._parse_datetime(row['START'])
                    end_date = self._parse_datetime(row['STOP']) if pd.notna(row.get('STOP')) else start_date
                    
                    if start_date:
                        evidence.append({
                            'person_id': str(row['PATIENT']),
                            'start_date': start_date.date(),
                            'end_date': end_date.date() if end_date else start_date.date(),
                            'source': 'medication'
                        })
            
            return pd.DataFrame(evidence)
            
        except Exception as e:
            print(f"⚠️ Error collecting medication evidence: {e}")
            return pd.DataFrame()
    
    def _get_observation_evidence(self) -> pd.DataFrame:
        """Extract evidence from observations (date only - normalize to start=end)"""
        try:
            observations_df = self.extractor.get_observations()
            if observations_df.empty:
                return pd.DataFrame()
            
            # Check if observations data has required columns
            if not all(col in observations_df.columns for col in ['PATIENT', 'DATE']):
                print("⚠️ Missing required columns in observations")
                return pd.DataFrame()
            
            evidence = []
            for _, row in observations_df.iterrows():
                if pd.notna(row['PATIENT']) and pd.notna(row['DATE']):
                    obs_date = self._parse_datetime(row['DATE'])
                    
                    if obs_date:
                        # For observations, use same date for both start and end
                        evidence.append({
                            'person_id': str(row['PATIENT']),
                            'start_date': obs_date.date(),
                            'end_date': obs_date.date(),  # Same date for both
                            'source': 'observation'
                        })
            
            return pd.DataFrame(evidence)
            
        except Exception as e:
            print(f"⚠️ Error collecting observation evidence: {e}")
            return pd.DataFrame()
    
    def _calculate_periods(self, evidence_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate observation periods per person with death date capping"""
        
        print("📊 Calculating observation periods per person...")
        
        # Group by person and calculate min/max dates
        person_periods = evidence_df.groupby('person_id').agg({
            'start_date': 'min',  # Earliest evidence date
            'end_date': 'max'     # Latest evidence date
        }).reset_index()
        
        person_periods.rename(columns={
            'start_date': 'observation_period_start_date',
            'end_date': 'observation_period_end_date'
        }, inplace=True)
        
        # Get death dates and apply capping
        death_dates = self._get_death_dates()
        if not death_dates.empty:
            print(f"📊 Found {len(death_dates)} death dates for capping")
            
            # Merge with death dates
            person_periods = person_periods.merge(
                death_dates, 
                left_on='person_id', 
                right_on='patient_id', 
                how='left'
            )
            
            # Apply death date capping
            def apply_death_cap(row):
                if pd.notna(row.get('death_date')):
                    return min(row['observation_period_end_date'], row['death_date'])
                return row['observation_period_end_date']
            
            person_periods['observation_period_end_date'] = person_periods.apply(apply_death_cap, axis=1)
            
            # Clean up merge columns
            person_periods = person_periods[['person_id', 'observation_period_start_date', 'observation_period_end_date']]
        
        # Convert person_id to OMOP person_id using UUID converter
        person_periods['omop_person_id'] = person_periods['person_id'].apply(UUIDConverter.person_id)
        
        # Generate deterministic observation_period_id based on person_id
        person_periods['observation_period_id'] = person_periods['person_id'].apply(
            lambda x: UUIDConverter.generic_id(f"obs_period_{x}")
        )
        
        # Add period type
        person_periods['period_type_concept_id'] = self.period_type_concept_id
        
        # Final column selection
        result_df = person_periods[[
            'observation_period_id',
            'omop_person_id',
            'observation_period_start_date', 
            'observation_period_end_date',
            'period_type_concept_id'
        ]].rename(columns={'omop_person_id': 'person_id'})
        
        print(f"📊 Generated deterministic observation_period_id for {len(result_df)} persons")
        return result_df
    
    def _get_death_dates(self) -> pd.DataFrame:
        """Extract death dates from patient source data"""
        try:
            patients_df = self.extractor.get_patients()
            if patients_df.empty:
                return pd.DataFrame()
            
            # Check if death date column exists
            death_col = None
            for col in ['DEATHDATE', 'DEATH_DATE', 'death_date']:
                if col in patients_df.columns:
                    death_col = col
                    break
            
            if not death_col:
                print("ℹ️ No death date column found in patient data")
                return pd.DataFrame()
            
            # Extract death dates
            death_data = patients_df[['Id', death_col]].dropna(subset=[death_col])
            
            if death_data.empty:
                return pd.DataFrame()
            
            death_dates = []
            for _, row in death_data.iterrows():
                death_date = self._parse_datetime_condition_format(row[death_col])
                if death_date:
                    death_dates.append({
                        'patient_id': str(row['Id']),
                        'death_date': death_date.date()
                    })
            
            return pd.DataFrame(death_dates)
            
        except Exception as e:
            print(f"⚠️ Error collecting death dates: {e}")
            return pd.DataFrame()
    
    def _parse_datetime(self, datetime_str: str) -> Optional[datetime]:
        """Parse ISO datetime format"""
        if pd.isna(datetime_str):
            return None
        
        try:
            # Handle ISO format: 2015-10-21T13:30:04Z
            return pd.to_datetime(datetime_str).to_pydatetime()
        except:
            return None
    
    def _parse_datetime_condition_format(self, date_str: str) -> Optional[datetime]:
        """Parse DD/MM/YYYY date format from condition/patient data"""
        if pd.isna(date_str):
            return None
        
        try:
            # Handle DD/MM/YYYY format: 21/10/2015
            return pd.to_datetime(date_str, format='%d/%m/%Y').to_pydatetime()
        except:
            try:
                # Fallback to auto-parsing
                return pd.to_datetime(date_str).to_pydatetime()
            except:
                return None