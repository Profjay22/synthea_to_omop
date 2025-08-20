import pandas as pd
import hashlib
from datetime import datetime
from typing import Optional
from src.utils.uuid_converter import UUIDConverter

class VisitOccurrenceTransformer:
    """Transform encounter data to OMOP visit_occurrence format"""
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        
        # OMOP visit concept mappings for Synthea encounter classes
        self.visit_concepts = {
            'outpatient': 9202,        # Outpatient Visit
            'ambulatory': 9202,        # Outpatient Visit  
            'wellness': 9202,          # Outpatient Visit (wellness exams are outpatient)
            'checkup': 9202,           # Outpatient Visit
            'preventive': 9202,        # Outpatient Visit
            'inpatient': 9201,         # Inpatient Visit
            'acute': 9201,             # Inpatient Visit
            'hospitalization': 9201,   # Inpatient Visit
            'emergency': 9203,         # Emergency Room Visit
            'urgent': 262,             # Emergency Room and Inpatient Visit
            'emergent': 9203,          # Emergency Room Visit
            'home': 581476,            # Home Visit  
            'telehealth': 581476,      # Home Visit (closest match)
            'virtual': 581476,         # Home Visit (closest match)
        }
        
        # Standard visit_type_concept_id for Synthea (EHR data)
        self.visit_type_concept_id = 32817  # EHR
    
    def transform(self, encounters_df: pd.DataFrame) -> pd.DataFrame:
        """Transform encounters to OMOP visit_occurrence format"""
        
        print(f"🔄 Transforming {len(encounters_df)} encounters to OMOP visit_occurrence format...")
        
        # Drop encounters with missing required fields
        required_cols = ['Id', 'START', 'STOP', 'PATIENT', 'ENCOUNTERCLASS']
        encounters_df = encounters_df.dropna(subset=required_cols)
        
        if encounters_df.empty:
            print("❌ No valid encounters after filtering")
            return pd.DataFrame()
        
        # Filter encounters to only include patients that exist in person table
        if self.db_manager:
            encounters_df = self._filter_existing_patients(encounters_df)
            print(f"✅ Filtered to {len(encounters_df)} encounters for existing patients")
        
        visit_occurrences = []
        
        for idx, encounter in encounters_df.iterrows():
            try:
                visit_record = self._transform_encounter(encounter)
                if visit_record:
                    visit_occurrences.append(visit_record)
            except Exception as e:
                print(f"⚠️ Error with encounter {encounter['Id']}: {e}")
                continue
        
        if not visit_occurrences:
            print("❌ No valid visit occurrence records created")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(visit_occurrences)
        
        # Fix data types to ensure database compatibility
        result_df = self._fix_data_types(result_df)
        
        print(f"✅ Successfully transformed {len(result_df)} visit occurrences")
        return result_df
    
    def _filter_existing_patients(self, encounters_df: pd.DataFrame) -> pd.DataFrame:
        """Filter encounters to only include patients that exist in person table"""
        try:
            # Get list of existing person_ids from database
            query = f"SELECT DISTINCT person_source_value FROM {self.db_manager.config.schema_cdm}.person"
            existing_persons = self.db_manager.execute_query(query)
            
            if existing_persons.empty:
                print("⚠️ No persons found in database - no encounters can be processed")
                return pd.DataFrame()
            
            existing_patient_uuids = set(existing_persons['person_source_value'].tolist())
            print(f"📊 Found {len(existing_patient_uuids)} existing patients in person table")
            
            # Filter encounters to only those patients
            filtered_encounters = encounters_df[
                encounters_df['PATIENT'].isin(existing_patient_uuids)
            ]
            
            return filtered_encounters
            
        except Exception as e:
            print(f"⚠️ Error filtering patients: {e}")
            print("⚠️ Proceeding without patient filtering - may cause foreign key errors")
            return encounters_df
    
    def _fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix data types for database compatibility"""
        
        # Convert integer columns to proper int32/Int32 types
        int_columns = [
            'visit_occurrence_id', 'person_id', 'visit_concept_id', 
            'visit_type_concept_id', 'visit_source_concept_id'
        ]
        for col in int_columns:
            if col in df.columns:
                df[col] = df[col].astype('int32')
        
        # Convert nullable integer columns
        nullable_int_columns = [
            'provider_id', 'care_site_id', 'admitted_from_concept_id',
            'discharged_to_concept_id', 'preceding_visit_occurrence_id'
        ]
        for col in nullable_int_columns:
            if col in df.columns:
                df[col] = df[col].astype('Int32')  # Nullable integer
        
        # Convert datetime columns - remove timezone info for database
        datetime_columns = ['visit_start_datetime', 'visit_end_datetime']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        
        # Convert date columns
        date_columns = ['visit_start_date', 'visit_end_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date
        
        # Ensure string columns are proper strings
        string_columns = [
            'visit_source_value', 'admitted_from_source_value', 
            'discharged_to_source_value'
        ]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype('string')
        
        return df
    
    def _transform_encounter(self, encounter: pd.Series) -> Optional[dict]:
        """Transform single encounter to OMOP visit_occurrence record"""
        
        # Parse dates
        start_datetime = self._parse_datetime(encounter['START'])
        end_datetime = self._parse_datetime(encounter['STOP'])
        
        if not start_datetime or not end_datetime:
            return None
        
        # Generate IDs using centralized UUID converter
        visit_occurrence_id = UUIDConverter.visit_occurrence_id(encounter['Id'])
        person_id = UUIDConverter.person_id(encounter['PATIENT'])
        
        # Map visit concept from encounter class
        visit_concept_id = self._map_visit_concept(encounter['ENCOUNTERCLASS'])
        
        # Convert organization and provider UUIDs to IDs (if present)
        care_site_id = UUIDConverter.care_site_id(encounter['ORGANIZATION']) if pd.notna(encounter.get('ORGANIZATION')) else None
        provider_id = UUIDConverter.provider_id(encounter['PROVIDER']) if pd.notna(encounter.get('PROVIDER')) else None
        
        # Create OMOP visit_occurrence record
        return {
            'visit_occurrence_id': visit_occurrence_id,
            'person_id': person_id,
            'visit_concept_id': visit_concept_id,
            'visit_start_date': start_datetime.date(),
            'visit_start_datetime': start_datetime,
            'visit_end_date': end_datetime.date(),
            'visit_end_datetime': end_datetime,
            'visit_type_concept_id': self.visit_type_concept_id,  # 32817 (EHR)
            'provider_id': provider_id,
            'care_site_id': care_site_id,
            'visit_source_value': str(encounter['ENCOUNTERCLASS']),
            'visit_source_concept_id': 0,  # No source concept mapping
            'admitted_from_concept_id': None,  # Not available in Synthea
            'admitted_from_source_value': None,  # Not available in Synthea
            'discharged_to_concept_id': None,  # Not available in Synthea
            'discharged_to_source_value': None,  # Not available in Synthea
            'preceding_visit_occurrence_id': None  # Would need complex visit linking logic
        }
    
    def _parse_datetime(self, datetime_str: str) -> Optional[datetime]:
        """Parse ISO datetime format from Synthea"""
        if pd.isna(datetime_str):
            return None
        
        try:
            # Synthea uses ISO format: 2018-06-22T23:55:48Z
            return pd.to_datetime(datetime_str).to_pydatetime()
        except:
            return None
    
    def _map_visit_concept(self, encounter_class) -> int:
        """Map encounter class to OMOP visit concept_id"""
        if pd.isna(encounter_class):
            return 0  # Unknown
        
        encounter_class_lower = str(encounter_class).lower()
        return self.visit_concepts.get(encounter_class_lower, 0)  # Default to Unknown