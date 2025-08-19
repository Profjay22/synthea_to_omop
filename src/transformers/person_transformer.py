import pandas as pd
import hashlib
from datetime import datetime
from typing import Optional

class PersonTransformer:
    """Simple Person transformer with hardcoded concept mappings"""
    
    def __init__(self):
        # Hardcoded OMOP concept mappings (these are standard and won't change)
        self.gender_concepts = {
            'M': 8507,      # MALE
            'F': 8532,      # FEMALE
        }
        
        self.race_concepts = {
            'white': 8527,                    # White
            'black': 8516,                    # Black or African American
            'asian': 8515,                    # Asian
            'native': 8657,                   # American Indian or Alaska Native
        }
        
        self.ethnicity_concepts = {
            'hispanic': 38003563,             # Hispanic or Latino
            'nonhispanic': 38003564,          # Not Hispanic or Latino
        }
    
    def transform(self, patients_df: pd.DataFrame) -> pd.DataFrame:
        """Transform patients to OMOP person format"""
        
        print(f"🔄 Transforming {len(patients_df)} patients to OMOP Person format...")
        
        omop_persons = []
        
        for idx, patient in patients_df.iterrows():
            try:
                person_record = self._transform_patient(patient)
                if person_record:
                    omop_persons.append(person_record)
            except Exception as e:
                print(f"⚠️ Error with patient {patient['Id']}: {e}")
                continue
        
        if not omop_persons:
            print("❌ No valid records created")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(omop_persons)
        print(f"✅ Successfully transformed {len(result_df)} persons")
        return result_df
    
    def _transform_patient(self, patient: pd.Series) -> Optional[dict]:
        """Transform single patient to OMOP person record"""
        
        # Parse birth date
        birth_date = self._parse_date(patient['BIRTHDATE'])
        if not birth_date:
            return None
        
        # Generate person_id
        person_id = self._uuid_to_int(patient['Id'])
        
        # Map concepts
        gender_concept_id = self._map_gender(patient['GENDER'])
        race_concept_id = self._map_race(patient.get('RACE', ''))
        ethnicity_concept_id = self._map_ethnicity(patient.get('ETHNICITY', ''))
        
        # Create OMOP person record
        return {
            'person_id': person_id,
            'gender_concept_id': gender_concept_id,
            'year_of_birth': birth_date.year,
            'month_of_birth': birth_date.month,
            'day_of_birth': birth_date.day,
            'birth_datetime': birth_date,
            'race_concept_id': race_concept_id,
            'ethnicity_concept_id': ethnicity_concept_id,
            'location_id': None,
            'provider_id': None,
            'care_site_id': None,
            'person_source_value': patient['Id'],
            'gender_source_value': str(patient['GENDER']),
            'gender_source_concept_id': 0,
            'race_source_value': str(patient.get('RACE', '')),
            'race_source_concept_id': 0,
            'ethnicity_source_value': str(patient.get('ETHNICITY', '')),
            'ethnicity_source_concept_id': 0,
            
        }
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse DD/MM/YYYY date format"""
        if pd.isna(date_str):
            return None
        
        try:
            return pd.to_datetime(date_str, format='%d/%m/%Y').to_pydatetime()
        except:
            try:
                return pd.to_datetime(date_str).to_pydatetime()
            except:
                return None
    
    def _uuid_to_int(self, uuid_str: str) -> int:
        """Convert UUID to integer safely within 32-bit signed integer range"""
        raw_hash = int(hashlib.md5(str(uuid_str).encode()).hexdigest()[:8], 16)
        return raw_hash % (2**31 - 1)
    
    def _map_gender(self, gender) -> int:
        """Map gender to concept_id"""
        if pd.isna(gender):
            return 8551  # Unknown
        return self.gender_concepts.get(str(gender).upper(), 8551)
    
    def _map_race(self, race) -> int:
        """Map race to concept_id"""
        if pd.isna(race) or race == '':
            return 8552  # Unknown
        return self.race_concepts.get(str(race).lower(), 8552)
    
    def _map_ethnicity(self, ethnicity) -> int:
        """Map ethnicity to concept_id"""
        if pd.isna(ethnicity) or ethnicity == '':
            return 0  # Unknown
        return self.ethnicity_concepts.get(str(ethnicity).lower(), 0)