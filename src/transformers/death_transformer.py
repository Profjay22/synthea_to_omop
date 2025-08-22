import pandas as pd
from datetime import datetime
from typing import Optional, Dict
from src.utils.uuid_converter import UUIDConverter

class DeathTransformer:
    """Transform death data from patient and observation sources to OMOP death format"""
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        self.death_certificate_code = "69453-9"  # Cause of Death [US Standard Certificate of Death]
        
        # Cache for concept lookups
        self._death_type_concept_cache = {}
        self._cause_concept_cache = {}
    
    def transform(self, patients_df: pd.DataFrame, observations_df: pd.DataFrame) -> pd.DataFrame:
        """Transform death data from patient and observation sources"""
        
        print("🔄 Transforming death data from patient and observation sources...")
        
        # Extract deaths from patient table
        deaths_df = self._extract_deaths_from_patients(patients_df)
        
        if deaths_df.empty:
            print("❌ No deaths found in patient data")
            return pd.DataFrame()
        
        print(f"✅ Found {len(deaths_df)} deaths in patient data")
        
        # Enrich with death certificate information from observations
        enriched_deaths = self._enrich_with_death_certificates(deaths_df, observations_df)
        
        # Filter to only include patients that exist in person table
        if self.db_manager:
            enriched_deaths = self._filter_existing_patients(enriched_deaths)
            print(f"✅ Filtered to {len(enriched_deaths)} deaths for existing patients")
        
        # Pre-load concept mappings
        if self.db_manager:
            self._preload_concept_mappings(enriched_deaths)
        
        death_records = []
        total_records = len(enriched_deaths)
        
        print(f"🔄 Processing {total_records} death records...")
        
        for idx, (row_idx, death) in enumerate(enriched_deaths.iterrows()):
            try:
                # Print progress every 50 records
                if idx % 50 == 0:
                    print(f"   Progress: {idx}/{total_records} ({idx/total_records*100:.1f}%)")
                
                death_record = self._transform_death_record(death)
                if death_record:
                    death_records.append(death_record)
            except Exception as e:
                print(f"⚠️ Error with death record {idx}: {e}")
                continue
        
        if not death_records:
            print("❌ No valid death records created")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(death_records)
        
        # Fix data types to ensure database compatibility
        result_df = self._fix_data_types(result_df)
        
        print(f"✅ Successfully transformed {len(result_df)} death records")
        return result_df
    
    def _extract_deaths_from_patients(self, patients_df: pd.DataFrame) -> pd.DataFrame:
        """Extract death information from patient data"""
        
        # Check if death date column exists
        death_col = None
        for col in ['DEATHDATE', 'DEATH_DATE', 'death_date']:
            if col in patients_df.columns:
                death_col = col
                break
        
        if not death_col:
            print("⚠️ No death date column found in patient data")
            return pd.DataFrame()
        
        # Filter to patients with death dates
        deaths_df = patients_df[patients_df[death_col].notna()].copy()
        
        if deaths_df.empty:
            return pd.DataFrame()
        
        # Standardize column names
        deaths_df = deaths_df.rename(columns={
            'Id': 'patient_id',
            death_col: 'death_date_raw'
        })
        
        return deaths_df[['patient_id', 'death_date_raw']]
    
    def _enrich_with_death_certificates(self, deaths_df: pd.DataFrame, observations_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich deaths with death certificate information from observations"""
        
        print("🔄 Enriching deaths with death certificate information...")
        
        # Filter observations to death certificates only
        death_certs = observations_df[
            observations_df['CODE'] == self.death_certificate_code
        ].copy()
        
        if death_certs.empty:
            print("⚠️ No death certificate observations found")
            # Return deaths without certificate info
            deaths_df['death_cert_value'] = None
            deaths_df['has_death_cert'] = False
            return deaths_df
        
        print(f"✅ Found {len(death_certs)} death certificate observations")
        
        # Merge deaths with death certificates on patient ID
        enriched = deaths_df.merge(
            death_certs[['PATIENT', 'VALUE']],
            left_on='patient_id',
            right_on='PATIENT',
            how='left'
        )
        
        # Clean up merge columns
        enriched = enriched.drop(columns=['PATIENT'], errors='ignore')
        enriched = enriched.rename(columns={'VALUE': 'death_cert_value'})
        
        # Mark which deaths have certificate information
        enriched['has_death_cert'] = enriched['death_cert_value'].notna()
        
        cert_count = enriched['has_death_cert'].sum()
        print(f"✅ Matched {cert_count}/{len(deaths_df)} deaths with death certificates")
        
        return enriched
    
    def _preload_concept_mappings(self, deaths_df: pd.DataFrame) -> None:
        """Pre-load death type concept mapping and cause concept lookups"""
        if not self.db_manager:
            return
        
        try:
            print("🔄 Pre-loading death type concept mapping...")
            
            # Map death certificate code to death_type_concept_id - search in Observation domain, not Type Concept
            death_type_query = f"""
            SELECT 
                c.concept_id,
                c.concept_name,
                c.vocabulary_id,
                c.standard_concept
            FROM {self.db_manager.config.schema_cdm}.concept c
            WHERE c.concept_code = %(code)s
              AND c.vocabulary_id = 'LOINC'
              AND c.domain_id = 'Observation'
              AND c.standard_concept = 'S'
              AND c.invalid_reason IS NULL
            LIMIT 1
            """
            
            result = self.db_manager.execute_query(death_type_query, {'code': self.death_certificate_code})
            
            if not result.empty:
                concept_id = int(result.iloc[0]['concept_id'])
                concept_name = result.iloc[0]['concept_name']
                self._death_type_concept_cache[self.death_certificate_code] = concept_id
                print(f"✅ Mapped death certificate code {self.death_certificate_code} to concept {concept_id} ({concept_name})")
            else:
                print(f"⚠️ No standard LOINC concept found for death certificate code {self.death_certificate_code}")
                self._death_type_concept_cache[self.death_certificate_code] = 0
            
            # Pre-load cause concept mappings for unique cause values
            unique_causes = deaths_df[deaths_df['has_death_cert']]['death_cert_value'].dropna().unique()
            
            if len(unique_causes) > 0:
                print(f"🔄 Pre-loading {len(unique_causes)} cause concept mappings...")
                
                for cause_value in unique_causes:
                    cause_concept_id = self._lookup_cause_concept_by_name(str(cause_value))
                    self._cause_concept_cache[str(cause_value)] = cause_concept_id
                
                print(f"✅ Pre-loaded cause concept mappings")
            
        except Exception as e:
            print(f"⚠️ Error pre-loading death concepts: {e}")
    
    def _lookup_cause_concept_by_name(self, cause_value: str) -> int:
        """Lookup cause concept by searching concept names with first 3 words matching"""
        try:
            # Extract first 3 words for better matching
            words = cause_value.split()
            if len(words) >= 3:
                first_three_words = ' '.join(words[:3])
                search_patterns = [
                    cause_value,  # Exact match first
                    first_three_words,  # First 3 words
                    words[0] if words else cause_value  # First word fallback
                ]
            else:
                search_patterns = [cause_value]
            
            for i, pattern in enumerate(search_patterns):
                print(f"   Searching for '{pattern}' (attempt {i+1})")
                
                # Search for concepts by name similarity
                search_query = f"""
                SELECT 
                    c.concept_id,
                    c.concept_name,
                    c.vocabulary_id,
                    c.standard_concept,
                    CASE 
                        WHEN LOWER(c.concept_name) = LOWER(%(exact_term)s) THEN 1
                        WHEN LOWER(c.concept_name) LIKE LOWER(%(starts_with)s) THEN 2
                        WHEN LOWER(c.concept_name) LIKE LOWER(%(contains)s) THEN 3
                        ELSE 4
                    END as match_priority
                FROM {self.db_manager.config.schema_cdm}.concept c
                WHERE (
                    LOWER(c.concept_name) ILIKE LOWER(%(search_term)s)
                    OR LOWER(c.concept_name) ILIKE LOWER(%(starts_with)s)
                )
                  AND c.domain_id = 'Condition'
                  AND c.standard_concept = 'S'
                  AND c.invalid_reason IS NULL
                ORDER BY 
                    match_priority,
                    LENGTH(c.concept_name)
                LIMIT 1
                """
                
                # Create search parameters
                search_term = f"%{pattern}%"
                starts_with = f"{pattern}%"
                contains = f"%{pattern}%"
                
                result = self.db_manager.execute_query(search_query, {
                    'search_term': search_term,
                    'exact_term': pattern,
                    'starts_with': starts_with,
                    'contains': contains
                })
                
                if not result.empty:
                    concept_id = int(result.iloc[0]['concept_id'])
                    concept_name = result.iloc[0]['concept_name']
                    match_priority = result.iloc[0]['match_priority']
                    match_type = ['exact', 'starts with', 'contains', 'fuzzy'][match_priority - 1]
                    print(f"   ✅ Cause '{cause_value}' → {concept_id} ({concept_name}) [{match_type} match]")
                    return concept_id
            
            print(f"   ⚠️ No concept found for cause: {cause_value}")
            return 0
                
        except Exception as e:
            print(f"⚠️ Error looking up cause concept for '{cause_value}': {e}")
            return 0
    
    def _transform_death_record(self, death: pd.Series) -> Optional[dict]:
        """Transform single death record to OMOP death format"""
        
        # Parse death date
        death_datetime = self._parse_datetime_patient_format(death['death_date_raw'])
        if not death_datetime:
            return None
        
        # Convert patient_id to OMOP person_id
        person_id = UUIDConverter.person_id(death['patient_id'])
        
        # Get death type concept ID (from death certificate code)
        death_type_concept_id = 0
        if death['has_death_cert']:
            death_type_concept_id = self._death_type_concept_cache.get(self.death_certificate_code, 0)
        
        # Get cause information
        cause_concept_id = 0
        cause_source_value = None
        
        if death['has_death_cert'] and pd.notna(death['death_cert_value']):
            cause_source_value = str(death['death_cert_value'])[:50]  # Truncate to fit varchar(50)
            cause_concept_id = self._cause_concept_cache.get(str(death['death_cert_value']), 0)
        
        return {
            'person_id': person_id,
            'death_date': death_datetime.date(),
            'death_datetime': death_datetime,
            'death_type_concept_id': death_type_concept_id,
            'cause_concept_id': cause_concept_id,
            'cause_source_value': cause_source_value,
            'cause_source_concept_id': None  # As specified - no code for cause
        }
    
    def _filter_existing_patients(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to only include patients that exist in person table"""
        try:
            query = f"SELECT DISTINCT person_source_value FROM {self.db_manager.config.schema_cdm}.person"
            existing_persons = self.db_manager.execute_query(query)
            
            if existing_persons.empty:
                print("⚠️ No persons found in database")
                return pd.DataFrame()
            
            existing_patient_uuids = set(existing_persons['person_source_value'].tolist())
            filtered_df = df[df['patient_id'].isin(existing_patient_uuids)]
            
            return filtered_df
            
        except Exception as e:
            print(f"⚠️ Error filtering patients: {e}")
            return df
    
    def _parse_datetime_patient_format(self, date_str: str) -> Optional[datetime]:
        """Parse death date from patient data (likely DD/MM/YYYY format)"""
        if pd.isna(date_str):
            return None
        
        try:
            # Try DD/MM/YYYY format first
            return pd.to_datetime(date_str, format='%d/%m/%Y').to_pydatetime()
        except:
            try:
                # Fallback to auto-parsing
                return pd.to_datetime(date_str).to_pydatetime()
            except:
                return None
    
    def _fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix data types for database compatibility"""
        
        # Convert integer columns
        int_columns = [
            'person_id', 'death_type_concept_id', 'cause_concept_id'
        ]
        for col in int_columns:
            if col in df.columns:
                df[col] = df[col].astype('int32')
        
        # Convert datetime columns
        df['death_datetime'] = pd.to_datetime(df['death_datetime']).dt.tz_localize(None)
        df['death_date'] = pd.to_datetime(df['death_date']).dt.date
        
        # Ensure string columns fit database constraints
        if 'cause_source_value' in df.columns:
            df['cause_source_value'] = df['cause_source_value'].astype('string').str[:50]
        
        # Handle nullable columns
        nullable_columns = ['cause_source_concept_id']
        for col in nullable_columns:
            if col in df.columns:
                df[col] = df[col].astype('Int32')
        
        return df