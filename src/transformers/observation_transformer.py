import pandas as pd
from datetime import datetime
from typing import Optional, Union, Dict
from src.utils.uuid_converter import UUIDConverter

class ObservationTransformer:
    """Transform observation data and excluded condition data to OMOP observation format"""
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        
        # Standard observation_type_concept_id for EHR data
        self.observation_type_concept_id = 32817  # EHR
        
        # Special concept mappings
        self.special_mappings = {
            'QALY': 4158643,
            'DALY': 4129922, 
            'QOLS': 4129922
        }
        
        # Cache for concept lookups to avoid repeated queries
        self._concept_cache = {}
        self._source_concept_cache = {}
        self._unit_cache = {}
    
    def transform_observations(self, observations_df: pd.DataFrame) -> pd.DataFrame:
        """Transform observation source data to OMOP observation format"""
        
        print(f"🔄 Transforming {len(observations_df)} observations to OMOP observation format...")
        
        # Drop observations with missing required fields
        required_cols = ['DATE', 'PATIENT', 'CODE', 'DESCRIPTION']
        observations_df = observations_df.dropna(subset=required_cols)
        
        if observations_df.empty:
            print("❌ No valid observations after filtering")
            return pd.DataFrame()
        
        # Filter to only valid observation domain codes
        if self.db_manager:
            observations_df = self._filter_observation_domain(observations_df)
            print(f"✅ Filtered to {len(observations_df)} records in Observation domain")
        
        # Filter to only include patients that exist in person table
        if self.db_manager:
            observations_df = self._filter_existing_patients(observations_df)
            print(f"✅ Filtered to {len(observations_df)} observations for existing patients")
        
        # Pre-load concept mappings to avoid individual lookups
        if self.db_manager:
            self._preload_concept_mappings(observations_df)
        
        observation_records = []
        total_records = len(observations_df)
        
        print(f"🔄 Processing {total_records} observation records...")
        
        for idx, (row_idx, observation) in enumerate(observations_df.iterrows()):
            try:
                # Print progress every 100 records
                if idx % 100 == 0:
                    print(f"   Progress: {idx}/{total_records} ({idx/total_records*100:.1f}%)")
                
                obs_record = self._transform_observation_record(observation, row_index=idx)
                if obs_record:
                    observation_records.append(obs_record)
            except Exception as e:
                print(f"⚠️ Error with observation {idx}: {e}")
                continue
        
        if not observation_records:
            print("❌ No valid observation records created")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(observation_records)
        
        # Fix data types to ensure database compatibility
        result_df = self._fix_data_types(result_df)
        
        print(f"✅ Successfully transformed {len(result_df)} observations")
        return result_df
    
    def transform_excluded_conditions(self, excluded_conditions_df: pd.DataFrame) -> pd.DataFrame:
        """Transform condition records that were excluded from condition_occurrence to observation format"""
        
        print(f"🔄 Transforming {len(excluded_conditions_df)} excluded conditions to observations...")
        
        # Drop conditions with missing required fields
        required_cols = ['START', 'PATIENT', 'CODE', 'DESCRIPTION']
        excluded_conditions_df = excluded_conditions_df.dropna(subset=required_cols)
        
        if excluded_conditions_df.empty:
            print("❌ No valid excluded conditions after filtering")
            return pd.DataFrame()
        
        # Pre-load concept mappings
        if self.db_manager:
            self._preload_concept_mappings(excluded_conditions_df, code_column='CODE')
        
        observation_records = []
        total_records = len(excluded_conditions_df)
        
        print(f"🔄 Processing {total_records} excluded condition records...")
        
        for idx, (row_idx, condition) in enumerate(excluded_conditions_df.iterrows()):
            try:
                # Print progress every 100 records
                if idx % 100 == 0:
                    print(f"   Progress: {idx}/{total_records} ({idx/total_records*100:.1f}%)")
                
                obs_record = self._transform_condition_to_observation(condition)
                if obs_record:
                    observation_records.append(obs_record)
            except Exception as e:
                print(f"⚠️ Error with excluded condition {idx}: {e}")
                continue
        
        if not observation_records:
            print("❌ No valid observation records created from excluded conditions")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(observation_records)
        
        # Fix data types to ensure database compatibility
        result_df = self._fix_data_types(result_df)
        
        print(f"✅ Successfully transformed {len(result_df)} excluded conditions to observations")
        return result_df
    
    def _preload_concept_mappings(self, df: pd.DataFrame, code_column: str = 'CODE') -> None:
        """Pre-load all concept mappings to avoid individual lookups"""
        if not self.db_manager:
            return
            
        try:
            print("🔄 Pre-loading concept mappings...")
            
            # Get unique codes
            unique_codes = df[code_column].unique()
            
            if len(unique_codes) == 0:
                return
            
            # Build query for all codes at once
            code_list = "', '".join(str(code) for code in unique_codes)
            
            # Get all concept mappings in one query
            query = f"""
            SELECT 
                c.concept_code,
                c.concept_id as source_concept_id,
                c.concept_name,
                c.standard_concept,
                COALESCE(cr.concept_id_2, c.concept_id) as standard_concept_id
            FROM {self.db_manager.config.schema_cdm}.concept c
            LEFT JOIN {self.db_manager.config.schema_cdm}.concept_relationship cr 
                ON c.concept_id = cr.concept_id_1 
                AND cr.relationship_id = 'Maps to'
                AND cr.invalid_reason IS NULL
            WHERE c.concept_code IN ('{code_list}')
              AND c.domain_id = 'Observation'
              AND c.invalid_reason IS NULL
            """
            
            result = self.db_manager.execute_query(query)
            
            # Build caches
            for _, row in result.iterrows():
                code = str(row['concept_code'])
                # Store both standard and source concept IDs
                self._concept_cache[code] = int(row['standard_concept_id'])
                self._source_concept_cache[code] = int(row['source_concept_id'])
            
            print(f"✅ Pre-loaded {len(self._concept_cache)} concept mappings")
            
            # Also pre-load unit mappings if we have units
            if 'UNITS' in df.columns:
                unique_units = df['UNITS'].dropna().unique()
                if len(unique_units) > 0:
                    unit_list = "', '".join(str(unit) for unit in unique_units)
                    unit_query = f"""
                    SELECT concept_name, concept_id 
                    FROM {self.db_manager.config.schema_cdm}.concept
                    WHERE concept_name IN ('{unit_list}')
                      AND domain_id = 'Unit'
                      AND standard_concept = 'S'
                      AND invalid_reason IS NULL
                    """
                    
                    unit_result = self.db_manager.execute_query(unit_query)
                    for _, row in unit_result.iterrows():
                        self._unit_cache[str(row['concept_name'])] = int(row['concept_id'])
                    
                    print(f"✅ Pre-loaded {len(self._unit_cache)} unit mappings")
            
        except Exception as e:
            print(f"⚠️ Error pre-loading concepts: {e}")
            # Continue without cache
    
    def _filter_observation_domain(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to only include codes that belong to the Observation domain"""
        try:
            print("🔍 Validating codes against OMOP vocabulary for Observation domain...")
            
            # Get unique codes from the data
            unique_codes = df['CODE'].unique()
            print(f"📊 Checking {len(unique_codes)} unique codes...")
            
            # Check for special mappings first (bypass database lookup)
            special_codes = set()
            for code in unique_codes:
                if str(code).upper() in self.special_mappings:
                    special_codes.add(str(code))
                    print(f"   ✅ Special mapping: {code} -> {self.special_mappings[str(code).upper()]}")
            
            # Process remaining codes in smaller batches to avoid memory issues
            remaining_codes = [code for code in unique_codes if str(code) not in special_codes]
            batch_size = 100
            valid_codes_set = set(special_codes)  # Start with special codes
            
            for i in range(0, len(remaining_codes), batch_size):
                batch_codes = remaining_codes[i:i+batch_size]
                code_list = "', '".join(str(code) for code in batch_codes)
                
                domain_query = f"""
                SELECT DISTINCT 
                    c.concept_code,
                    c.concept_id,
                    c.concept_name,
                    c.domain_id,
                    c.vocabulary_id,
                    c.standard_concept
                FROM {self.db_manager.config.schema_cdm}.concept c
                WHERE c.concept_code IN ('{code_list}')
                  AND c.domain_id = 'Observation'
                  AND c.vocabulary_id IN ('SNOMED', 'ICD10CM', 'ICD9CM', 'ICD10', 'ICD9Proc', 'LOINC')
                  AND c.invalid_reason IS NULL
                """
                
                batch_valid_codes = self.db_manager.execute_query(domain_query)
                if not batch_valid_codes.empty:
                    valid_codes_set.update(batch_valid_codes['concept_code'].astype(str))
                    # Log found codes
                    for _, row in batch_valid_codes.iterrows():
                        print(f"   ✅ Found: {row['concept_code']} -> {row['concept_name']} ({row['vocabulary_id']})")
                
                print(f"   Processed batch {i//batch_size + 1}/{(len(remaining_codes)-1)//batch_size + 1}")
            
            if not valid_codes_set:
                print("⚠️ No valid observation codes found in OMOP vocabulary")
                return pd.DataFrame()
            
            print(f"📊 Found {len(valid_codes_set)} valid observation codes")
            
            # Filter to only valid observation codes
            filtered_df = df[df['CODE'].astype(str).isin(valid_codes_set)]
            
            invalid_count = len(df) - len(filtered_df)
            if invalid_count > 0:
                print(f"⚠️ Excluded {invalid_count} records not in Observation domain")
            
            return filtered_df
            
        except Exception as e:
            print(f"⚠️ Error validating observation domains: {e}")
            print("⚠️ Proceeding without domain validation")
            return df
    
    def _transform_observation_record(self, observation: pd.Series, row_index: int = 0) -> Optional[dict]:
        """Transform single observation record to OMOP observation format"""
        
        # Parse date
        obs_datetime = self._parse_datetime(observation['DATE'])
        if not obs_datetime:
            return None
        
        # Generate observation_id using patient, date, code, and value for uniqueness
        unique_string = f"{observation['PATIENT']}_{observation['DATE']}_{observation['CODE']}"
        
        # Add value to make it unique when same code appears multiple times
        if pd.notna(observation.get('VALUE')):
            unique_string += f"_{observation['VALUE']}"
        
        # Add encounter if available for additional uniqueness
        if pd.notna(observation.get('ENCOUNTER')):
            unique_string += f"_{observation['ENCOUNTER']}"
        
        # Add row index as final guarantee of uniqueness
        unique_string += f"_row_{row_index}"
        
        observation_id = UUIDConverter.generic_id(unique_string)
        person_id = UUIDConverter.person_id(observation['PATIENT'])
        
        # Look up visit_occurrence_id if encounter is provided
        visit_occurrence_id = None
        if pd.notna(observation.get('ENCOUNTER')) and self.db_manager:
            visit_occurrence_id = self._lookup_visit_occurrence_id(observation['ENCOUNTER'])
        
        # Map observation concept using cache
        observation_concept_id = self._get_cached_concept_id(observation['CODE'])
        observation_source_concept_id = self._get_cached_source_concept_id(observation['CODE'])
        
        # Handle value - determine if numeric or string
        value_as_number, value_as_string, value_as_concept_id = self._process_value(observation.get('VALUE'))
        
        # Map units to concept_id using cache
        unit_concept_id = self._get_cached_unit_id(observation.get('UNITS'))
        
        return {
            'observation_id': observation_id,
            'person_id': person_id,
            'observation_concept_id': observation_concept_id,
            'observation_date': obs_datetime.date(),
            'observation_datetime': obs_datetime,
            'observation_type_concept_id': self.observation_type_concept_id,
            'value_as_number': value_as_number,
            'value_as_string': value_as_string,
            'value_as_concept_id': value_as_concept_id,
            'qualifier_concept_id': None,
            'unit_concept_id': unit_concept_id,
            'provider_id': None,
            'visit_occurrence_id': visit_occurrence_id,
            'visit_detail_id': None,
            'observation_source_value': str(observation['DESCRIPTION'])[:50],
            'observation_source_concept_id': observation_source_concept_id,  # Fixed!
            'unit_source_value': str(observation.get('UNITS', ''))[:50] if pd.notna(observation.get('UNITS')) else None,
            'qualifier_source_value': None,
            'value_source_value': str(observation.get('VALUE', ''))[:50] if pd.notna(observation.get('VALUE')) else None,
            'observation_event_id': None,
            'obs_event_field_concept_id': None
        }
    
    def _transform_condition_to_observation(self, condition: pd.Series) -> Optional[dict]:
        """Transform excluded condition record to OMOP observation format"""
        
        # Parse date (different format from observations)
        obs_datetime = self._parse_datetime_condition_format(condition['START'])
        if not obs_datetime:
            return None
        
        # Generate observation_id using patient, date, code, and additional unique elements
        unique_string = f"{condition['PATIENT']}_{condition['START']}_{condition['CODE']}"
        
        # Add encounter if available for additional uniqueness
        if pd.notna(condition.get('ENCOUNTER')):
            unique_string += f"_{condition['ENCOUNTER']}"
        
        # Add stop date if available for additional uniqueness
        if pd.notna(condition.get('STOP')):
            unique_string += f"_{condition['STOP']}"
        
        observation_id = UUIDConverter.generic_id(unique_string)
        person_id = UUIDConverter.person_id(condition['PATIENT'])
        
        # Look up visit_occurrence_id if encounter is provided
        visit_occurrence_id = None
        if pd.notna(condition.get('ENCOUNTER')) and self.db_manager:
            visit_occurrence_id = self._lookup_visit_occurrence_id(condition['ENCOUNTER'])
        
        # Map observation concept using cache
        observation_concept_id = self._get_cached_concept_id(condition['CODE'])
        observation_source_concept_id = self._get_cached_source_concept_id(condition['CODE'])
        
        return {
            'observation_id': observation_id,
            'person_id': person_id,
            'observation_concept_id': observation_concept_id,
            'observation_date': obs_datetime.date(),
            'observation_datetime': obs_datetime,
            'observation_type_concept_id': self.observation_type_concept_id,
            'value_as_number': None,
            'value_as_string': None,
            'value_as_concept_id': None,
            'qualifier_concept_id': None,
            'unit_concept_id': None,
            'provider_id': None,
            'visit_occurrence_id': visit_occurrence_id,
            'visit_detail_id': None,
            'observation_source_value': str(condition['DESCRIPTION'])[:50],
            'observation_source_concept_id': observation_source_concept_id,  # Fixed!
            'unit_source_value': None,
            'qualifier_source_value': None,
            'value_source_value': None,
            'observation_event_id': None,
            'obs_event_field_concept_id': None
        }
    
    def _get_cached_concept_id(self, code: str) -> int:
        """Get standard concept ID from cache or return 0"""
        # Check special mappings first (bypass database entirely)
        if str(code).upper() in self.special_mappings:
            return self.special_mappings[str(code).upper()]
        
        return self._concept_cache.get(str(code), 0)
    
    def _get_cached_source_concept_id(self, code: str) -> int:
        """Get source concept ID from cache or return 0"""
        # For special mappings, use the same concept as standard
        if str(code).upper() in self.special_mappings:
            return self.special_mappings[str(code).upper()]
        
        return self._source_concept_cache.get(str(code), 0)
    
    def _get_cached_unit_id(self, unit: str) -> Optional[int]:
        """Get unit concept ID from cache"""
        if pd.isna(unit) or unit == '':
            return None
        return self._unit_cache.get(str(unit))
    
    def _process_value(self, value) -> tuple[Optional[float], Optional[str], Optional[int]]:
        """Process observation value and determine appropriate OMOP field"""
        if pd.isna(value) or value == '':
            return None, None, None
        
        value_str = str(value)
        
        # Check for special mappings first
        if value_str.upper() in self.special_mappings:
            return None, None, self.special_mappings[value_str.upper()]
        
        # Try to parse as number
        try:
            # Remove common text markers that indicate non-numeric
            if any(marker in value_str.lower() for marker in ['{nominal}', '{ordinal}', 'finding', 'normal', 'abnormal']):
                return None, value_str[:60], None
            
            # Try to extract numeric value
            numeric_value = float(value_str)
            return numeric_value, None, None
        except (ValueError, TypeError):
            # Not numeric, store as string
            return None, value_str[:60], None
    
    def _filter_existing_patients(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to only include patients that exist in person table"""
        try:
            query = f"SELECT DISTINCT person_source_value FROM {self.db_manager.config.schema_cdm}.person"
            existing_persons = self.db_manager.execute_query(query)
            
            if existing_persons.empty:
                print("⚠️ No persons found in database")
                return pd.DataFrame()
            
            existing_patient_uuids = set(existing_persons['person_source_value'].tolist())
            filtered_df = df[df['PATIENT'].isin(existing_patient_uuids)]
            
            return filtered_df
            
        except Exception as e:
            print(f"⚠️ Error filtering patients: {e}")
            return df
    
    def _lookup_visit_occurrence_id(self, encounter_uuid: str) -> Optional[int]:
        """Lookup visit_occurrence_id based on encounter UUID"""
        try:
            visit_occurrence_id = UUIDConverter.visit_occurrence_id(encounter_uuid)
            
            query = f"""
                SELECT visit_occurrence_id 
                FROM {self.db_manager.config.schema_cdm}.visit_occurrence
                WHERE visit_occurrence_id = %(visit_id)s
                LIMIT 1
            """
            result = self.db_manager.execute_query(query, {'visit_id': visit_occurrence_id})
            
            return visit_occurrence_id if not result.empty else None
                
        except Exception as e:
            return None
    
    def _parse_datetime(self, datetime_str: str) -> Optional[datetime]:
        """Parse ISO datetime format from observation data"""
        if pd.isna(datetime_str):
            return None
        
        try:
            return pd.to_datetime(datetime_str).to_pydatetime()
        except:
            return None
    
    def _parse_datetime_condition_format(self, date_str: str) -> Optional[datetime]:
        """Parse DD/MM/YYYY date format from condition data"""
        if pd.isna(date_str):
            return None
        
        try:
            return pd.to_datetime(date_str, format='%d/%m/%Y').to_pydatetime()
        except:
            try:
                return pd.to_datetime(date_str).to_pydatetime()
            except:
                return None
    
    def _fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix data types for database compatibility"""
        
        # Convert integer columns
        int_columns = [
            'observation_id', 'person_id', 'observation_concept_id',
            'observation_type_concept_id', 'observation_source_concept_id'
        ]
        for col in int_columns:
            if col in df.columns:
                df[col] = df[col].astype('int32')
        
        # Convert nullable integer columns
        nullable_int_columns = [
            'value_as_concept_id', 'qualifier_concept_id', 'unit_concept_id',
            'provider_id', 'visit_occurrence_id', 'visit_detail_id',
            'observation_event_id', 'obs_event_field_concept_id'
        ]
        for col in nullable_int_columns:
            if col in df.columns:
                df[col] = df[col].astype('Int32')
        
        # Convert datetime columns
        df['observation_datetime'] = pd.to_datetime(df['observation_datetime']).dt.tz_localize(None)
        df['observation_date'] = pd.to_datetime(df['observation_date']).dt.date
        
        # Convert numeric column
        df['value_as_number'] = df['value_as_number'].astype('float64')
        
        # Ensure string columns fit database constraints
        string_columns = {
            'value_as_string': 60,
            'observation_source_value': 50,
            'unit_source_value': 50,
            'qualifier_source_value': 50,
            'value_source_value': 50
        }
        for col, max_length in string_columns.items():
            if col in df.columns:
                df[col] = df[col].astype('string').str[:max_length]
        
        return df