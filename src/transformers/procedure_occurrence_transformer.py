import pandas as pd
from datetime import datetime
from typing import Optional, Dict
from src.utils.uuid_converter import UUIDConverter

class ProcedureOccurrenceTransformer:
    """Transform procedure data and procedure observations to OMOP procedure_occurrence format"""
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        
        # Standard procedure_type_concept_id for EHR data
        self.procedure_type_concept_id = 32817  # EHR
        
        # Cache for concept lookups to avoid repeated queries
        self._concept_cache = {}
        self._source_concept_cache = {}
    
    def transform_procedures(self, procedures_df: pd.DataFrame) -> pd.DataFrame:
        """Transform procedure source data to OMOP procedure_occurrence format"""
        
        print(f"🔄 Transforming {len(procedures_df)} procedures to OMOP procedure_occurrence format...")
        
        # Drop procedures with missing required fields
        required_cols = ['START', 'PATIENT', 'CODE', 'DESCRIPTION']
        procedures_df = procedures_df.dropna(subset=required_cols)
        
        if procedures_df.empty:
            print("❌ No valid procedures after filtering")
            return pd.DataFrame()
        
        # Filter to only valid procedure domain codes
        if self.db_manager:
            procedures_df = self._filter_procedure_domain(procedures_df)
            print(f"✅ Filtered to {len(procedures_df)} records in Procedure domain")
        
        # Filter to only include patients that exist in person table
        if self.db_manager:
            procedures_df = self._filter_existing_patients(procedures_df)
            print(f"✅ Filtered to {len(procedures_df)} procedures for existing patients")
        
        # Pre-load concept mappings to avoid individual lookups
        if self.db_manager:
            self._preload_concept_mappings(procedures_df, code_column='CODE')
        
        procedure_records = []
        total_records = len(procedures_df)
        
        print(f"🔄 Processing {total_records} procedure records...")
        
        for idx, (row_idx, procedure) in enumerate(procedures_df.iterrows()):
            try:
                # Print progress every 100 records
                if idx % 100 == 0:
                    print(f"   Progress: {idx}/{total_records} ({idx/total_records*100:.1f}%)")
                
                proc_record = self._transform_procedure_record(procedure, row_index=idx)
                if proc_record:
                    procedure_records.append(proc_record)
            except Exception as e:
                print(f"⚠️ Error with procedure {idx}: {e}")
                continue
        
        if not procedure_records:
            print("❌ No valid procedure occurrence records created")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(procedure_records)
        
        # Fix data types to ensure database compatibility
        result_df = self._fix_data_types(result_df)
        
        print(f"✅ Successfully transformed {len(result_df)} procedure occurrences")
        return result_df
    
    def transform_observation_procedures(self, observations_df: pd.DataFrame) -> pd.DataFrame:
        """Transform observation records with CATEGORY='procedure' to procedure_occurrence format"""
        
        print(f"🔄 Extracting procedure observations from {len(observations_df)} observation records...")
        
        # Filter to only procedure category observations
        procedure_obs = observations_df[observations_df['CATEGORY'] == 'procedure'].copy()
        
        if procedure_obs.empty:
            print("❌ No procedure observations found")
            return pd.DataFrame()
        
        print(f"✅ Found {len(procedure_obs)} procedure observations")
        
        # Drop observations with missing required fields
        required_cols = ['DATE', 'PATIENT', 'CODE', 'DESCRIPTION']
        procedure_obs = procedure_obs.dropna(subset=required_cols)
        
        if procedure_obs.empty:
            print("❌ No valid procedure observations after filtering")
            return pd.DataFrame()
        
        # Filter to only valid procedure domain codes
        if self.db_manager:
            procedure_obs = self._filter_procedure_domain(procedure_obs)
            print(f"✅ Filtered to {len(procedure_obs)} observation procedures in Procedure domain")
        
        # Filter to only include patients that exist in person table
        if self.db_manager:
            procedure_obs = self._filter_existing_patients(procedure_obs)
            print(f"✅ Filtered to {len(procedure_obs)} observation procedures for existing patients")
        
        # Pre-load concept mappings
        if self.db_manager:
            self._preload_concept_mappings(procedure_obs, code_column='CODE')
        
        procedure_records = []
        total_records = len(procedure_obs)
        
        print(f"🔄 Processing {total_records} observation procedure records...")
        
        for idx, (row_idx, obs_procedure) in enumerate(procedure_obs.iterrows()):
            try:
                # Print progress every 100 records
                if idx % 100 == 0:
                    print(f"   Progress: {idx}/{total_records} ({idx/total_records*100:.1f}%)")
                
                proc_record = self._transform_observation_procedure_record(obs_procedure, row_index=idx)
                if proc_record:
                    procedure_records.append(proc_record)
            except Exception as e:
                print(f"⚠️ Error with observation procedure {idx}: {e}")
                continue
        
        if not procedure_records:
            print("❌ No valid procedure occurrence records created from observations")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(procedure_records)
        
        # Fix data types to ensure database compatibility
        result_df = self._fix_data_types(result_df)
        
        print(f"✅ Successfully transformed {len(result_df)} observation procedures")
        return result_df
    
    def _preload_concept_mappings(self, df: pd.DataFrame, code_column: str = 'CODE') -> None:
        """Pre-load all concept mappings to avoid individual lookups"""
        if not self.db_manager:
            return
            
        try:
            print("🔄 Pre-loading concept mappings with vocabulary priority...")
            
            # Get unique codes
            unique_codes = df[code_column].unique()
            
            if len(unique_codes) == 0:
                return
            
            # Build query for all codes at once
            code_list = "', '".join(str(code) for code in unique_codes)
            
            # Query with vocabulary priority to handle code collisions
            query = f"""
            WITH ranked_concepts AS (
                SELECT 
                    c.concept_code,
                    c.concept_id as source_concept_id,
                    c.concept_name,
                    c.vocabulary_id,
                    c.standard_concept,
                    COALESCE(cr.concept_id_2, c.concept_id) as standard_concept_id,
                    -- Priority ranking: prefer standard vocabularies first, then source vocabularies
                    CASE 
                        WHEN c.vocabulary_id = 'SNOMED' AND c.standard_concept = 'S' THEN 1
                        WHEN c.vocabulary_id = 'LOINC' AND c.standard_concept = 'S' THEN 2
                        WHEN c.vocabulary_id = 'CPT4' AND c.standard_concept = 'S' THEN 3
                        WHEN c.vocabulary_id = 'HCPCS' AND c.standard_concept = 'S' THEN 4
                        WHEN c.vocabulary_id = 'SNOMED' THEN 5
                        WHEN c.vocabulary_id = 'LOINC' THEN 6
                        WHEN c.vocabulary_id = 'CPT4' THEN 7
                        WHEN c.vocabulary_id = 'HCPCS' THEN 8
                        WHEN c.vocabulary_id = 'ICD10PCS' THEN 9  -- Non-standard source
                        WHEN c.vocabulary_id = 'ICD9Proc' THEN 10  -- Non-standard source
                        ELSE 99
                    END as vocab_priority,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.concept_code 
                        ORDER BY 
                            CASE 
                                WHEN c.vocabulary_id = 'SNOMED' AND c.standard_concept = 'S' THEN 1
                                WHEN c.vocabulary_id = 'LOINC' AND c.standard_concept = 'S' THEN 2
                                WHEN c.vocabulary_id = 'CPT4' AND c.standard_concept = 'S' THEN 3
                                WHEN c.vocabulary_id = 'HCPCS' AND c.standard_concept = 'S' THEN 4
                                WHEN c.vocabulary_id = 'SNOMED' THEN 5
                                WHEN c.vocabulary_id = 'LOINC' THEN 6
                                WHEN c.vocabulary_id = 'CPT4' THEN 7
                                WHEN c.vocabulary_id = 'HCPCS' THEN 8
                                WHEN c.vocabulary_id = 'ICD10PCS' THEN 9
                                WHEN c.vocabulary_id = 'ICD9Proc' THEN 10
                                ELSE 99 
                            END,
                            c.concept_id
                    ) as rn
                FROM {self.db_manager.config.schema_cdm}.concept c
                LEFT JOIN {self.db_manager.config.schema_cdm}.concept_relationship cr 
                    ON c.concept_id = cr.concept_id_1 
                    AND cr.relationship_id = 'Maps to'
                    AND cr.invalid_reason IS NULL
                WHERE c.concept_code IN ('{code_list}')
                  AND c.domain_id = 'Procedure'
                  AND c.vocabulary_id IN ('SNOMED', 'LOINC', 'CPT4', 'HCPCS', 'ICD10PCS', 'ICD9Proc')
                  AND c.invalid_reason IS NULL
            )
            SELECT 
                concept_code,
                source_concept_id,
                concept_name,
                vocabulary_id,
                standard_concept_id
            FROM ranked_concepts 
            WHERE rn = 1
            """
            
            result = self.db_manager.execute_query(query)
            
            # Build caches
            for _, row in result.iterrows():
                code = str(row['concept_code'])
                # Store both standard and source concept IDs
                self._concept_cache[code] = int(row['standard_concept_id'])
                self._source_concept_cache[code] = int(row['source_concept_id'])
                
                # Log vocabulary used for debugging
                vocab = row['vocabulary_id']
                name = row['concept_name']
                print(f"   Code {code}: {name} (from {vocab})")
            
            print(f"✅ Pre-loaded {len(self._concept_cache)} concept mappings")
            
        except Exception as e:
            print(f"⚠️ Error pre-loading concepts: {e}")
            # Continue without cache
    
    def _filter_procedure_domain(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to only include codes that belong to the Procedure domain"""
        try:
            print("🔍 Validating codes against OMOP vocabulary for Procedure domain...")
            
            # Get unique codes from the data
            unique_codes = df['CODE'].unique()
            print(f"📊 Checking {len(unique_codes)} unique codes...")
            
            # Process codes in smaller batches to avoid memory issues
            batch_size = 100
            valid_codes_set = set()
            
            for i in range(0, len(unique_codes), batch_size):
                batch_codes = unique_codes[i:i+batch_size]
                code_list = "', '".join(batch_codes.astype(str))
                
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
                  AND c.domain_id = 'Procedure'
                  AND c.vocabulary_id IN ('SNOMED', 'LOINC', 'CPT4', 'HCPCS', 'ICD10PCS', 'ICD9Proc')
                  AND c.invalid_reason IS NULL
                """
                
                batch_valid_codes = self.db_manager.execute_query(domain_query)
                if not batch_valid_codes.empty:
                    valid_codes_set.update(batch_valid_codes['concept_code'].astype(str))
                    # Log found codes
                    for _, row in batch_valid_codes.iterrows():
                        print(f"   ✅ Found: {row['concept_code']} -> {row['concept_name']} ({row['vocabulary_id']})")
                
                print(f"   Processed batch {i//batch_size + 1}/{(len(unique_codes)-1)//batch_size + 1}")
            
            if not valid_codes_set:
                print("⚠️ No valid procedure codes found in OMOP vocabulary")
                return pd.DataFrame()
            
            print(f"📊 Found {len(valid_codes_set)} valid procedure codes")
            
            # Filter to only valid procedure codes
            filtered_df = df[df['CODE'].astype(str).isin(valid_codes_set)]
            
            invalid_count = len(df) - len(filtered_df)
            if invalid_count > 0:
                print(f"⚠️ Excluded {invalid_count} records not in Procedure domain")
            
            return filtered_df
            
        except Exception as e:
            print(f"⚠️ Error validating procedure domains: {e}")
            print("⚠️ Proceeding without domain validation")
            return df
    
    def _transform_procedure_record(self, procedure: pd.Series, row_index: int = 0) -> Optional[dict]:
        """Transform single procedure record to OMOP procedure_occurrence format"""
        
        # Parse dates - handle DD/MM/YYYY format for procedures
        start_datetime = self._parse_datetime_procedure_format(procedure['START'])
        end_datetime = self._parse_datetime_procedure_format(procedure.get('STOP')) if pd.notna(procedure.get('STOP')) else start_datetime
        
        if not start_datetime:
            return None
        
        # Generate unique procedure_occurrence_id
        unique_string = f"{procedure['PATIENT']}_{procedure['START']}_{procedure['CODE']}"
        
        # Add encounter if available for additional uniqueness
        if pd.notna(procedure.get('ENCOUNTER')):
            unique_string += f"_{procedure['ENCOUNTER']}"
        
        # Add row index as final guarantee of uniqueness
        unique_string += f"_row_{row_index}"
        
        procedure_occurrence_id = UUIDConverter.generic_id(unique_string)
        person_id = UUIDConverter.person_id(procedure['PATIENT'])
        
        # Look up visit_occurrence_id if encounter is provided
        visit_occurrence_id = None
        if pd.notna(procedure.get('ENCOUNTER')) and self.db_manager:
            visit_occurrence_id = self._lookup_visit_occurrence_id(procedure['ENCOUNTER'])
        
        # Map procedure concept using cache
        procedure_concept_id = self._get_cached_concept_id(procedure['CODE'])
        procedure_source_concept_id = self._get_cached_source_concept_id(procedure['CODE'])
        
        return {
            'procedure_occurrence_id': procedure_occurrence_id,
            'person_id': person_id,
            'procedure_concept_id': procedure_concept_id,
            'procedure_date': start_datetime.date(),
            'procedure_datetime': start_datetime,
            'procedure_end_date': end_datetime.date() if end_datetime else None,
            'procedure_end_datetime': end_datetime,
            'procedure_type_concept_id': self.procedure_type_concept_id,
            'modifier_concept_id': None,
            'quantity': 1,  # Default to 1 procedure
            'provider_id': None,
            'visit_occurrence_id': visit_occurrence_id,
            'visit_detail_id': None,
            'procedure_source_value': str(procedure['DESCRIPTION'])[:50],
            'procedure_source_concept_id': procedure_source_concept_id,
            'modifier_source_value': None
        }
    
    def _transform_observation_procedure_record(self, obs_procedure: pd.Series, row_index: int = 0) -> Optional[dict]:
        """Transform observation procedure record to OMOP procedure_occurrence format"""
        
        # Parse date (single DATE field, use as both start and end) - ISO format for observations
        proc_datetime = self._parse_datetime_iso_format(obs_procedure['DATE'])
        if not proc_datetime:
            return None
        
        # Generate unique procedure_occurrence_id
        unique_string = f"{obs_procedure['PATIENT']}_{obs_procedure['DATE']}_{obs_procedure['CODE']}"
        
        # Add value to make it unique when same code appears multiple times
        if pd.notna(obs_procedure.get('VALUE')):
            unique_string += f"_{obs_procedure['VALUE']}"
        
        # Add encounter if available for additional uniqueness
        if pd.notna(obs_procedure.get('ENCOUNTER')):
            unique_string += f"_{obs_procedure['ENCOUNTER']}"
        
        # Add row index as final guarantee of uniqueness
        unique_string += f"_obs_row_{row_index}"
        
        procedure_occurrence_id = UUIDConverter.generic_id(unique_string)
        person_id = UUIDConverter.person_id(obs_procedure['PATIENT'])
        
        # Look up visit_occurrence_id if encounter is provided
        visit_occurrence_id = None
        if pd.notna(obs_procedure.get('ENCOUNTER')) and self.db_manager:
            visit_occurrence_id = self._lookup_visit_occurrence_id(obs_procedure['ENCOUNTER'])
        
        # Map procedure concept using cache
        procedure_concept_id = self._get_cached_concept_id(obs_procedure['CODE'])
        procedure_source_concept_id = self._get_cached_source_concept_id(obs_procedure['CODE'])
        
        return {
            'procedure_occurrence_id': procedure_occurrence_id,
            'person_id': person_id,
            'procedure_concept_id': procedure_concept_id,
            'procedure_date': proc_datetime.date(),
            'procedure_datetime': proc_datetime,
            'procedure_end_date': proc_datetime.date(),  # Same as start for observation procedures
            'procedure_end_datetime': proc_datetime,     # Same as start for observation procedures
            'procedure_type_concept_id': self.procedure_type_concept_id,
            'modifier_concept_id': None,
            'quantity': 1,  # Default to 1 procedure
            'provider_id': None,
            'visit_occurrence_id': visit_occurrence_id,
            'visit_detail_id': None,
            'procedure_source_value': str(obs_procedure['DESCRIPTION'])[:50],
            'procedure_source_concept_id': procedure_source_concept_id,
            'modifier_source_value': None
        }
    
    def _get_cached_concept_id(self, code: str) -> int:
        """Get standard concept ID from cache or return 0"""
        return self._concept_cache.get(str(code), 0)
    
    def _get_cached_source_concept_id(self, code: str) -> int:
        """Get source concept ID from cache or return 0"""
        return self._source_concept_cache.get(str(code), 0)
    
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
    
    def _parse_datetime_procedure_format(self, date_str: str) -> Optional[datetime]:
        """Parse procedure date format (likely DD/MM/YYYY or similar)"""
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
    
    def _parse_datetime_iso_format(self, datetime_str: str) -> Optional[datetime]:
        """Parse ISO datetime format from observation data"""
        if pd.isna(datetime_str):
            return None
        
        try:
            # Handle ISO format: 2015-10-21T13:30:04Z
            return pd.to_datetime(datetime_str).to_pydatetime()
        except:
            return None
    
    def _fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix data types for database compatibility"""
        
        # Convert integer columns
        int_columns = [
            'procedure_occurrence_id', 'person_id', 'procedure_concept_id',
            'procedure_type_concept_id', 'procedure_source_concept_id', 'quantity'
        ]
        for col in int_columns:
            if col in df.columns:
                df[col] = df[col].astype('int32')
        
        # Convert nullable integer columns
        nullable_int_columns = [
            'modifier_concept_id', 'provider_id', 'visit_occurrence_id', 'visit_detail_id'
        ]
        for col in nullable_int_columns:
            if col in df.columns:
                df[col] = df[col].astype('Int32')
        
        # Convert datetime columns
        datetime_columns = ['procedure_datetime', 'procedure_end_datetime']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        
        # Convert date columns
        date_columns = ['procedure_date', 'procedure_end_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date
        
        # Ensure string columns fit database constraints
        string_columns = {
            'procedure_source_value': 50,
            'modifier_source_value': 50
        }
        for col, max_length in string_columns.items():
            if col in df.columns:
                df[col] = df[col].astype('string').str[:max_length]
        
        return df