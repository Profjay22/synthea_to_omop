import pandas as pd
from datetime import datetime
from typing import Optional, Dict
from src.utils.uuid_converter import UUIDConverter

class ConditionOccurrenceTransformer:
    """Optimized transformer for condition data to OMOP condition_occurrence format"""
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        self.condition_type_concept_id = 32817  # EHR
        self.condition_status_concept_id = 32902  # Active
        
        # Cache for lookups to avoid repeated database calls
        self._concept_cache = {}
        self._visit_cache = {}
        self._provider_cache = {}  # New cache for provider lookups
    
    def transform(self, conditions_df: pd.DataFrame) -> pd.DataFrame:
        """Transform conditions to OMOP condition_occurrence format with optimizations"""
        
        print(f"🔄 Transforming {len(conditions_df)} conditions to OMOP condition_occurrence format...")
        
        # Pre-validate and clean data
        conditions_df = self._validate_and_clean_data(conditions_df)
        if conditions_df.empty:
            return pd.DataFrame()
        
        # Bulk lookups for performance
        concept_mappings = self._bulk_lookup_concepts(conditions_df['CODE'].unique())
        visit_mapping = self._bulk_lookup_visits(conditions_df['ENCOUNTER'].dropna().unique())
        provider_mapping = self._bulk_lookup_providers_from_visits(conditions_df['ENCOUNTER'].dropna().unique())
        existing_patients = self._get_existing_patients()
        
        # Critical validation - ensure we have patients to work with
        if not existing_patients:
            raise Exception("❌ No patients found in person table - cannot process conditions")
        
        # Filter to existing patients
        conditions_df = conditions_df[conditions_df['PATIENT'].isin(existing_patients)]
        print(f"✅ Filtered to {len(conditions_df)} conditions for existing patients")
        
        if conditions_df.empty:
            print("⚠️ No conditions remain after patient filtering")
            return pd.DataFrame()
        
        # Vectorized transformations
        result_df = self._vectorized_transform(conditions_df, concept_mappings, visit_mapping, provider_mapping)
        
        print(f"✅ Successfully transformed {len(result_df)} condition occurrences")
        return result_df
    
    def _bulk_lookup_providers_from_visits(self, encounter_uuids: pd.Series) -> Dict[str, int]:
        """Bulk lookup provider IDs from visit_occurrence table via encounter UUIDs"""
        if not self.db_manager or len(encounter_uuids) == 0:
            return {}
        
        try:
            # Convert UUIDs to visit_occurrence_ids using the same algorithm
            visit_ids = [UUIDConverter.visit_occurrence_id(uuid) for uuid in encounter_uuids]
            
            # Try tuple parameter format first
            try:
                placeholders = ','.join(['%s'] * len(visit_ids))
                query = f"""
                SELECT visit_occurrence_id, provider_id, visit_source_value
                FROM {self.db_manager.config.schema_cdm}.visit_occurrence
                WHERE visit_occurrence_id IN ({placeholders})
                  AND provider_id IS NOT NULL
                """
                
                results = self.db_manager.execute_query(query, tuple(visit_ids))
                
            except Exception as e:
                print(f"⚠️ Tuple parameters failed for provider lookup, trying dictionary approach: {e}")
                
                # Fallback to dictionary parameters
                param_dict = {f'visit_{i}': visit_id for i, visit_id in enumerate(visit_ids)}
                param_names = ', '.join([f'%({name})s' for name in param_dict.keys()])
                
                query = f"""
                SELECT visit_occurrence_id, provider_id, visit_source_value
                FROM {self.db_manager.config.schema_cdm}.visit_occurrence
                WHERE visit_occurrence_id IN ({param_names})
                  AND provider_id IS NOT NULL
                """
                
                results = self.db_manager.execute_query(query, param_dict)
            
            # Map back from visit_occurrence_id to encounter UUID -> provider_id
            provider_mapping = {}
            for i, uuid in enumerate(encounter_uuids):
                visit_id = visit_ids[i]
                matching_visit = results[results['visit_occurrence_id'] == visit_id]
                if not matching_visit.empty:
                    provider_id = matching_visit.iloc[0]['provider_id']
                    if pd.notna(provider_id):
                        provider_mapping[uuid] = int(provider_id)
            
            print(f"📊 Provider mapping: {len(provider_mapping)}/{len(encounter_uuids)} encounters linked to providers")
            return provider_mapping
            
        except Exception as e:
            print(f"⚠️ Error in bulk provider lookup from visits: {e}")
            # Non-critical - providers are optional, so return empty mapping
            return {}
    
    def _validate_and_clean_data(self, conditions_df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean input data"""
        required_cols = ['START', 'PATIENT', 'CODE', 'DESCRIPTION']
        conditions_df = conditions_df.dropna(subset=required_cols)
        
        if conditions_df.empty:
            print("❌ No valid conditions after filtering")
            return pd.DataFrame()
        
        # Filter to valid condition domain codes
        if self.db_manager:
            conditions_df = self._filter_condition_domain(conditions_df)
            print(f"✅ Filtered to {len(conditions_df)} records in Condition domain")
        
        return conditions_df
    
    def _bulk_lookup_concepts(self, codes: pd.Series) -> Dict[str, Dict[str, int]]:
        """Bulk lookup concept IDs for all codes - both condition and source concepts"""
        if not self.db_manager or len(codes) == 0:
            return {'condition': {}, 'source': {}}
        
        try:
            # Convert to list and ensure all are strings
            codes_list = [str(code) for code in codes.astype(str).tolist()]
            
            # Try tuple parameter format first
            try:
                placeholders = ','.join(['%s'] * len(codes_list))
                
                # Get condition domain concepts
                condition_query = f"""
                SELECT concept_code, concept_id 
                FROM {self.db_manager.config.schema_cdm}.concept
                WHERE concept_code IN ({placeholders})
                  AND vocabulary_id = 'SNOMED'
                  AND domain_id = 'Condition'
                  AND invalid_reason IS NULL
                """
                
                # Get source concepts (any SNOMED code, regardless of domain)
                source_query = f"""
                SELECT concept_code, concept_id 
                FROM {self.db_manager.config.schema_cdm}.concept
                WHERE concept_code IN ({placeholders})
                  AND vocabulary_id = 'SNOMED'
                  AND invalid_reason IS NULL
                """
                
                condition_results = self.db_manager.execute_query(condition_query, tuple(codes_list))
                source_results = self.db_manager.execute_query(source_query, tuple(codes_list))
                
            except Exception as e:
                print(f"⚠️ Tuple parameters failed, trying dictionary approach: {e}")
                
                # Fallback to dictionary parameters
                param_dict = {f'code_{i}': code for i, code in enumerate(codes_list)}
                param_names = ', '.join([f'%({name})s' for name in param_dict.keys()])
                
                condition_query = f"""
                SELECT concept_code, concept_id 
                FROM {self.db_manager.config.schema_cdm}.concept
                WHERE concept_code IN ({param_names})
                  AND vocabulary_id = 'SNOMED'
                  AND domain_id = 'Condition'
                  AND invalid_reason IS NULL
                """
                
                source_query = f"""
                SELECT concept_code, concept_id 
                FROM {self.db_manager.config.schema_cdm}.concept
                WHERE concept_code IN ({param_names})
                  AND vocabulary_id = 'SNOMED'
                  AND invalid_reason IS NULL
                """
                
                condition_results = self.db_manager.execute_query(condition_query, param_dict)
                source_results = self.db_manager.execute_query(source_query, param_dict)
            
            condition_mapping = dict(zip(condition_results['concept_code'].astype(str), condition_results['concept_id']))
            source_mapping = dict(zip(source_results['concept_code'].astype(str), source_results['concept_id']))
            
            print(f"📊 Concept mapping: {len(condition_mapping)} condition concepts, {len(source_mapping)} source concepts")
            
            return {'condition': condition_mapping, 'source': source_mapping}
            
        except Exception as e:
            print(f"❌ Critical error in bulk concept lookup: {e}")
            print("⚠️ Proceeding with empty mappings - all concept_ids will be 0")
            return {'condition': {}, 'source': {}}
    
    def _bulk_lookup_visits(self, encounter_uuids: pd.Series) -> Dict[str, int]:
        """Bulk lookup visit occurrence IDs"""
        if not self.db_manager or len(encounter_uuids) == 0:
            return {}
        
        try:
            # Convert UUIDs to visit_occurrence_ids using the same algorithm
            visit_ids = [UUIDConverter.visit_occurrence_id(uuid) for uuid in encounter_uuids]
            
            # Try tuple parameter format first
            try:
                placeholders = ','.join(['%s'] * len(visit_ids))
                query = f"""
                SELECT visit_occurrence_id, visit_source_value
                FROM {self.db_manager.config.schema_cdm}.visit_occurrence
                WHERE visit_occurrence_id IN ({placeholders})
                """
                
                results = self.db_manager.execute_query(query, tuple(visit_ids))
                
            except Exception as e:
                print(f"⚠️ Tuple parameters failed for visits, trying dictionary approach: {e}")
                
                # Fallback to dictionary parameters
                param_dict = {f'visit_{i}': visit_id for i, visit_id in enumerate(visit_ids)}
                param_names = ', '.join([f'%({name})s' for name in param_dict.keys()])
                
                query = f"""
                SELECT visit_occurrence_id, visit_source_value
                FROM {self.db_manager.config.schema_cdm}.visit_occurrence
                WHERE visit_occurrence_id IN ({param_names})
                """
                
                results = self.db_manager.execute_query(query, param_dict)
            
            # Map back from visit_occurrence_id to encounter UUID
            mapping = {}
            for i, uuid in enumerate(encounter_uuids):
                visit_id = visit_ids[i]
                if visit_id in results['visit_occurrence_id'].values:
                    mapping[uuid] = visit_id
            
            print(f"📊 Visit mapping: {len(mapping)}/{len(encounter_uuids)} encounters linked to visits")
            return mapping
            
        except Exception as e:
            print(f"⚠️ Error in bulk visit lookup: {e}")
            # Non-critical - visits are optional, so return empty mapping
            return {}
    
    def _get_existing_patients(self) -> set:
        """Get set of existing patient UUIDs"""
        if not self.db_manager:
            return set()
        
        try:
            query = f"SELECT DISTINCT person_source_value FROM {self.db_manager.config.schema_cdm}.person"
            result = self.db_manager.execute_query(query)
            return set(result['person_source_value'].tolist())
        except Exception as e:
            print(f"⚠️ Error getting existing patients: {e}")
            return set()
    
    def _vectorized_transform(self, conditions_df: pd.DataFrame, 
                            concept_mappings: Dict, visit_mapping: Dict, provider_mapping: Dict) -> pd.DataFrame:
        """Perform vectorized transformation instead of row-by-row processing"""
        
        # Parse dates vectorized
        conditions_df['start_datetime'] = pd.to_datetime(conditions_df['START'], errors='coerce')
        conditions_df['end_datetime'] = pd.to_datetime(conditions_df['STOP'], errors='coerce')
        
        # Generate IDs vectorized
        conditions_df['condition_occurrence_id'] = conditions_df.apply(
            lambda row: UUIDConverter.generic_id(f"{row['PATIENT']}_{row['START']}_{row['CODE']}"), 
            axis=1
        )
        conditions_df['person_id'] = conditions_df['PATIENT'].apply(UUIDConverter.person_id)
        
        # Map concepts vectorized - use condition domain concepts first, fallback to 0
        conditions_df['condition_concept_id'] = conditions_df['CODE'].astype(str).map(
            concept_mappings['condition']
        ).fillna(0).astype(int)
        
        # Map source concepts - use any SNOMED concept, fallback to condition_concept_id, then 0
        conditions_df['condition_source_concept_id'] = conditions_df['CODE'].astype(str).map(
            concept_mappings['source']
        ).fillna(conditions_df['condition_concept_id']).fillna(0).astype(int)
        
        # Map visits vectorized
        conditions_df['visit_occurrence_id'] = conditions_df['ENCOUNTER'].map(visit_mapping)
        
        # Map providers vectorized - NEW: get provider from visit_occurrence
        conditions_df['provider_id'] = conditions_df['ENCOUNTER'].map(provider_mapping)
        
        # Create the final DataFrame with proper OMOP structure
        result = pd.DataFrame({
            'condition_occurrence_id': conditions_df['condition_occurrence_id'],
            'person_id': conditions_df['person_id'],
            'condition_concept_id': conditions_df['condition_concept_id'],
            'condition_start_date': conditions_df['start_datetime'].dt.date,
            'condition_start_datetime': conditions_df['start_datetime'],
            'condition_end_date': conditions_df['end_datetime'].dt.date,
            'condition_end_datetime': conditions_df['end_datetime'],
            'condition_type_concept_id': self.condition_type_concept_id,
            'condition_status_concept_id': self.condition_status_concept_id,
            'stop_reason': None,
            'provider_id': conditions_df['provider_id'],  # Now populated from visits!
            'visit_occurrence_id': conditions_df['visit_occurrence_id'],
            'visit_detail_id': None,
            'condition_source_value': conditions_df['DESCRIPTION'].str[:50],
            'condition_source_concept_id': conditions_df['condition_source_concept_id'],
            'condition_status_source_value': None
        })
        
        # Apply data type fixes
        return self._fix_data_types(result)
    
    def _filter_condition_domain(self, conditions_df: pd.DataFrame) -> pd.DataFrame:
        """Filter conditions to only include codes in Condition domain"""
        try:
            print("🔍 Validating condition codes against OMOP vocabulary...")
            
            unique_codes = conditions_df['CODE'].unique()
            
            # Convert to list and ensure all are strings
            codes_list = [str(code) for code in unique_codes]
            
            # Use proper SQL parameterization based on your database adapter
            # Try using tuple parameter format which is more compatible
            placeholders = ','.join(['%s'] * len(codes_list))
            domain_query = f"""
            SELECT DISTINCT concept_code
            FROM {self.db_manager.config.schema_cdm}.concept c
            WHERE c.concept_code IN ({placeholders})
              AND c.vocabulary_id = 'SNOMED'
              AND c.domain_id = 'Condition'
              AND c.invalid_reason IS NULL
            """
            
            # Pass parameters as tuple instead of list
            valid_condition_codes = self.db_manager.execute_query(domain_query, tuple(codes_list))
            
            if valid_condition_codes.empty:
                print("⚠️ No valid condition codes found in OMOP vocabulary")
                print(f"📊 Checked {len(codes_list)} unique codes for Condition domain")
                
                # Show sample codes that were checked
                print("Sample codes checked:")
                for code in codes_list[:5]:
                    print(f"  {code}")
                
                # This might not be an error - could be that your data doesn't contain condition codes
                # Return empty DataFrame rather than raising exception
                return pd.DataFrame()
            
            valid_codes_set = set(valid_condition_codes['concept_code'].astype(str))
            filtered_conditions = conditions_df[
                conditions_df['CODE'].astype(str).isin(valid_codes_set)
            ]
            
            invalid_count = len(conditions_df) - len(filtered_conditions)
            if invalid_count > 0:
                print(f"⚠️ Excluded {invalid_count} records not in Condition domain")
                
                # Show examples of excluded codes for debugging
                excluded_df = conditions_df[~conditions_df['CODE'].astype(str).isin(valid_codes_set)]
                print("Examples of excluded codes (may belong in observation/procedure tables):")
                for _, row in excluded_df[['CODE', 'DESCRIPTION']].drop_duplicates().head(3).iterrows():
                    print(f"  {row['CODE']}: {row['DESCRIPTION']}")
            
            return filtered_conditions
            
        except Exception as e:
            print(f"❌ Critical error validating condition domains: {e}")
            print(f"📊 Attempted to validate {len(unique_codes)} unique codes")
            
            # Fallback: try a different parameter format
            try:
                print("🔄 Trying alternative parameter format...")
                
                # Alternative approach: use dictionary parameters
                codes_list = [str(code) for code in unique_codes]
                
                # Build IN clause manually with individual parameters
                param_dict = {f'code_{i}': code for i, code in enumerate(codes_list)}
                param_names = ', '.join([f'%({name})s' for name in param_dict.keys()])
                
                domain_query = f"""
                SELECT DISTINCT concept_code
                FROM {self.db_manager.config.schema_cdm}.concept c
                WHERE c.concept_code IN ({param_names})
                  AND c.vocabulary_id = 'SNOMED'
                  AND c.domain_id = 'Condition'
                  AND c.invalid_reason IS NULL
                """
                
                valid_condition_codes = self.db_manager.execute_query(domain_query, param_dict)
                
                if valid_condition_codes.empty:
                    print("⚠️ No condition codes found - returning empty DataFrame")
                    return pd.DataFrame()
                
                valid_codes_set = set(valid_condition_codes['concept_code'].astype(str))
                filtered_conditions = conditions_df[
                    conditions_df['CODE'].astype(str).isin(valid_codes_set)
                ]
                
                print(f"✅ Alternative approach succeeded - found {len(filtered_conditions)} condition records")
                return filtered_conditions
                
            except Exception as e2:
                print(f"❌ Alternative approach also failed: {e2}")
                
                # Final fallback: proceed without domain validation but warn user
                print("⚠️ PROCEEDING WITHOUT DOMAIN VALIDATION - All records will be treated as conditions")
                print("⚠️ This may result in non-condition data in the condition_occurrence table")
                return conditions_df
    
    def _fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix data types for database compatibility"""
        
        # Integer columns
        int_columns = [
            'condition_occurrence_id', 'person_id', 'condition_concept_id',
            'condition_type_concept_id', 'condition_status_concept_id',
            'condition_source_concept_id'
        ]
        for col in int_columns:
            if col in df.columns:
                df[col] = df[col].astype('int32')
        
        # Nullable integer columns (including provider_id which can now be populated)
        nullable_int_columns = ['provider_id', 'visit_occurrence_id', 'visit_detail_id']
        for col in nullable_int_columns:
            if col in df.columns:
                df[col] = df[col].astype('Int32')
        
        # Datetime columns - remove timezone info
        datetime_columns = ['condition_start_datetime', 'condition_end_datetime']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        
        # String columns with length constraints
        string_columns = {
            'condition_source_value': 50,
            'condition_status_source_value': 50,
            'stop_reason': 20
        }
        for col, max_length in string_columns.items():
            if col in df.columns and df[col].notna().any():
                df[col] = df[col].astype('string').str[:max_length]
        
        return df