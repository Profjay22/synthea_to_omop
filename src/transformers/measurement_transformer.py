import pandas as pd
from datetime import datetime
from typing import Optional, Dict
from src.utils.uuid_converter import UUIDConverter

class MeasurementTransformer:
    """Transform observation data to OMOP measurement format for lab tests and clinical measurements"""
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        
        # Standard measurement_type_concept_id for EHR data
        self.measurement_type_concept_id = 32817  # EHR
        
        # Cache for concept lookups
        self._concept_cache = {}
        self._source_concept_cache = {}
        self._unit_cache = {}
        self._value_concept_cache = {}
    
    def transform(self, observations_df: pd.DataFrame) -> pd.DataFrame:
        """Transform observation source data to OMOP measurement format"""
        
        print(f"🔄 Transforming {len(observations_df)} observations to OMOP measurement format...")
        
        # Drop observations with missing required fields
        required_cols = ['DATE', 'PATIENT', 'CODE', 'DESCRIPTION']
        observations_df = observations_df.dropna(subset=required_cols)
        
        if observations_df.empty:
            print("❌ No valid observations after filtering")
            return pd.DataFrame()
        
        # Filter to only valid measurement domain codes
        if self.db_manager:
            observations_df = self._filter_measurement_domain(observations_df)
            print(f"✅ Filtered to {len(observations_df)} records in Measurement domain")
        
        # Filter to only include patients that exist in person table
        if self.db_manager:
            observations_df = self._filter_existing_patients(observations_df)
            print(f"✅ Filtered to {len(observations_df)} measurements for existing patients")
        
        # Pre-load concept mappings to avoid individual lookups
        if self.db_manager:
            self._preload_concept_mappings(observations_df)
        
        # Process records
        measurement_records = []
        total_records = len(observations_df)
        
        print(f"🔄 Processing {total_records} measurement records using vectorized operations...")
        
        # Process in chunks to avoid memory issues
        chunk_size = 10000
        for chunk_start in range(0, total_records, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_records)
            chunk_df = observations_df.iloc[chunk_start:chunk_end].copy()
            
            print(f"   Processing chunk {chunk_start//chunk_size + 1}/{(total_records-1)//chunk_size + 1} ({len(chunk_df)} records)")
            
            # Vectorized operations
            chunk_records = self._transform_chunk_vectorized(chunk_df, chunk_start)
            measurement_records.extend(chunk_records)
        
        if not measurement_records:
            print("❌ No valid measurement records created")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(measurement_records)
        
        # Fix data types to ensure database compatibility
        result_df = self._fix_data_types(result_df)
        
        print(f"✅ Successfully transformed {len(result_df)} measurements")
        return result_df
    
    def _transform_chunk_vectorized(self, chunk_df: pd.DataFrame, chunk_offset: int) -> list:
        """Vectorized transformation of a chunk of measurement records"""
        records = []
        
        # Vectorized datetime parsing
        try:
            chunk_df['parsed_datetime'] = pd.to_datetime(chunk_df['DATE'])
        except Exception as e:
            print(f"⚠️ Error parsing datetime: {e}")
            # Try alternative parsing methods
            chunk_df['parsed_datetime'] = pd.to_datetime(chunk_df['DATE'], errors='coerce')
        
        # Remove rows where datetime parsing failed
        chunk_df = chunk_df.dropna(subset=['parsed_datetime'])
        
        if chunk_df.empty:
            print("⚠️ No valid datetimes in chunk")
            return records
        
        # Vectorized UUID generation
        chunk_df['row_index'] = range(chunk_offset, chunk_offset + len(chunk_df))
        chunk_df['unique_string'] = (
            chunk_df['PATIENT'].astype(str) + '_' +
            chunk_df['DATE'].astype(str) + '_' +
            chunk_df['CODE'].astype(str) + '_' +
            chunk_df.get('VALUE', '').fillna('').astype(str) + '_' +
            chunk_df.get('ENCOUNTER', '').fillna('').astype(str) + '_meas_row_' +
            chunk_df['row_index'].astype(str)
        )
        
        # Vectorized concept and person ID generation
        chunk_df['measurement_id'] = chunk_df['unique_string'].apply(UUIDConverter.generic_id)
        chunk_df['person_id'] = chunk_df['PATIENT'].apply(UUIDConverter.person_id)
        chunk_df['visit_occurrence_id'] = chunk_df['ENCOUNTER'].apply(
            lambda x: UUIDConverter.visit_occurrence_id(x) if pd.notna(x) else None
        )
        
        # Vectorized concept mapping using cached values
        chunk_df['measurement_concept_id'] = chunk_df['CODE'].apply(lambda x: self._concept_cache.get(str(x), 0))
        chunk_df['measurement_source_concept_id'] = chunk_df['CODE'].apply(lambda x: self._source_concept_cache.get(str(x), 0))
        
        # Vectorized unit mapping
        chunk_df['unit_concept_id'] = chunk_df.get('UNITS', pd.Series(dtype='object')).apply(
            lambda x: self._unit_cache.get(str(x)) if pd.notna(x) else None
        )
        
        # Process values in batch
        value_results = chunk_df.get('VALUE', pd.Series(dtype='object')).apply(self._process_measurement_value_fast)
        chunk_df['value_as_number'] = value_results.apply(lambda x: x[0])
        chunk_df['value_as_concept_id'] = value_results.apply(lambda x: x[1])
        chunk_df['operator_concept_id'] = value_results.apply(lambda x: x[2])
        
        # Convert to records
        for _, row in chunk_df.iterrows():
            record = {
                'measurement_id': row['measurement_id'],
                'person_id': row['person_id'],
                'measurement_concept_id': row['measurement_concept_id'],
                'measurement_date': row['parsed_datetime'].date(),
                'measurement_datetime': row['parsed_datetime'],
                'measurement_time': row['parsed_datetime'].strftime('%H:%M:%S'),
                'measurement_type_concept_id': self.measurement_type_concept_id,
                'operator_concept_id': row['operator_concept_id'],
                'value_as_number': row['value_as_number'],
                'value_as_concept_id': row['value_as_concept_id'],
                'unit_concept_id': row['unit_concept_id'],
                'range_low': None,
                'range_high': None,
                'provider_id': None,
                'visit_occurrence_id': row['visit_occurrence_id'],
                'visit_detail_id': None,
                'measurement_source_value': str(row['DESCRIPTION'])[:50],
                'measurement_source_concept_id': row['measurement_source_concept_id'],
                'unit_source_value': str(row.get('UNITS', ''))[:50] if pd.notna(row.get('UNITS')) else None,
                'unit_source_concept_id': None,
                'value_source_value': str(row.get('VALUE', ''))[:50] if pd.notna(row.get('VALUE')) else None,
                'measurement_event_id': None,
                'meas_event_field_concept_id': None
            }
            records.append(record)
        
        return records
    
    def _process_measurement_value_fast(self, value) -> tuple[Optional[float], Optional[int], Optional[int]]:
        """Faster version of value processing for vectorized operations"""
        if pd.isna(value) or value == '':
            return None, None, None
        
        value_str = str(value).strip()
        
        # Quick operator check without complex parsing
        operator_concept_id = None
        clean_value = value_str
        
        if value_str.startswith('>='):
            operator_concept_id = 4171754
            clean_value = value_str[2:].strip()
        elif value_str.startswith('<='):
            operator_concept_id = 4171756
            clean_value = value_str[2:].strip()
        elif value_str.startswith('>'):
            operator_concept_id = 4172703
            clean_value = value_str[1:].strip()
        elif value_str.startswith('<'):
            operator_concept_id = 4171755
            clean_value = value_str[1:].strip()
        
        # Quick numeric check
        try:
            numeric_value = float(clean_value)
            return numeric_value, None, operator_concept_id
        except (ValueError, TypeError):
            # Non-numeric - check value concept cache
            value_concept_id = self._value_concept_cache.get(value_str, 0)
            return None, value_concept_id if value_concept_id > 0 else None, operator_concept_id
    
    def _preload_concept_mappings(self, df: pd.DataFrame) -> None:
        """Pre-load all concept mappings to avoid individual lookups"""
        if not self.db_manager:
            return
            
        try:
            print("🔄 Pre-loading concept mappings with vocabulary priority...")
            
            # Get unique codes
            unique_codes = df['CODE'].unique()
            
            if len(unique_codes) == 0:
                return
            
            # Build query for all codes at once
            code_list = "', '".join(str(code) for code in unique_codes)
            
            # Query with vocabulary priority for measurements
            query = f"""
            WITH ranked_concepts AS (
                SELECT 
                    c.concept_code,
                    c.concept_id as source_concept_id,
                    c.concept_name,
                    c.vocabulary_id,
                    c.standard_concept,
                    COALESCE(cr.concept_id_2, c.concept_id) as standard_concept_id,
                    -- Priority ranking: prefer LOINC for measurements, then others
                    CASE 
                        WHEN c.vocabulary_id = 'LOINC' AND c.standard_concept = 'S' THEN 1
                        WHEN c.vocabulary_id = 'SNOMED' AND c.standard_concept = 'S' THEN 2
                        WHEN c.vocabulary_id = 'UCUM' AND c.standard_concept = 'S' THEN 3
                        WHEN c.vocabulary_id = 'LOINC' THEN 4
                        WHEN c.vocabulary_id = 'SNOMED' THEN 5
                        WHEN c.vocabulary_id = 'UCUM' THEN 6
                        ELSE 99
                    END as vocab_priority,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.concept_code 
                        ORDER BY 
                            CASE 
                                WHEN c.vocabulary_id = 'LOINC' AND c.standard_concept = 'S' THEN 1
                                WHEN c.vocabulary_id = 'SNOMED' AND c.standard_concept = 'S' THEN 2
                                WHEN c.vocabulary_id = 'UCUM' AND c.standard_concept = 'S' THEN 3
                                WHEN c.vocabulary_id = 'LOINC' THEN 4
                                WHEN c.vocabulary_id = 'SNOMED' THEN 5
                                WHEN c.vocabulary_id = 'UCUM' THEN 6
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
                  AND c.domain_id = 'Measurement'
                  AND c.vocabulary_id IN ('LOINC', 'SNOMED', 'UCUM')
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
            
            # Build measurement concept caches
            for _, row in result.iterrows():
                code = str(row['concept_code'])
                self._concept_cache[code] = int(row['standard_concept_id'])
                self._source_concept_cache[code] = int(row['source_concept_id'])
                
                vocab = row['vocabulary_id']
                name = row['concept_name']
                print(f"   Code {code}: {name} (from {vocab})")
            
            print(f"✅ Pre-loaded {len(self._concept_cache)} measurement concept mappings")
            
            # Pre-load unit mappings
            unique_units = df.get('UNITS', pd.Series(dtype='object')).dropna().unique()
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
            
            # Pre-load value concept mappings for categorical results
            unique_values = df.get('VALUE', pd.Series(dtype='object')).dropna().unique()
            categorical_values = [v for v in unique_values if not self._is_numeric(v)]
            
            if len(categorical_values) > 0:
                print(f"🔄 Pre-loading {len(categorical_values)} value concept mappings...")
                for value in categorical_values:
                    value_concept_id = self._lookup_value_concept(str(value))
                    self._value_concept_cache[str(value)] = value_concept_id
                
                print(f"✅ Pre-loaded value concept mappings")
            
        except Exception as e:
            print(f"⚠️ Error pre-loading concepts: {e}")
    
    def _filter_measurement_domain(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to only include codes that belong to the Measurement domain"""
        try:
            print("🔍 Validating codes against OMOP vocabulary for Measurement domain...")
            
            unique_codes = df['CODE'].unique()
            print(f"📊 Checking {len(unique_codes)} unique codes...")
            
            # Process codes in batches
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
                  AND c.domain_id = 'Measurement'
                  AND c.vocabulary_id IN ('LOINC', 'SNOMED', 'UCUM')
                  AND c.invalid_reason IS NULL
                """
                
                batch_valid_codes = self.db_manager.execute_query(domain_query)
                if not batch_valid_codes.empty:
                    valid_codes_set.update(batch_valid_codes['concept_code'].astype(str))
                    for _, row in batch_valid_codes.iterrows():
                        print(f"   ✅ Found: {row['concept_code']} -> {row['concept_name']} ({row['vocabulary_id']})")
                
                print(f"   Processed batch {i//batch_size + 1}/{(len(unique_codes)-1)//batch_size + 1}")
            
            if not valid_codes_set:
                print("⚠️ No valid measurement codes found in OMOP vocabulary")
                return pd.DataFrame()
            
            print(f"📊 Found {len(valid_codes_set)} valid measurement codes")
            
            # Filter to only valid measurement codes
            filtered_df = df[df['CODE'].astype(str).isin(valid_codes_set)]
            
            invalid_count = len(df) - len(filtered_df)
            if invalid_count > 0:
                print(f"⚠️ Excluded {invalid_count} records not in Measurement domain")
            
            return filtered_df
            
        except Exception as e:
            print(f"⚠️ Error validating measurement domains: {e}")
            print("⚠️ Proceeding without domain validation")
            return df
    
    def _is_numeric(self, value_str: str) -> bool:
        """Check if a string represents a numeric value"""
        try:
            float(str(value_str).strip())
            return True
        except (ValueError, TypeError):
            return False
    
    def _lookup_value_concept(self, value_str: str) -> int:
        """Lookup concept ID for categorical measurement values"""
        if not self.db_manager:
            return 0
        
        try:
            # Search for matching concepts in Meas Value domain
            search_query = f"""
            SELECT 
                c.concept_id,
                c.concept_name,
                c.vocabulary_id
            FROM {self.db_manager.config.schema_cdm}.concept c
            WHERE LOWER(c.concept_name) ILIKE %(search_term)s
              AND c.domain_id = 'Meas Value'
              AND c.standard_concept = 'S'
              AND c.invalid_reason IS NULL
            ORDER BY 
                CASE WHEN LOWER(c.concept_name) = LOWER(%(exact_term)s) THEN 1 ELSE 2 END,
                LENGTH(c.concept_name)
            LIMIT 1
            """
            
            search_term = f"%{value_str.lower()}%"
            
            result = self.db_manager.execute_query(search_query, {
                'search_term': search_term,
                'exact_term': value_str
            })
            
            if not result.empty:
                return int(result.iloc[0]['concept_id'])
            else:
                return 0
                
        except Exception as e:
            return 0
    
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
    
    def _fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix data types for database compatibility"""
        
        # Convert integer columns
        int_columns = [
            'measurement_id', 'person_id', 'measurement_concept_id',
            'measurement_type_concept_id', 'measurement_source_concept_id'
        ]
        for col in int_columns:
            if col in df.columns:
                df[col] = df[col].astype('int32')
        
        # Convert nullable integer columns
        nullable_int_columns = [
            'operator_concept_id', 'value_as_concept_id', 'unit_concept_id',
            'provider_id', 'visit_occurrence_id', 'visit_detail_id',
            'unit_source_concept_id', 'measurement_event_id', 'meas_event_field_concept_id'
        ]
        for col in nullable_int_columns:
            if col in df.columns:
                df[col] = df[col].astype('Int32')
        
        # Convert datetime columns - this is crucial for the fix
        if 'measurement_datetime' in df.columns:
            df['measurement_datetime'] = pd.to_datetime(df['measurement_datetime']).dt.tz_localize(None)
        
        if 'measurement_date' in df.columns:
            df['measurement_date'] = pd.to_datetime(df['measurement_date']).dt.date
        
        # Convert numeric columns
        numeric_columns = ['value_as_number', 'range_low', 'range_high']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].astype('float64')
        
        # Ensure string columns fit database constraints
        string_columns = {
            'measurement_time': 10,
            'measurement_source_value': 50,
            'unit_source_value': 50,
            'value_source_value': 50
        }
        for col, max_length in string_columns.items():
            if col in df.columns:
                df[col] = df[col].astype('string').str[:max_length]
        
        return df