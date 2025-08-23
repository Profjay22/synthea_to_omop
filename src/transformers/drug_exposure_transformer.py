import pandas as pd
from datetime import datetime
from typing import Optional, Dict
from src.utils.uuid_converter import UUIDConverter

class DrugExposureTransformer:
    """Transform medication and immunization data to OMOP drug_exposure format"""
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        
        # Drug type concept IDs
        self.medication_drug_type_concept_id = 38000176  # EHR administration
        self.immunization_drug_type_concept_id = 38000176  # EHR administration
        
        # Cache for concept lookups to avoid repeated queries
        self._concept_cache = {}
        self._source_concept_cache = {}
    
    def transform_medications(self, medications_df: pd.DataFrame) -> pd.DataFrame:
        """Transform medication source data to OMOP drug_exposure format"""
        
        print(f"🔄 Transforming {len(medications_df)} medications to OMOP drug_exposure format...")
        
        # Drop medications with missing required fields
        required_cols = ['START', 'PATIENT', 'CODE', 'DESCRIPTION']
        medications_df = medications_df.dropna(subset=required_cols)
        
        if medications_df.empty:
            print("❌ No valid medications after filtering")
            return pd.DataFrame()
        
        # Filter to only valid drug domain codes
        if self.db_manager:
            medications_df = self._filter_drug_domain(medications_df)
            print(f"✅ Filtered to {len(medications_df)} records in Drug domain")
        
        # Filter to only include patients that exist in person table
        if self.db_manager:
            medications_df = self._filter_existing_patients(medications_df)
            print(f"✅ Filtered to {len(medications_df)} medications for existing patients")
        
        # Pre-load concept mappings to avoid individual lookups
        if self.db_manager:
            self._preload_concept_mappings(medications_df, code_column='CODE')
        
        drug_records = []
        total_records = len(medications_df)
        
        print(f"🔄 Processing {total_records} medication records...")
        
        for idx, (row_idx, medication) in enumerate(medications_df.iterrows()):
            try:
                # Print progress every 100 records
                if idx % 100 == 0:
                    print(f"   Progress: {idx}/{total_records} ({idx/total_records*100:.1f}%)")
                
                drug_record = self._transform_medication_record(medication, row_index=idx)
                if drug_record:
                    drug_records.append(drug_record)
            except Exception as e:
                print(f"⚠️ Error with medication {idx}: {e}")
                continue
        
        if not drug_records:
            print("❌ No valid drug exposure records created from medications")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(drug_records)
        
        # Fix data types to ensure database compatibility
        result_df = self._fix_data_types(result_df)
        
        print(f"✅ Successfully transformed {len(result_df)} medication drug exposures")
        return result_df
    
    def transform_immunizations(self, immunizations_df: pd.DataFrame) -> pd.DataFrame:
        """Transform immunization source data to OMOP drug_exposure format"""
        
        print(f"🔄 Transforming {len(immunizations_df)} immunizations to OMOP drug_exposure format...")
        
        # Drop immunizations with missing required fields
        required_cols = ['DATE', 'PATIENT', 'CODE', 'DESCRIPTION']
        immunizations_df = immunizations_df.dropna(subset=required_cols)
        
        if immunizations_df.empty:
            print("❌ No valid immunizations after filtering")
            return pd.DataFrame()
        
        # Filter to only valid drug domain codes (CVX should map to RxNorm)
        if self.db_manager:
            immunizations_df = self._filter_drug_domain(immunizations_df)
            print(f"✅ Filtered to {len(immunizations_df)} immunization records in Drug domain")
        
        # Filter to only include patients that exist in person table
        if self.db_manager:
            immunizations_df = self._filter_existing_patients(immunizations_df)
            print(f"✅ Filtered to {len(immunizations_df)} immunizations for existing patients")
        
        # Pre-load concept mappings (CVX → RxNorm)
        if self.db_manager:
            self._preload_concept_mappings(immunizations_df, code_column='CODE')
        
        drug_records = []
        total_records = len(immunizations_df)
        
        print(f"🔄 Processing {total_records} immunization records...")
        
        for idx, (row_idx, immunization) in enumerate(immunizations_df.iterrows()):
            try:
                # Print progress every 100 records
                if idx % 100 == 0:
                    print(f"   Progress: {idx}/{total_records} ({idx/total_records*100:.1f}%)")
                
                drug_record = self._transform_immunization_record(immunization, row_index=idx)
                if drug_record:
                    drug_records.append(drug_record)
            except Exception as e:
                print(f"⚠️ Error with immunization {idx}: {e}")
                continue
        
        if not drug_records:
            print("❌ No valid drug exposure records created from immunizations")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(drug_records)
        
        # Fix data types to ensure database compatibility
        result_df = self._fix_data_types(result_df)
        
        print(f"✅ Successfully transformed {len(result_df)} immunization drug exposures")
        return result_df
    
    def _preload_concept_mappings(self, df: pd.DataFrame, code_column: str = 'CODE') -> None:
        """Pre-load all concept mappings to avoid individual lookups"""
        if not self.db_manager:
            return
            
        try:
            print("🔄 Pre-loading drug concept mappings with vocabulary priority...")
            
            # Get unique codes
            unique_codes = df[code_column].unique()
            
            if len(unique_codes) == 0:
                return
            
            # Build query for all codes at once
            code_list = "', '".join(str(code) for code in unique_codes)
            
            # Query with vocabulary priority for drug concepts
            query = f"""
            WITH ranked_concepts AS (
                SELECT 
                    c.concept_code,
                    c.concept_id as source_concept_id,
                    c.concept_name,
                    c.vocabulary_id,
                    c.standard_concept,
                    COALESCE(cr.concept_id_2, c.concept_id) as standard_concept_id,
                    -- Priority ranking: prefer RxNorm standard, then others
                    CASE 
                        WHEN c.vocabulary_id = 'RxNorm' AND c.standard_concept = 'S' THEN 1
                        WHEN c.vocabulary_id = 'RxNorm' THEN 2
                        WHEN c.vocabulary_id = 'CVX' THEN 3  -- Immunization codes
                        WHEN c.vocabulary_id = 'NDC' THEN 4  -- Drug codes
                        WHEN c.vocabulary_id = 'ATC' THEN 5  -- Drug classification
                        ELSE 99
                    END as vocab_priority,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.concept_code 
                        ORDER BY 
                            CASE 
                                WHEN c.vocabulary_id = 'RxNorm' AND c.standard_concept = 'S' THEN 1
                                WHEN c.vocabulary_id = 'RxNorm' THEN 2
                                WHEN c.vocabulary_id = 'CVX' THEN 3
                                WHEN c.vocabulary_id = 'NDC' THEN 4
                                WHEN c.vocabulary_id = 'ATC' THEN 5
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
                  AND c.domain_id = 'Drug'
                  AND c.vocabulary_id IN ('RxNorm', 'CVX', 'NDC', 'ATC')
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
    
    def _filter_drug_domain(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to only include codes that belong to the Drug domain"""
        try:
            print("🔍 Validating codes against OMOP vocabulary for Drug domain...")
            
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
                  AND c.domain_id = 'Drug'
                  AND c.vocabulary_id IN ('RxNorm', 'CVX', 'NDC', 'ATC')
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
                print("⚠️ No valid drug codes found in OMOP vocabulary")
                return pd.DataFrame()
            
            print(f"📊 Found {len(valid_codes_set)} valid drug codes")
            
            # Filter to only valid drug codes
            filtered_df = df[df['CODE'].astype(str).isin(valid_codes_set)]
            
            invalid_count = len(df) - len(filtered_df)
            if invalid_count > 0:
                print(f"⚠️ Excluded {invalid_count} records not in Drug domain")
            
            return filtered_df
            
        except Exception as e:
            print(f"⚠️ Error validating drug domains: {e}")
            print("⚠️ Proceeding without domain validation")
            return df
    
    def _transform_medication_record(self, medication: pd.Series, row_index: int = 0) -> Optional[dict]:
        """Transform single medication record to OMOP drug_exposure format"""
        
        # Parse dates - ISO format for medications
        start_datetime = self._parse_datetime_iso_format(medication['START'])
        end_datetime = self._parse_datetime_iso_format(medication.get('STOP')) if pd.notna(medication.get('STOP')) else start_datetime
        
        if not start_datetime:
            return None
        
        # Generate unique drug_exposure_id
        unique_string = f"{medication['PATIENT']}_{medication['START']}_{medication['CODE']}"
        
        # Add encounter if available for additional uniqueness
        if pd.notna(medication.get('ENCOUNTER')):
            unique_string += f"_{medication['ENCOUNTER']}"
        
        # Add dispenses for uniqueness (in case same drug prescribed multiple times)
        if pd.notna(medication.get('DISPENSES')):
            unique_string += f"_{medication['DISPENSES']}"
        
        # Add row index as final guarantee of uniqueness
        unique_string += f"_med_row_{row_index}"
        
        drug_exposure_id = UUIDConverter.generic_id(unique_string)
        person_id = UUIDConverter.person_id(medication['PATIENT'])
        
        # Look up visit_occurrence_id if encounter is provided
        visit_occurrence_id = None
        if pd.notna(medication.get('ENCOUNTER')) and self.db_manager:
            visit_occurrence_id = UUIDConverter.visit_occurrence_id(medication['ENCOUNTER'])
        
        # Map drug concept using cache
        drug_concept_id = self._get_cached_concept_id(medication['CODE'])
        drug_source_concept_id = self._get_cached_source_concept_id(medication['CODE'])
        
        # Calculate days supply from start/end dates
        days_supply = None
        if start_datetime and end_datetime and end_datetime > start_datetime:
            days_supply = (end_datetime - start_datetime).days + 1
        
        # Get quantity from DISPENSES
        quantity = None
        if pd.notna(medication.get('DISPENSES')):
            try:
                quantity = float(medication['DISPENSES'])
            except:
                quantity = None
        
        return {
            'drug_exposure_id': drug_exposure_id,
            'person_id': person_id,
            'drug_concept_id': drug_concept_id,
            'drug_exposure_start_date': start_datetime.date(),
            'drug_exposure_start_datetime': start_datetime,
            'drug_exposure_end_date': end_datetime.date() if end_datetime else None,
            'drug_exposure_end_datetime': end_datetime,
            'verbatim_end_date': end_datetime.date() if end_datetime else None,
            'drug_type_concept_id': self.medication_drug_type_concept_id,
            'stop_reason': None,  # Not available in source data
            'refills': None,  # Not available in source data
            'quantity': quantity,
            'days_supply': days_supply,
            'sig': None,  # Not available in source data
            'route_concept_id': None,  # Could be derived from description if needed
            'lot_number': None,  # Not available in source data
            'provider_id': None,  # Could be derived from visit if needed
            'visit_occurrence_id': visit_occurrence_id,
            'visit_detail_id': None,  # Not implemented yet
            'drug_source_value': str(medication['DESCRIPTION'])[:50],
            'drug_source_concept_id': drug_source_concept_id,
            'route_source_value': None,  # Not available in source data
            'dose_unit_source_value': None  # Could be extracted from description if needed
        }
    
    def _transform_immunization_record(self, immunization: pd.Series, row_index: int = 0) -> Optional[dict]:
        """Transform single immunization record to OMOP drug_exposure format"""
        
        # Parse date - ISO format, use as both start and end
        immun_datetime = self._parse_datetime_iso_format(immunization['DATE'])
        if not immun_datetime:
            return None
        
        # Generate unique drug_exposure_id
        unique_string = f"{immunization['PATIENT']}_{immunization['DATE']}_{immunization['CODE']}"
        
        # Add encounter if available for additional uniqueness
        if pd.notna(immunization.get('ENCOUNTER')):
            unique_string += f"_{immunization['ENCOUNTER']}"
        
        # Add row index as final guarantee of uniqueness
        unique_string += f"_imm_row_{row_index}"
        
        drug_exposure_id = UUIDConverter.generic_id(unique_string)
        person_id = UUIDConverter.person_id(immunization['PATIENT'])
        
        # Look up visit_occurrence_id if encounter is provided
        visit_occurrence_id = None
        if pd.notna(immunization.get('ENCOUNTER')) and self.db_manager:
            visit_occurrence_id = UUIDConverter.visit_occurrence_id(immunization['ENCOUNTER'])
        
        # Map drug concept using cache (CVX → RxNorm)
        drug_concept_id = self._get_cached_concept_id(immunization['CODE'])
        drug_source_concept_id = self._get_cached_source_concept_id(immunization['CODE'])
        
        return {
            'drug_exposure_id': drug_exposure_id,
            'person_id': person_id,
            'drug_concept_id': drug_concept_id,
            'drug_exposure_start_date': immun_datetime.date(),
            'drug_exposure_start_datetime': immun_datetime,
            'drug_exposure_end_date': immun_datetime.date(),  # Same as start for immunizations
            'drug_exposure_end_datetime': immun_datetime,     # Same as start for immunizations
            'verbatim_end_date': immun_datetime.date(),       # Same as start for immunizations
            'drug_type_concept_id': self.immunization_drug_type_concept_id,
            'stop_reason': None,
            'refills': None,
            'quantity': 1.0,  # Default to 1 dose/shot for immunizations
            'days_supply': 1,  # Single day for immunizations
            'sig': None,
            'route_concept_id': None,  # Could be "injection" if we wanted to map it
            'lot_number': None,
            'provider_id': None,
            'visit_occurrence_id': visit_occurrence_id,
            'visit_detail_id': None,
            'drug_source_value': str(immunization['DESCRIPTION'])[:50],
            'drug_source_concept_id': drug_source_concept_id,
            'route_source_value': None,
            'dose_unit_source_value': None
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
    
    def _parse_datetime_iso_format(self, datetime_str: str) -> Optional[datetime]:
        """Parse ISO datetime format"""
        if pd.isna(datetime_str):
            return None
        
        try:
            # Handle ISO format: 2015-10-21T14:00:04Z
            return pd.to_datetime(datetime_str).to_pydatetime()
        except:
            return None
    
    def _fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix data types for database compatibility"""
        
        # Convert integer columns
        int_columns = [
            'drug_exposure_id', 'person_id', 'drug_concept_id',
            'drug_type_concept_id', 'drug_source_concept_id'
        ]
        for col in int_columns:
            if col in df.columns:
                df[col] = df[col].astype('int32')
        
        # Convert nullable integer columns
        nullable_int_columns = [
            'refills', 'days_supply', 'route_concept_id', 'provider_id', 
            'visit_occurrence_id', 'visit_detail_id'
        ]
        for col in nullable_int_columns:
            if col in df.columns:
                df[col] = df[col].astype('Int32')
        
        # Convert numeric columns
        if 'quantity' in df.columns:
            df['quantity'] = df['quantity'].astype('float64')
        
        # Convert datetime columns
        datetime_columns = ['drug_exposure_start_datetime', 'drug_exposure_end_datetime']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        
        # Convert date columns
        date_columns = ['drug_exposure_start_date', 'drug_exposure_end_date', 'verbatim_end_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date
        
        # Ensure string columns fit database constraints
        string_columns = {
            'stop_reason': 20,
            'sig': None,  # text field - no limit
            'lot_number': 50,
            'drug_source_value': 50,
            'route_source_value': 50,
            'dose_unit_source_value': 50
        }
        for col, max_length in string_columns.items():
            if col in df.columns and max_length:
                df[col] = df[col].astype('string').str[:max_length]
            elif col in df.columns:
                df[col] = df[col].astype('string')
        
        return df