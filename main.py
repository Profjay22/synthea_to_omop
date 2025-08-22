#!/usr/bin/env python3
"""
Main ETL Pipeline for Synthea to OMOP transformation
Built iteratively - starting with Person table
"""

import os
import argparse
from sqlalchemy import text
from config.database import DatabaseConfig
from src.database.connection import DatabaseManager
from src.extractors.synthea_extractor import SyntheaExtractor
from src.utils.logging import setup_logging
import pandas as pd

from src.transformers.person_transformer import PersonTransformer
from src.loaders.person_loader import PersonLoader

class SyntheaToOMOPPipeline:
    def __init__(self, test_mode: bool = True, batch_size: int = 500):
        self.test_mode = test_mode
        self.batch_size = batch_size
        self.logger = setup_logging(log_level="INFO")

        self.db_config = DatabaseConfig.from_env()
        self.db_manager = DatabaseManager(self.db_config)
        self.extractor = SyntheaExtractor(os.getenv('SYNTHEA_DATA_PATH'))

        self.stats = {
            'patients_extracted': 0,
            'persons_transformed': 0,
            'persons_loaded': 0,
            'errors': []
        }

    def run_pipeline(self, tables_to_process: list = None):
        if tables_to_process is None:
            tables_to_process = ['person']

        self.logger.info("🚀 Starting Synthea to OMOP ETL Pipeline")
        self.logger.info(f"Mode: {'TEST' if self.test_mode else 'PRODUCTION'}")
        self.logger.info(f"Tables to process: {tables_to_process}")
        self.logger.info("=" * 60)

        try:
            if not self._setup_and_validate():
                return False

            for table in tables_to_process:
                self.logger.info(f"\n📋 Processing {table.upper()} table...")

                if table == 'person':
                    success = self._process_person_table()
                elif table == 'location':
                    success = self._process_location_table()
                elif table == 'care_site':
                    success = self._process_care_site_table()
                elif table == 'provider':
                    success = self._process_provider_table()
                elif table == 'visit_occurrence':
                    success = self._process_visit_occurrence_table()
                elif table == 'update_person':
                    success = self._update_person_assignments()
                elif table == 'condition_occurrence':
                    success = self._process_condition_occurrence_table()     
                elif table == 'observation':
                    success = self._process_observation_table()
                elif table == 'observation_period':
                    success = self._process_observation_period_table()
                elif table == 'procedure_occurrence':
                    success = self._process_procedure_occurrence_table()
                elif table == 'death':
                    success = self._process_death_table()
                # Also add to the run_pipeline method:
                else:
                    self.logger.warning(f"⚠️ Table {table} not implemented yet")
                    continue

                if not success:
                    self.logger.error(f"❌ Failed to process {table} table")
                    return False

                self.logger.info(f"✅ {table.upper()} table processed successfully")

            self._print_summary()
            return True

        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            return False

    def _setup_and_validate(self) -> bool:
        self.logger.info("1️⃣ Setting up connections and validating data...")

        if not self.db_manager.test_connection():
            self.logger.error("❌ Database connection failed")
            return False

        self.logger.info("✅ Database connection successful")

        try:
            data_summary = self.extractor.get_data_summary()
            self.logger.info("✅ Synthea data accessible")
            self.logger.info(f"📊 Available data: {data_summary}")

            if data_summary.get('patients', 0) == 0:
                self.logger.error("❌ No patient data found")
                return False

        except Exception as e:
            self.logger.error(f"❌ Synthea data validation failed: {e}")
            return False

        return True

    def _process_person_table(self) -> bool:
        try:
            self.clear_person_table()
            self.logger.info("📥 Extracting patient data...")
            patients_df = self.extractor.get_patients()

            if patients_df.empty:
                self.logger.error("❌ No patient data to process")
                return False

            self.stats['patients_extracted'] = len(patients_df)
            self.logger.info(f"✅ Extracted {len(patients_df)} patients")

            if self.test_mode:
                patients_df = patients_df.head(10)
                self.logger.info(f"🧪 Test mode: Processing {len(patients_df)} patients")

            self._show_sample_patient(patients_df)

            self.logger.info("🔄 Transforming to OMOP Person format...")
            transformer = PersonTransformer(self.db_manager)
            omop_persons = transformer.transform(patients_df)

            if omop_persons.empty:
                self.logger.error("❌ Transformation produced no valid records")
                return False

            self.stats['persons_transformed'] = len(omop_persons)
            self.logger.info(f"✅ Transformed {len(omop_persons)} persons")

            self._show_sample_person_omop(omop_persons)

            self.logger.info("💾 Loading to database...")
            loader = PersonLoader(self.db_manager)

            if not loader.load_persons(omop_persons, batch_size=self.batch_size):
                self.logger.error("❌ Database loading failed")
                return False

            self.stats['persons_loaded'] = len(omop_persons)
            loader.verify_data()
            return True

        except Exception as e:
            self.logger.error(f"❌ Person table processing failed: {e}")
            self.stats['errors'].append(f"Person: {str(e)}")
            return False

    def _process_location_table(self) -> bool:
            try:
                self.clear_location_table()
                
                # Extract both provider and patient data
                self.logger.info("📥 Extracting provider data for locations...")
                providers_df = self.extractor.get_providers()
                
                self.logger.info("📥 Extracting patient data for locations...")
                patients_df = self.extractor.get_patients()

                if providers_df.empty and patients_df.empty:
                    self.logger.error("❌ No provider or patient data found")
                    return False

                self.logger.info(f"✅ Extracted {len(providers_df)} providers, {len(patients_df)} patients")

                from src.transformers.location_transformer import LocationTransformer
                transformer = LocationTransformer()  # Remove db_manager parameter
                
                # Use the combined transform method
                omop_locations = transformer.transform_combined(providers_df, patients_df)

                if omop_locations.empty:
                    self.logger.error("❌ No locations after transformation")
                    return False

                self.logger.info(f"✅ Transformed to {len(omop_locations)} unique locations")

                from src.loaders.location_loader import LocationLoader
                loader = LocationLoader(self.db_manager)

                if not loader.load_locations(omop_locations, batch_size=self.batch_size):
                    return False

                loader.verify_data()
                return True

            except Exception as e:
                self.logger.error(f"❌ Location table processing failed: {e}")
                self.stats['errors'].append(f"Location: {str(e)}")
                return False

    def _process_care_site_table(self) -> bool:
        """Process care site table - extracts organizations from provider data"""
        try:
            self.clear_care_site_table()
            self.logger.info("📥 Extracting provider data for care sites...")
            providers_df = self.extractor.get_providers()

            if providers_df.empty:
                self.logger.error("❌ No provider data found")
                return False

            self.logger.info(f"✅ Extracted {len(providers_df)} provider records")

            from src.transformers.care_site_transformer import CareSiteTransformer
            transformer = CareSiteTransformer()
            omop_care_sites = transformer.transform(providers_df)

            if omop_care_sites.empty:
                self.logger.error("❌ No care sites after transformation")
                return False

            self.logger.info(f"✅ Transformed to {len(omop_care_sites)} unique care sites")

            from src.loaders.care_site_loader import CareSiteLoader
            loader = CareSiteLoader(self.db_manager)

            if not loader.load_care_sites(omop_care_sites, batch_size=self.batch_size):
                return False

            loader.verify_data()
            return True

        except Exception as e:
            self.logger.error(f"❌ Care site table processing failed: {e}")
            self.stats['errors'].append(f"Care site: {str(e)}")
            return False

    def _process_provider_table(self) -> bool:
        try:
            self.clear_provider_table()
            self.logger.info("📥 Extracting provider data...")
            providers_df = self.extractor.get_providers()

            if providers_df.empty:
                self.logger.error("❌ No provider data found")
                return False

            self.logger.info(f"✅ Extracted {len(providers_df)} provider records")

            from src.transformers.provider_transformer import ProviderTransformer
            transformer = ProviderTransformer(self.db_manager)
            omop_providers = transformer.transform(providers_df)

            if omop_providers.empty:
                self.logger.error("❌ No providers after transformation")
                return False

            self.logger.info(f"✅ Transformed to {len(omop_providers)} OMOP providers")

            from src.loaders.provider_loader import ProviderLoader
            loader = ProviderLoader(self.db_manager)

            if not loader.load_providers(omop_providers, batch_size=self.batch_size):
                return False

            loader.verify_data()
            return True

        except Exception as e:
            self.logger.error(f"❌ Provider table processing failed: {e}")
            self.stats['errors'].append(f"Provider: {str(e)}")
            return False

    def _process_visit_occurrence_table(self) -> bool:
            """Process visit_occurrence table from encounter data"""
            try:
                self.clear_visit_occurrence_table()
                self.logger.info("📥 Extracting encounter data...")
                encounters_df = self.extractor.get_encounters()

                if encounters_df.empty:
                    self.logger.error("❌ No encounter data found")
                    return False

                self.logger.info(f"✅ Extracted {len(encounters_df)} encounters")

                from src.transformers.visit_occurrence_transformer import VisitOccurrenceTransformer
                transformer = VisitOccurrenceTransformer()
                omop_visits = transformer.transform(encounters_df)

                if omop_visits.empty:
                    self.logger.error("❌ No visit occurrences after transformation")
                    return False

                self.logger.info(f"✅ Transformed to {len(omop_visits)} visit occurrences")

                from src.loaders.visit_occurrence_loader import VisitOccurrenceLoader
                loader = VisitOccurrenceLoader(self.db_manager)

                if not loader.load_visit_occurrences(omop_visits, batch_size=100):  # Smaller batch size
                    return False

                loader.verify_data()
                return True

            except Exception as e:
                self.logger.error(f"❌ Visit occurrence table processing failed: {e}")
                self.stats['errors'].append(f"Visit occurrence: {str(e)}")
                return False
    
    def _update_person_assignments(self) -> bool:
        """Update person table with provider and care site assignments from visit data"""
        try:
            self.logger.info("🔄 Updating person assignments from visit data...")
            
            from src.updaters.person_assignment_updater import PersonAssignmentUpdater
            updater = PersonAssignmentUpdater(self.db_manager)
            
            if not updater.update_assignments():
                self.logger.error("❌ Failed to update person assignments")
                return False
            
            self.logger.info("✅ Person assignments updated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Person assignment update failed: {e}")
            self.stats['errors'].append(f"Person update: {str(e)}")
            return False
    def _process_condition_occurrence_table(self) -> bool:
        """Process condition_occurrence table from condition data"""
        try:
            self.clear_condition_occurrence_table()
            self.logger.info("📥 Extracting condition data...")
            conditions_df = self.extractor.get_conditions()

            if conditions_df.empty:
                self.logger.error("❌ No condition data found")
                return False

            self.logger.info(f"✅ Extracted {len(conditions_df)} conditions")

            from src.transformers.condition_occurrence_transformer import ConditionOccurrenceTransformer
            transformer = ConditionOccurrenceTransformer(self.db_manager)
            omop_conditions = transformer.transform(conditions_df)

            if omop_conditions.empty:
                self.logger.error("❌ No condition occurrences after transformation")
                return False

            self.logger.info(f"✅ Transformed to {len(omop_conditions)} condition occurrences")

            from src.loaders.condition_occurrence_loader import ConditionOccurrenceLoader
            loader = ConditionOccurrenceLoader(self.db_manager)

            if not loader.load_condition_occurrences(omop_conditions, batch_size=100):
                return False

            loader.verify_data()
            return True

        except Exception as e:
            self.logger.error(f"❌ Condition occurrence table processing failed: {e}")
            self.stats['errors'].append(f"Condition occurrence: {str(e)}")
            return False
    def _process_observation_table(self) -> bool:
        """Process observation table from observation data and excluded condition data"""
        try:
            self.clear_observation_table()
            
            all_observations = []
            
            # Process observation source data
            self.logger.info("📥 Extracting observation data...")
            observations_df = self.extractor.get_observations()
            
            if not observations_df.empty:
                self.logger.info(f"✅ Extracted {len(observations_df)} observation records")
                
                from src.transformers.observation_transformer import ObservationTransformer
                transformer = ObservationTransformer(self.db_manager)
                
                omop_observations = transformer.transform_observations(observations_df)
                if not omop_observations.empty:
                    all_observations.append(omop_observations)
                    self.logger.info(f"✅ Transformed {len(omop_observations)} observation records")
            
            # Process excluded condition data (records that should be observations)
            self.logger.info("📥 Extracting excluded condition data for observations...")
            conditions_df = self.extractor.get_conditions()
            
            if not conditions_df.empty:
                # Get conditions that were excluded from condition_occurrence
                excluded_conditions = self._get_excluded_conditions(conditions_df)
                
                if not excluded_conditions.empty:
                    self.logger.info(f"✅ Found {len(excluded_conditions)} excluded conditions to process as observations")
                    
                    transformer = ObservationTransformer(self.db_manager)
                    omop_excluded_obs = transformer.transform_excluded_conditions(excluded_conditions)
                    
                    if not omop_excluded_obs.empty:
                        all_observations.append(omop_excluded_obs)
                        self.logger.info(f"✅ Transformed {len(omop_excluded_obs)} excluded conditions to observations")
            
            # Combine all observation data
            if not all_observations:
                self.logger.error("❌ No observation data to process")
                return False
            
            combined_observations = pd.concat(all_observations, ignore_index=True)
            self.logger.info(f"✅ Combined total: {len(combined_observations)} observation records")
            
            # Load to database
            from src.loaders.observation_loader import ObservationLoader
            loader = ObservationLoader(self.db_manager)

            if not loader.load_observations(combined_observations, batch_size=50):
                return False

            loader.verify_data()
            return True

        except Exception as e:
            self.logger.error(f"❌ Observation table processing failed: {e}")
            self.stats['errors'].append(f"Observation: {str(e)}")
            return False
        
    def _get_excluded_conditions(self, conditions_df: pd.DataFrame) -> pd.DataFrame:
        """Get condition records that were excluded from condition_occurrence (should be observations)"""
        try:
            if not self.db_manager:
                return pd.DataFrame()
            
            # Get unique SNOMED codes from conditions
            unique_codes = conditions_df['CODE'].unique()
            code_list = "', '".join(unique_codes.astype(str))
            
            # Find codes that are NOT in Condition domain (should be observations)
            excluded_codes_query = f"""
            SELECT DISTINCT 
                c.concept_code,
                c.concept_id,
                c.concept_name,
                c.domain_id,
                c.vocabulary_id
            FROM {self.db_manager.config.schema_cdm}.concept c
            WHERE c.concept_code IN ('{code_list}')
              AND c.vocabulary_id = 'SNOMED'
              AND c.domain_id != 'Condition'
              AND c.domain_id = 'Observation'
              AND c.invalid_reason IS NULL
            """
            
            excluded_codes = self.db_manager.execute_query(excluded_codes_query)
            
            if excluded_codes.empty:
                self.logger.info("ℹ️ No condition codes found that should be observations")
                return pd.DataFrame()
            
            self.logger.info(f"📊 Found {len(excluded_codes)} condition codes that belong in Observation domain:")
            for _, code_info in excluded_codes.head(3).iterrows():
                self.logger.info(f"  {code_info['concept_code']}: {code_info['concept_name']}")
            
            # Filter conditions to only those that should be observations
            excluded_codes_set = set(excluded_codes['concept_code'].astype(str))
            excluded_conditions = conditions_df[
                conditions_df['CODE'].astype(str).isin(excluded_codes_set)
            ]
            
            return excluded_conditions
            
        except Exception as e:
            self.logger.error(f"⚠️ Error getting excluded conditions: {e}")
            return pd.DataFrame()
    
    def _process_observation_period_table(self) -> bool:
        """Process observation_period table by calculating periods from all source data"""
        try:
            self.clear_observation_period_table()
            self.logger.info("🔄 Calculating observation periods from all source data...")
            
            from src.transformers.observation_period_transformer import ObservationPeriodTransformer
            transformer = ObservationPeriodTransformer(self.extractor)
            
            observation_periods = transformer.transform()
            
            if observation_periods.empty:
                self.logger.error("❌ No observation periods calculated")
                return False

            self.logger.info(f"✅ Calculated {len(observation_periods)} observation periods")

            from src.loaders.observation_period_loader import ObservationPeriodLoader
            loader = ObservationPeriodLoader(self.db_manager)

            if not loader.load_observation_periods(observation_periods, batch_size=500):
                return False

            loader.verify_data()
            return True

        except Exception as e:
            self.logger.error(f"❌ Observation period table processing failed: {e}")
            self.stats['errors'].append(f"Observation period: {str(e)}")
            return False
    
    def _process_procedure_occurrence_table(self) -> bool:
        """Process procedure_occurrence table from procedure data and observation procedure data"""
        try:
            self.clear_procedure_occurrence_table()
            
            all_procedures = []
            
            # Process procedure source data
            self.logger.info("📥 Extracting procedure data...")
            procedures_df = self.extractor.get_procedures()
            
            if not procedures_df.empty:
                self.logger.info(f"✅ Extracted {len(procedures_df)} procedure records")
                
                from src.transformers.procedure_occurrence_transformer import ProcedureOccurrenceTransformer
                transformer = ProcedureOccurrenceTransformer(self.db_manager)
                
                omop_procedures = transformer.transform_procedures(procedures_df)
                if not omop_procedures.empty:
                    all_procedures.append(omop_procedures)
                    self.logger.info(f"✅ Transformed {len(omop_procedures)} procedure records")
            
            # Process observation data for procedures (CATEGORY='procedure')
            self.logger.info("📥 Extracting observation data for procedures...")
            observations_df = self.extractor.get_observations()
            
            if not observations_df.empty:
                transformer = ProcedureOccurrenceTransformer(self.db_manager)
                omop_obs_procedures = transformer.transform_observation_procedures(observations_df)
                
                if not omop_obs_procedures.empty:
                    all_procedures.append(omop_obs_procedures)
                    self.logger.info(f"✅ Transformed {len(omop_obs_procedures)} observation procedures")
            
            # Combine all procedure data
            if not all_procedures:
                self.logger.error("❌ No procedure data to process")
                return False
            
            combined_procedures = pd.concat(all_procedures, ignore_index=True)
            self.logger.info(f"✅ Combined total: {len(combined_procedures)} procedure occurrence records")
            
            # Load to database
            from src.loaders.procedure_occurrence_loader import ProcedureOccurrenceLoader
            loader = ProcedureOccurrenceLoader(self.db_manager)

            if not loader.load_procedure_occurrences(combined_procedures, batch_size=100):
                return False

            loader.verify_data()
            return True

        except Exception as e:
            self.logger.error(f"❌ Procedure occurrence table processing failed: {e}")
            self.stats['errors'].append(f"Procedure occurrence: {str(e)}")
            return False
    
    def _process_death_table(self) -> bool:
        """Process death table from patient and observation data"""
        try:
            self.clear_death_table()
            
            # Extract patient data (needed for death dates)
            self.logger.info("📥 Extracting patient data for deaths...")
            patients_df = self.extractor.get_patients()
            
            if patients_df.empty:
                self.logger.error("❌ No patient data found")
                return False
            
            # Extract observation data (needed for death certificates)
            self.logger.info("📥 Extracting observation data for death certificates...")
            observations_df = self.extractor.get_observations()
            
            if observations_df.empty:
                self.logger.warning("⚠️ No observation data found - will process deaths without certificate info")
                observations_df = pd.DataFrame()  # Empty dataframe for transformer
            
            self.logger.info(f"✅ Extracted {len(patients_df)} patients and {len(observations_df)} observations")
            
            # Transform death data
            from src.transformers.death_transformer import DeathTransformer
            transformer = DeathTransformer(self.db_manager)
            
            omop_deaths = transformer.transform(patients_df, observations_df)
            
            if omop_deaths.empty:
                self.logger.error("❌ No death records after transformation")
                return False
            
            self.logger.info(f"✅ Transformed {len(omop_deaths)} death records")
            
            # Load to database
            from src.loaders.death_loader import DeathLoader
            loader = DeathLoader(self.db_manager)
            
            if not loader.load_deaths(omop_deaths, batch_size=500):
                return False
            
            loader.verify_data()
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Death table processing failed: {e}")
            self.stats['errors'].append(f"Death: {str(e)}")
            return False

    def _show_sample_patient(self, patients_df):
        sample = patients_df.iloc[0]
        self.logger.info("📋 Sample source patient:")
        self.logger.info(f"  ID: {sample['Id']}")
        self.logger.info(f"  Birth: {sample['BIRTHDATE']}")
        self.logger.info(f"  Gender: {sample['GENDER']}")
        self.logger.info(f"  Race: {sample.get('RACE', 'N/A')}")
        self.logger.info(f"  Ethnicity: {sample.get('ETHNICITY', 'N/A')}")

    def _show_sample_person_omop(self, omop_persons):
        sample = omop_persons.iloc[0]
        self.logger.info("📋 Sample OMOP person:")
        self.logger.info(f"  person_id: {sample['person_id']}")
        self.logger.info(f"  gender_concept_id: {sample['gender_concept_id']}")
        self.logger.info(f"  year_of_birth: {sample['year_of_birth']}")
        self.logger.info(f"  race_concept_id: {sample['race_concept_id']}")
        self.logger.info(f"  ethnicity_concept_id: {sample['ethnicity_concept_id']}")

    def _print_summary(self):
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 PIPELINE EXECUTION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Patients extracted: {self.stats['patients_extracted']}")
        self.logger.info(f"Persons transformed: {self.stats['persons_transformed']}")
        self.logger.info(f"Persons loaded: {self.stats['persons_loaded']}")

        if self.stats['errors']:
            self.logger.info(f"Errors: {len(self.stats['errors'])}")
            for error in self.stats['errors']:
                self.logger.info(f"  - {error}")
        else:
            self.logger.info("Errors: 0")

        self.logger.info("\n🎉 Pipeline completed successfully!")
        self.logger.info("👉 Check your database in DataGrip to verify results")

    def clear_person_table(self):
        self.logger.info("🧹 Clearing person table...")
        try:
            schema = self.db_config.schema_cdm
            with self.db_manager.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {schema}.person RESTART IDENTITY CASCADE"))
            self.logger.info("✅ Person table cleared")
        except Exception as e:
            self.logger.error(f"❌ Clear failed: {e}")

    def clear_location_table(self):
        self.logger.info("🧹 Clearing location table...")
        try:
            schema = self.db_config.schema_cdm
            with self.db_manager.engine.begin() as conn:
                # Use DELETE instead of TRUNCATE to avoid foreign key issues
                conn.execute(text(f"DELETE FROM {schema}.location"))
            self.logger.info("✅ Location table cleared")
        except Exception as e:
            self.logger.error(f"❌ Clear failed: {e}")

    def clear_care_site_table(self):
        self.logger.info("🧹 Clearing care_site table...")
        try:
            schema = self.db_config.schema_cdm
            with self.db_manager.engine.begin() as conn:
                # Use DELETE instead of TRUNCATE to avoid foreign key issues
                conn.execute(text(f"DELETE FROM {schema}.care_site"))
            self.logger.info("✅ Care site table cleared")
        except Exception as e:
            self.logger.error(f"❌ Clear failed: {e}")

    def clear_provider_table(self):
        self.logger.info("🧹 Clearing provider table...")
        try:
            schema = self.db_config.schema_cdm
            with self.db_manager.engine.begin() as conn:
                # Use DELETE instead of TRUNCATE to avoid foreign key issues
                conn.execute(text(f"DELETE FROM {schema}.provider"))
            self.logger.info("✅ Provider table cleared")
        except Exception as e:
            self.logger.error(f"❌ Clear failed: {e}")
    
    def clear_visit_occurrence_table(self):
            self.logger.info("🧹 Clearing visit_occurrence table...")
            try:
                schema = self.db_config.schema_cdm
                with self.db_manager.engine.begin() as conn:
                    # Use DELETE instead of TRUNCATE to avoid foreign key issues
                    conn.execute(text(f"DELETE FROM {schema}.visit_occurrence"))
                self.logger.info("✅ Visit occurrence table cleared")
            except Exception as e:
                self.logger.error(f"❌ Clear failed: {e}")
    
    def clear_condition_occurrence_table(self):
            self.logger.info("🧹 Clearing condition_occurrence table...")
            try:
                schema = self.db_config.schema_cdm
                with self.db_manager.engine.begin() as conn:
                    # Use DELETE instead of TRUNCATE to avoid foreign key issues
                    conn.execute(text(f"DELETE FROM {schema}.condition_occurrence"))
                self.logger.info("✅ Condition occurrence table cleared")
            except Exception as e:
                self.logger.error(f"❌ Clear failed: {e}")
    
    def clear_observation_table(self):
        self.logger.info("🧹 Clearing observation table...")
        try:
            schema = self.db_config.schema_cdm
            with self.db_manager.engine.begin() as conn:
                # Use DELETE instead of TRUNCATE to avoid foreign key issues
                conn.execute(text(f"DELETE FROM {schema}.observation"))
            self.logger.info("✅ Observation table cleared")
        except Exception as e:
            self.logger.error(f"❌ Clear failed: {e}")
    
    def clear_observation_period_table(self):
        self.logger.info("🧹 Clearing observation_period table...")
        try:
            schema = self.db_config.schema_cdm
            with self.db_manager.engine.begin() as conn:
                # Use DELETE instead of TRUNCATE to avoid foreign key issues
                conn.execute(text(f"DELETE FROM {schema}.observation_period"))
            self.logger.info("✅ Observation period table cleared")
        except Exception as e:
            self.logger.error(f"❌ Clear failed: {e}")
    
    def clear_procedure_occurrence_table(self):
        self.logger.info("🧹 Clearing procedure_occurrence table...")
        try:
            schema = self.db_config.schema_cdm
            with self.db_manager.engine.begin() as conn:
                # Use DELETE instead of TRUNCATE to avoid foreign key issues
                conn.execute(text(f"DELETE FROM {schema}.procedure_occurrence"))
            self.logger.info("✅ Procedure occurrence table cleared")
        except Exception as e:
            self.logger.error(f"❌ Clear failed: {e}")
    
    def clear_death_table(self):
        self.logger.info("🧹 Clearing death table...")
        try:
            schema = self.db_config.schema_cdm
            with self.db_manager.engine.begin() as conn:
                # Use DELETE instead of TRUNCATE to avoid foreign key issues
                conn.execute(text(f"DELETE FROM {schema}.death"))
            self.logger.info("✅ Death table cleared")
        except Exception as e:
            self.logger.error(f"❌ Clear failed: {e}")


    
def main():
    parser = argparse.ArgumentParser(description='Synthea to OMOP ETL Pipeline')
    parser.add_argument('--test', action='store_true', help='Run in test mode (small sample)')
    parser.add_argument('--clear', action='store_true', help='Clear tables before running')
    parser.add_argument('--tables', nargs='+', default=['person'], help='Tables to process (default: person)')
    parser.add_argument('--batch-size', type=int, default=500, help='Batch size for processing (default: 500)')

    args = parser.parse_args()

    pipeline = SyntheaToOMOPPipeline(test_mode=args.test, batch_size=args.batch_size)

    if args.clear:
        if 'person' in args.tables:
            pipeline.clear_person_table()
        if 'location' in args.tables:
            pipeline.clear_location_table()
        if 'care_site' in args.tables:
            pipeline.clear_care_site_table()
        if 'provider' in args.tables:
            pipeline.clear_provider_table()

    success = pipeline.run_pipeline(tables_to_process=args.tables)

    if success:
        print("\n✅ ETL Pipeline completed successfully!")
        print("👉 Check DataGrip to verify your data")
    else:
        print("\n❌ ETL Pipeline failed")
        exit(1)

if __name__ == "__main__":
    main()