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
                    self.logger.info("🔄 Visit occurrence processing - Coming soon!")
                    continue
                elif table == 'condition_occurrence':
                    self.logger.info("🔄 Condition occurrence processing - Coming soon!")
                    continue
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