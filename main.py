#!/usr/bin/env python3
"""
Main ETL Pipeline for Synthea to OMOP transformation
Built iteratively - starting with Person table
"""
 
import os
import argparse
from sqlalchemy import text   # ✅ added for safe SQL execution
from config.database import DatabaseConfig
from src.database.connection import DatabaseManager
from src.extractors.synthea_extractor import SyntheaExtractor
from src.utils.logging import setup_logging

# Import transformers (will add more iteratively)
from src.transformers.person_transformer import PersonTransformer
from src.loaders.person_loader import PersonLoader


class SyntheaToOMOPPipeline:
    """Main ETL pipeline class"""
    
    def __init__(self, test_mode: bool = True, batch_size: int = 500):
        self.test_mode = test_mode
        self.batch_size = batch_size
        self.logger = setup_logging(log_level="INFO")
        
        # Initialize components
        self.db_config = DatabaseConfig.from_env()
        self.db_manager = DatabaseManager(self.db_config)
        self.extractor = SyntheaExtractor(os.getenv('SYNTHEA_DATA_PATH'))
        
        # Statistics
        self.stats = {
            'patients_extracted': 0,
            'persons_transformed': 0,
            'persons_loaded': 0,
            'errors': []
        }

    def run_pipeline(self, tables_to_process: list = None):
        """Run the complete ETL pipeline"""
        
        if tables_to_process is None:
            tables_to_process = ['person']  # Start with person only
        
        self.logger.info("🚀 Starting Synthea to OMOP ETL Pipeline")
        self.logger.info(f"Mode: {'TEST' if self.test_mode else 'PRODUCTION'}")
        self.logger.info(f"Tables to process: {tables_to_process}")
        self.logger.info("=" * 60)
        
        try:
            # 1. Setup and validation
            if not self._setup_and_validate():
                return False
            
            # 2. Process each table iteratively
            for table in tables_to_process:
                self.logger.info(f"\n📋 Processing {table.upper()} table...")
                
                if table == 'person':
                    success = self._process_person_table()
                elif table == 'location':
                    success = self._process_location_table()

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
            
            # 3. Final summary
            self._print_summary()
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            return False

    def _setup_and_validate(self) -> bool:
        """Setup and validate all connections"""
        
        self.logger.info("1️⃣ Setting up connections and validating data...")
        
        # Test database connection
        if not self.db_manager.test_connection():
            self.logger.error("❌ Database connection failed")
            return False
        
        self.logger.info("✅ Database connection successful")
        
        # Validate Synthea data
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
        """Process Person table specifically"""
        try:
            # ✅ Always clear before loading new data
            self.clear_person_table()

            # Extract patients
            self.logger.info("📥 Extracting patient data...")
            patients_df = self.extractor.get_patients()
            
            if patients_df.empty:
                self.logger.error("❌ No patient data to process")
                return False
            
            self.stats['patients_extracted'] = len(patients_df)
            self.logger.info(f"✅ Extracted {len(patients_df)} patients")
            
            # Test mode: limit to smaller sample
            if self.test_mode:
                patients_df = patients_df.head(10)
                self.logger.info(f"🧪 Test mode: Processing {len(patients_df)} patients")
            
            # Show sample
            self._show_sample_patient(patients_df)
            
            # Transform
            self.logger.info("🔄 Transforming to OMOP Person format...")
            transformer = PersonTransformer()
            omop_persons = transformer.transform(patients_df)
            
            if omop_persons.empty:
                self.logger.error("❌ Transformation produced no valid records")
                return False
            
            self.stats['persons_transformed'] = len(omop_persons)
            self.logger.info(f"✅ Transformed {len(omop_persons)} persons")
            
            # Show sample transformed
            self._show_sample_person_omop(omop_persons)
            
            # Load
            self.logger.info("💾 Loading to database...")
            loader = PersonLoader(self.db_manager)
            
            if not loader.load_persons(omop_persons, batch_size=self.batch_size):
                self.logger.error("❌ Database loading failed")
                return False
            
            self.stats['persons_loaded'] = len(omop_persons)
            
            # Verify
            self.logger.info("🔍 Verifying loaded data...")
            loader.verify_data()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Person table processing failed: {e}")
            self.stats['errors'].append(f"Person: {str(e)}")
            return False

    def _process_location_table(self) -> bool:
        try:
            self.clear_location_table()  # ✅ Clear location table before inserting

            self.logger.info("📥 Extracting provider data for location...")
            providers_df = self.extractor.get_providers()

            if providers_df.empty:
                self.logger.error("❌ No provider data found")
                return False

            self.logger.info(f"✅ Extracted {len(providers_df)} provider records")

            from src.transformers.location_transformer import LocationTransformer
            transformer = LocationTransformer()
            omop_locations = transformer.transform(providers_df)

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

    def _show_sample_patient(self, patients_df):
        """Show sample source patient data"""
        sample = patients_df.iloc[0]
        self.logger.info("📋 Sample source patient:")
        self.logger.info(f"  ID: {sample['Id']}")
        self.logger.info(f"  Birth: {sample['BIRTHDATE']}")
        self.logger.info(f"  Gender: {sample['GENDER']}")
        self.logger.info(f"  Race: {sample.get('RACE', 'N/A')}")
        self.logger.info(f"  Ethnicity: {sample.get('ETHNICITY', 'N/A')}")

    def _show_sample_person_omop(self, omop_persons):
        """Show sample transformed OMOP person"""
        sample = omop_persons.iloc[0]
        self.logger.info("📋 Sample OMOP person:")
        self.logger.info(f"  person_id: {sample['person_id']}")
        self.logger.info(f"  gender_concept_id: {sample['gender_concept_id']}")
        self.logger.info(f"  year_of_birth: {sample['year_of_birth']}")
        self.logger.info(f"  race_concept_id: {sample['race_concept_id']}")
        self.logger.info(f"  ethnicity_concept_id: {sample['ethnicity_concept_id']}")

    def _print_summary(self):
        """Print pipeline execution summary"""
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
        """Clear person table for fresh run"""
        self.logger.info("🧹 Clearing person table...")
        try:
            schema = self.db_config.schema_cdm
            with self.db_manager.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {schema}.person RESTART IDENTITY CASCADE"))
            self.logger.info("✅ Person table cleared")
        except Exception as e:
            self.logger.error(f"❌ Clear failed: {e}")

    def clear_location_table(self):
        """Clear location table for fresh run"""
        self.logger.info("🧹 Clearing location table...")
        try:
            schema = self.db_config.schema_cdm
            with self.db_manager.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {schema}.location RESTART IDENTITY CASCADE"))
            self.logger.info("✅ Location table cleared")
        except Exception as e:
            self.logger.error(f"❌ Clear failed: {e}")


def main():
    """Main entry point"""
    
    parser = argparse.ArgumentParser(description='Synthea to OMOP ETL Pipeline')
    parser.add_argument('--test', action='store_true',
                       help='Run in test mode (small sample)')
    parser.add_argument('--clear', action='store_true',
                       help='Clear tables before running')
    parser.add_argument('--tables', nargs='+', default=['person'],
                       help='Tables to process (default: person)')
    parser.add_argument('--batch-size', type=int, default=500,
                       help='Batch size for processing (default: 500)')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = SyntheaToOMOPPipeline(
        test_mode=args.test,
        batch_size=args.batch_size
    )
    
    # Clear tables if requested
    if args.clear:
        if 'person' in args.tables:
            pipeline.clear_person_table()
        if 'location' in args.tables:
            pipeline.clear_location_table()
    
    # Run pipeline
    success = pipeline.run_pipeline(tables_to_process=args.tables)
    
    if success:
        print("\n✅ ETL Pipeline completed successfully!")
        print("👉 Check DataGrip to verify your data")
    else:
        print("\n❌ ETL Pipeline failed")
        exit(1)


if __name__ == "__main__":
    main()
