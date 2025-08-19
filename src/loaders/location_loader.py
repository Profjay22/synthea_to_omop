# src/loaders/location_loader.py

import pandas as pd
from sqlalchemy import text
from src.utils.logging import setup_logging
from src.database.connection import DatabaseManager  


class LocationLoader:

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = setup_logging(log_level="INFO")
        self.schema = db_manager.config.schema_cdm  # ✅ FIXED

    def load_locations(self, locations_df: pd.DataFrame, batch_size: int = 500) -> bool:
        try:
            self.logger.info(f"💾 Loading {len(locations_df)} locations to database...")
            self.db_manager.bulk_insert(locations_df, table_name="location", schema=self.schema)
            self.logger.info("✅ Location data loaded successfully")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to load location data: {e}")
            return False

    def verify_data(self):
        try:
            query = text(f"SELECT COUNT(*) FROM {self.schema}.location")  # ✅ FIXED HERE
            with self.db_manager.engine.connect() as conn:
                result = conn.execute(query)
                count = result.scalar()
            self.logger.info(f"🔍 {count} total locations in OMOP database")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not verify location data: {e}")
