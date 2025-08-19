# src/loaders/provider_loader.py

import pandas as pd
from sqlalchemy import text
from src.utils.logging import setup_logging
from src.database.connection import DatabaseManager

class ProviderLoader:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = setup_logging(log_level="INFO")
        self.schema = db_manager.config.schema_cdm

    def load_providers(self, df: pd.DataFrame, batch_size: int = 500) -> bool:
        try:
            self.logger.info(f"💾 Loading {len(df)} providers to database...")
            self.db_manager.bulk_insert(df, table_name="provider", schema=self.schema)
            self.logger.info("✅ Provider data loaded successfully")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to load provider data: {e}")
            return False

    def verify_data(self):
        try:
            query = text(f"SELECT COUNT(*) FROM {self.schema}.provider")
            with self.db_manager.engine.connect() as conn:
                count = conn.execute(query).scalar()
            self.logger.info(f"🔍 {count} total providers in OMOP database")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not verify provider data: {e}")
