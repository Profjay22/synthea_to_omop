from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from typing import Generator, Dict, Any, List, Optional
from config.database import DatabaseConfig

class DatabaseManager:
    """Handles database connections and operations"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = create_engine(
            config.connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame"""
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params=params)
    
    def bulk_insert(self, df: pd.DataFrame, table_name: str, schema: str = None) -> None:
        """Bulk insert DataFrame into table"""
        schema = schema or self.config.schema_cdm
        df.to_sql(
            table_name, 
            self.engine, 
            schema=schema,
            if_exists='append', 
            index=False,
            method='multi',
            chunksize=1000
        )
    
    def get_concept_id(self, source_code: str, vocabulary_id: str) -> Optional[int]:
        """Look up OMOP concept_id from vocabulary"""
        query = f"""
        SELECT concept_id 
        FROM {self.config.schema_vocab}.concept 
        WHERE concept_code = %(code)s 
        AND vocabulary_id = %(vocab)s
        AND invalid_reason IS NULL
        """
        result = self.execute_query(query, {
            'code': source_code, 
            'vocab': vocabulary_id
        })
        return result['concept_id'].iloc[0] if not result.empty else None
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False