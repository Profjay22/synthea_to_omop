import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    schema_cdm: str = 'public'
    schema_vocab: str = 'public'
    
    @classmethod
    def from_env(cls):
        """Create config from environment variables"""
        return cls(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 5432)),
            database=os.getenv('DB_NAME'),
            username=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            schema_cdm=os.getenv('SCHEMA_CDM', 'public'),
            schema_vocab=os.getenv('SCHEMA_VOCAB', 'public')
        )
    
    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"