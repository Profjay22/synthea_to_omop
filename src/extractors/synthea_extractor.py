import pandas as pd
from pathlib import Path
from typing import Dict, Optional
import os

class SyntheaExtractor:
    """Extracts data from Synthea CSV files"""
    
    def __init__(self, data_path: str):
        self.data_path = Path(data_path)
        self._validate_path()
        self._cache = {}
    
    def _validate_path(self):
        """Validate that the Synthea data path exists"""
        if not self.data_path.exists():
            raise FileNotFoundError(f"Synthea data path not found: {self.data_path}")
    
    def get_patients(self) -> pd.DataFrame:
        """Extract patient data"""
        return self._load_csv('patients.csv')
    
    def get_encounters(self) -> pd.DataFrame:
        """Extract encounter data"""
        return self._load_csv('encounters.csv')
    
    def get_conditions(self) -> pd.DataFrame:
        """Extract conditions data"""
        return self._load_csv('conditions.csv')
    
    def get_procedures(self) -> pd.DataFrame:
        """Extract procedures data"""
        return self._load_csv('procedures.csv')
    
    def get_medications(self) -> pd.DataFrame:
        """Extract medications data"""
        return self._load_csv('medications.csv')
    
    def get_observations(self) -> pd.DataFrame:
        """Extract observations data"""
        return self._load_csv('observations.csv')
    
    def get_immunizations(self) -> pd.DataFrame:
        """Extract immunizations data"""
        return self._load_csv('immunizations.csv')
    
    def get_careplans(self) -> pd.DataFrame:
        """Extract care plans data"""
        return self._load_csv('careplans.csv')
    
    def get_providers(self) -> pd.DataFrame:
        """Extract providers data"""
        return self._load_csv('providers.csv')
    
    def get_organizations(self) -> pd.DataFrame:
        """Extract organizations data"""
        return self._load_csv('organizations.csv')
    
    def get_allergies(self) -> pd.DataFrame:
        """Extract allergies data"""
        return self._load_csv('allergies.csv')
    
    def get_devices(self) -> pd.DataFrame:
        """Extract devices data"""
        return self._load_csv('devices.csv')
    
    def get_imaging_studies(self) -> pd.DataFrame:
        """Extract imaging studies data"""
        return self._load_csv('imaging_studies.csv')
    
    def get_claims(self) -> pd.DataFrame:
        """Extract claims data"""
        return self._load_csv('claims.csv')
    
    def get_claims_transactions(self) -> pd.DataFrame:
        """Extract claims transactions data"""
        return self._load_csv('claims_transactions.csv')
    
    def get_payers(self) -> pd.DataFrame:
        """Extract payers data"""
        return self._load_csv('payers.csv')
    
    def get_payer_transitions(self) -> pd.DataFrame:
        """Extract payer transitions data"""
        return self._load_csv('payer_transitions.csv')
    
    def get_supplies(self) -> pd.DataFrame:
        """Extract supplies data"""
        return self._load_csv('supplies.csv')
    
    def _load_csv(self, filename: str) -> pd.DataFrame:
        """Load CSV file with caching"""
        if filename in self._cache:
            return self._cache[filename]
        
        file_path = self.data_path / filename
        if not file_path.exists():
            print(f"Warning: {filename} not found in {self.data_path}")
            return pd.DataFrame()
        
        df = pd.read_csv(file_path)
        self._cache[filename] = df
        return df
    
    def get_data_summary(self) -> Dict[str, int]:
        """Get summary of available data"""
        summary = {}
        csv_files = [
            'patients.csv', 'encounters.csv', 'conditions.csv', 
            'procedures.csv', 'medications.csv', 'observations.csv',
            'immunizations.csv', 'careplans.csv', 'providers.csv',
            'organizations.csv', 'allergies.csv', 'devices.csv',
            'imaging_studies.csv', 'claims.csv', 'claims_transactions.csv',
            'payers.csv', 'payer_transitions.csv', 'supplies.csv'
        ]
        
        for file in csv_files:
            df = self._load_csv(file)
            summary[file.replace('.csv', '')] = len(df)
        
        return summary