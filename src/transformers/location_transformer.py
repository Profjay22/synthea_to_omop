# src/transformers/location_transformer.py
import pandas as pd
import uuid
from src.database.connection import DatabaseManager

class LocationTransformer:
    def __init__(self):
        self.location_id_map = {}

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform provider data to extract locations (existing method)"""
        required_cols = ['ADDRESS', 'CITY', 'STATE', 'ZIP', 'LAT', 'LON']
        df = df.dropna(subset=required_cols)

        # Remove duplicates
        df = df.drop_duplicates(subset=required_cols)

        # Generate location_id (uuid int)
        df['location_id'] = df.apply(lambda row: self._generate_location_id(row), axis=1)

        # Rename columns to match OMOP
        df_omop = df[['location_id', 'ADDRESS', 'CITY', 'STATE', 'ZIP', 'LAT', 'LON']].copy()
        df_omop.columns = ['location_id', 'address_1', 'city', 'state', 'zip', 'latitude', 'longitude']

        return df_omop

    def transform_combined(self, providers_df: pd.DataFrame, patients_df: pd.DataFrame) -> pd.DataFrame:
        """Transform both provider and patient data to extract all unique locations"""
        
        print(f"🔄 Extracting locations from {len(providers_df)} providers and {len(patients_df)} patients...")
        
        all_locations = []
        
        # Extract provider locations
        provider_locations = self._extract_provider_locations(providers_df)
        all_locations.extend(provider_locations)
        print(f"✅ Found {len(provider_locations)} provider locations")
        
        # Extract patient locations
        patient_locations = self._extract_patient_locations(patients_df)
        all_locations.extend(patient_locations)
        print(f"✅ Found {len(patient_locations)} patient locations")
        
        if not all_locations:
            print("❌ No locations found")
            return pd.DataFrame()
        
        # Create DataFrame and remove duplicates
        locations_df = pd.DataFrame(all_locations)
        
        # Remove duplicates based on address combination
        unique_locations = locations_df.drop_duplicates(
            subset=['address_1', 'city', 'state', 'zip']
        ).reset_index(drop=True)
        
        # Generate location_ids for unique locations
        unique_locations['location_id'] = unique_locations.apply(
            lambda row: self._generate_location_id_from_address(row), axis=1
        )
        
        print(f"✅ Total: {len(all_locations)} locations, {len(unique_locations)} unique")
        return unique_locations

    def _extract_provider_locations(self, providers_df: pd.DataFrame) -> list:
        """Extract locations from provider data"""
        locations = []
        required_cols = ['ADDRESS', 'CITY', 'STATE', 'ZIP', 'LAT', 'LON']
        
        # Filter valid provider locations
        valid_providers = providers_df.dropna(subset=required_cols)
        provider_locations = valid_providers[required_cols].drop_duplicates()
        
        for _, row in provider_locations.iterrows():
            # Truncate long values to fit database constraints
            address = str(row['ADDRESS'])[:50]  # Limit to 50 chars
            city = str(row['CITY'])[:50]
            state = str(row['STATE'])[:2]  # State codes are usually 2 chars
            location_source = f"Provider: {address}"[:50]  # Limit to 50 chars
            
            location = {
                'address_1': address,
                'city': city,
                'state': state,
                'zip': str(row['ZIP']).zfill(5)[:5],  # ZIP codes are 5 digits
                'latitude': float(row['LAT']) if pd.notna(row['LAT']) else None,
                'longitude': float(row['LON']) if pd.notna(row['LON']) else None,
                'county': None,
                'location_source_value': location_source
            }
            locations.append(location)
        
        return locations

    def _extract_patient_locations(self, patients_df: pd.DataFrame) -> list:
        """Extract locations from patient data"""
        locations = []
        
        # Check what address columns exist in patient data
        required_cols = ['ADDRESS', 'CITY', 'STATE', 'ZIP', 'LAT', 'LON']
        
        # Check if all required columns exist
        missing_cols = [col for col in required_cols if col not in patients_df.columns]
        if missing_cols:
            print(f"⚠️ Patient data missing columns: {missing_cols}")
            return locations
        
        # Filter valid patient locations
        valid_patients = patients_df.dropna(subset=required_cols)
        patient_locations = valid_patients[required_cols].drop_duplicates()
        
        for _, row in patient_locations.iterrows():
            # Truncate long values to fit database constraints
            address = str(row['ADDRESS'])[:50]  # Limit to 50 chars
            city = str(row['CITY'])[:50]
            state = str(row['STATE'])[:2]  # State codes are usually 2 chars
            location_source = f"Patient: {address}"[:50]  # Limit to 50 chars
            
            location = {
                'address_1': address,
                'city': city,
                'state': state,
                'zip': str(row['ZIP']).zfill(5)[:5],  # ZIP codes are 5 digits
                'latitude': float(row['LAT']) if pd.notna(row['LAT']) else None,
                'longitude': float(row['LON']) if pd.notna(row['LON']) else None,
                'county': None,
                'location_source_value': location_source
            }
            locations.append(location)
        
        return locations

    def _generate_location_id(self, row) -> int:
        """Generate location_id from provider data (existing method)"""
        key = f"{row['ADDRESS']}_{row['CITY']}_{row['STATE']}_{row['ZIP']}_{row['LAT']}_{row['LON']}"
        if key not in self.location_id_map:
            self.location_id_map[key] = uuid.uuid5(uuid.NAMESPACE_DNS, key).int % (10 ** 9)
        return self.location_id_map[key]

    def _generate_location_id_from_address(self, row) -> int:
        """Generate location_id from address data (for combined approach)"""
        # Use lat/lon if available, otherwise just address
        if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
            key = f"{row['address_1']}_{row['city']}_{row['state']}_{row['zip']}_{row['latitude']}_{row['longitude']}"
        else:
            key = f"{row['address_1']}_{row['city']}_{row['state']}_{row['zip']}"
        
        if key not in self.location_id_map:
            self.location_id_map[key] = uuid.uuid5(uuid.NAMESPACE_DNS, key).int % (10 ** 9)
        return self.location_id_map[key]