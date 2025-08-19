# src/transformers/location_transformer.py
import pandas as pd
import uuid

class LocationTransformer:
    def __init__(self):
        self.location_id_map = {}

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def _generate_location_id(self, row) -> int:
        key = f"{row['ADDRESS']}_{row['CITY']}_{row['STATE']}_{row['ZIP']}_{row['LAT']}_{row['LON']}"
        if key not in self.location_id_map:
            self.location_id_map[key] = uuid.uuid5(uuid.NAMESPACE_DNS, key).int % (10 ** 9)
        return self.location_id_map[key]
