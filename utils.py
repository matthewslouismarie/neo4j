"""Utility module for preparing the original JSON earthquakes dataset.

This module was taken from the previous Cassandra report and provides functions
to prepare the `earthquakes_big.geojson.json` dataset in several ways.
"""

import pandas as pd
import numpy as np

def prepare_dataset(df_path) -> pd.DataFrame:
    """Prepare the original dataset file named 'merged_df'.
    
    This method performs several improvements on the original JSON earthquakes
    dataset, such as applying the correct types, removing unneeded (*i.e.*
    static) columns, fixing the timestamps incorrectly encoded, etc."""

    df = pd.read_json(df_path, lines=True)

    df.drop('type', axis=1, inplace=True) # Contains one unique value

    df_properties = pd.json_normalize(df['properties'])

    # Format types, sources and ids using correct array notation
    df_properties['types'] = df_properties['types'].apply(lambda x: x[1:-1].split(','))
    df_properties['sources'] = df_properties['sources'].apply(lambda x: x[1:-1].split(','))
    df_properties['ids'] = df_properties['ids'].apply(lambda x: x[1:-1].split(','))

    # Convert TZ-aware timestamps
    df_properties['time'] = df_properties.apply(lambda t: pd.Timestamp(t['time'], unit='ms', tz=t['tz']) if not np.isnan(t['time']) else None, axis=1)
    df_properties['updated'] = df_properties.apply(lambda t: pd.Timestamp(t['updated'], unit='ms', tz=t['tz']) if not np.isnan(t['updated']) else None, axis=1)
    df_properties.drop('tz', axis=1, inplace=True)

    # There are some duplicate values in `magType` column but formatted differently.
    # So let’s remove the duplicate values from the `magType` column.
    df_properties['magType'] = df_properties['magType'].str.lower().str.replace('_', '')

    # If it caused a tsunami (convert to Boolean correct format)
    df_properties['tsunami'] = df_properties['tsunami'] == 1

    df_geometry = pd.json_normalize(df['geometry'])
    df_geometry.drop('type', axis=1, inplace=True) # Contains one unique value

    # Concatenate properties and geometry
    merged_df = pd.concat([df.drop(['properties', 'geometry'], axis=1), df_properties, df_geometry], axis=1)

    merged_df.rename(str.lower, axis='columns', inplace=True)

    str_columns = ['alert', 'code', 'detail', 'id', 'magtype', 'place', 'net', 'url', 'status', 'type']
    merged_df[str_columns] = merged_df[str_columns].astype(pd.StringDtype())

    return merged_df

DATASET_PATH = 'earthquakes_big.geojson.json'