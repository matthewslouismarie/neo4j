---
title: OCC2 MongoDB Report
author:
 - Klink, Carl
 - Lefebvre, Romain
 - Matthews, Louis-Marie
 - Muller, Julie
date: March the 10th, 2025
---

# Setup

## Cloning the repository

We start our journey by simpling cloning our repository, which contains the original version of the dataset we are working on: `earthquakes_big.geojson.jsonl`.

```
git clone git@github.com:Jlemlr/MongoDB.git
```

## Preparing the dataset

First, as instructed, we will display one row of the original dataset. (We only formatted the first row for readability, no transformation of any kind was performed.)

```json
{
    "type": "Feature",
    "properties": {
        "mag": 0.8,
        "place": "6km W of Cobb, California",
        "time": 1370259968000,
        "updated": 1370260630761,
        "tz": -420,
        "url": "http://earthquake.usgs.gov/earthquakes/eventpage/nc72001620",
        "detail": "http://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/
          nc72001620.geojson",
        "felt": null,
        "cdi": null,
        "mmi": null,
        "alert": null,
        "status": "AUTOMATIC",
        "tsunami": null,
        "sig": 10,
        "net": "nc",
        "code": "72001620",
        "ids": ",nc72001620,",
        "sources": ",nc,",
        "types": ",general-link,geoserve,nearby-cities,origin,phase-data,
        scitech-link,",
        "nst": null,
        "dmin": 0.00898315,
        "rms": 0.06,
        "gap": 82.8,
        "magType": "Md",
        "type": "earthquake"
    },
    "geometry": {
        "type": "Point",
        "coordinates": [
            -122.7955,
            38.8232,
            3
        ]
    },
    "id": "nc72001620"
}
```

We will also make sure to keep a note of the number of lines of the original dataset.

```bash
root@6d7fb3f3b522:/workspaces/MongoDB# wc -l earthquakes_big.geojson.jsonl 
7668 earthquakes_big.geojson.jsonl
```

There are are 7668 newline characters. In other words, there are 766**9** lines. :) 

Next, we load our original dataset `earthquakes_big.geojson.jsonl` into a Pandas DataFrame and transform it into another DataFrame called `merged_df`, the same we used since the Cassandra assignment. For this, we will use `prepare_dataset` function defined in `utils.py`. This is the same preparation code than we used in the Cassandra assignment, with a few improvements. (Which are documented in the file `utils.py`.) **The code that was used is included in the appendices of this report (at the very end of this document).**

Before anything, we install the Python librairies needed by our code:

```bash
pip install -r requirements.txt
```

Then, we create the `merged_df` DataFrame.

```python
merged_df = utils.prepare_dataset(utils.DATASET_PATH)
```

The resulting `merged_df` dataset has the correct types applied. Unneeded (*i.e.* static) columns are removed. Timestamps are converted to the right format and are made timezone-independent. Nested properties are flattened, etc. Duplicates values for magnitude were unified (*e.g.* `mb_Lg` and `MbLg` stand for the same thing).


# Appendices

## utils.py

This is the code that was used to prepare the original `earthquakes_big.geojson.jsonl` dataset. It is very similar to the code we used in the Cassandra assignment, except it is more efficient and performs additional transformations.

What it does is:

 - It loads the original dataset into a Python object (more precisely, a Pandas DataFrame object);
 - It removes columns that do not bring any information (*i.e.* static columns);
 - It applies the correct types.
 - It transforms timezone-dependent timestamps into proper timestamps;
 - It formats values correctly and applies the corresponding types.

```python
"""Utility module for preparing the original JSON earthquakes dataset.

This module was taken from the previous Cassandra report and provides functions
to prepare the `earthquakes_big.geojson.jsonl` dataset in several ways.
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
    df_properties['time'] = df_properties.apply(lambda t: pd.Timestamp(
      t['time'], unit='ms', tz=t['tz']) if not np.isnan(t['time']) else None,
      axis=1
    )
    df_properties['updated'] = df_properties.apply(lambda t: pd.Timestamp(
      t['updated'], unit='ms', tz=t['tz']) if not np.isnan(t['updated']) else None,
      axis=1
    )
    df_properties.drop('tz', axis=1, inplace=True)

    # There are some duplicate values in `magType` column but formatted differently.
    # So let’s remove the duplicate values from the `magType` column.
    df_properties['magType'] = df_properties['magType'].str.lower().str.replace('_', '')

    # If it caused a tsunami (convert to Boolean correct format)
    df_properties['tsunami'] = df_properties['tsunami'] == 1

    df_geometry = pd.json_normalize(df['geometry'])
    df_geometry.drop('type', axis=1, inplace=True) # Contains one unique value

    # Concatenate properties and geometry
    merged_df = pd.concat(
      [
        df.drop(['properties', 'geometry'], axis=1),
        df_properties,
        df_geometry
      ],
      axis=1
    )

    merged_df.rename(str.lower, axis='columns', inplace=True)

    str_columns = [
      'alert',
      'code',
      'detail',
      'id',
      'magtype',
      'place',
      'net',
      'url',
      'status',
      'type'
    ]
    merged_df[str_columns] = merged_df[str_columns].astype(pd.StringDtype())

    return merged_df

DATASET_PATH = 'earthquakes_big.geojson.jsonl'
```