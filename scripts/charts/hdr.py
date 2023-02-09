""" """

import pandas as pd
import numpy as np
import country_converter as coco
from bblocks.dataframe_tools import add


from scripts.config import PATHS

GII = pd.read_csv(f'{PATHS.raw_data}/hdr_gii.csv')

def _keep_only_indicator(df: pd.DataFrame, indicator: str, col: str = 'indicator'):
    """Helper function to filter dataframe keeping only selected indicator"""

    return df.loc[df[col] == indicator].reset_index(drop=True)


def get_gii_regions_timeseries():
    """ """
    regions = ['Arab States', 'Europe and Central Asia',
               'Latin America and the Caribbean', 'South Asia', 'World',
               'East Asia and the Pacific', 'Sub-Saharan Africa']

    return (GII.pipe(_keep_only_indicator, 'gii', 'variable')
            .loc[lambda d: d.country.isin(regions)]
            .pivot(index='year', columns='country', values='value')
            .reset_index()
            )


def get_country_latest():
    """ """

    return (GII
     .pipe(_keep_only_indicator, 'gii', 'variable')
     .dropna(subset = ['hdicode', 'value'])
     .loc[lambda d: d.groupby('country')['year'].idxmax()]
     .assign(continent = lambda d: coco.convert(d.iso3, to='continent'),
             )
     .pipe(add.add_population_column, 'iso3', 'ISO3')
     )

def histogram():
    """ """

    b = np.array([0. , 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1. ])
    d = {region: np.histogram(GII.loc[GII.continent == region, 'value'], bins = b)[0]
         for region in GII.continent.unique()
         }

    return pd.DataFrame({**d, **{'bins': b[:-1]}}).melt(id_vars='bins')
