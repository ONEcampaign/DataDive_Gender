""" """

import pandas as pd
import country_converter as coco
from bblocks.dataframe_tools import add

from scripts.config import PATHS

GII = pd.read_csv(f'{PATHS.raw_data}/hdr_gii.csv')


def _keep_only_indicator(df: pd.DataFrame, indicator: str, col: str = 'indicator'):
    """Helper function to filter dataframe keeping only selected indicator"""

    return df.loc[df[col] == indicator].reset_index(drop=True)


def gii_regions_timeseries_chart() -> pd.DataFrame:
    """Chart showing GII for regions over time"""

    regions = ['Arab States', 'Europe and Central Asia',
               'Latin America and the Caribbean', 'South Asia', 'World',
               'East Asia and the Pacific', 'Sub-Saharan Africa']

    return (GII.pipe(_keep_only_indicator, 'gii', 'variable')
            .loc[lambda d: d.country.isin(regions)]
            .pivot(index='year', columns='country', values='value')
            .reset_index()
            )


def get_latest_for_countries(variable: str) -> pd.DataFrame:
    """Get a dataframe with latest values for countries for a given variable

    Args:
        variable: variable to get latest values for

    """

    return (GII
            .pipe(_keep_only_indicator, variable, 'variable')
            .dropna(subset=['hdicode', 'value'])
            .loc[lambda d: d.groupby('country')['year'].idxmax()]
            .reset_index(drop=True)
            )


def chart_gii_explorer() -> None:
    """Create a chart for the GII explorer scrolly"""

    (get_latest_for_countries('gii')
     # add female population)
     .pipe(add.add_population_column, id_column='iso3', id_type='iso3')
     .pipe(add.add_income_level_column, id_column='iso3', id_type='iso3')
     .assign(continent=lambda d: coco.convert(d.iso3, to='continent'))
     .to_csv(f'{PATHS.output}/hdr_gii_explorer.csv', index=False)
     )


def _histogram_chart(df: pd.DataFrame, grouping: str) -> pd.DataFrame:
    """Create a curved histogram from a dataframe with 10 bins from 0-1

    Args:
        df: dataframe with values to bin
        grouping: column to group by

    Returns:
        dataframe with binned values and counts


    """

    bins = [0, 0.0001, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    labels = {'0': 0,
              '0.001-0.1': 0.05,
              '0.1-0.2': 0.15,
              '0.2-0.3': 0.25,
              '0.3-0.4': 0.35,
              '0.4-0.5': 0.45,
              '0.5-0.6': 0.55,
              '0.6-0.7': 0.65,
              '0.7-0.8': 0.75,
              '0.8-0.9': 0.85,
              '0.9-1': 0.95}

    return (df
            .assign(binned=lambda d: pd.cut(d.value, bins=bins, labels=labels.keys(),
                                            include_lowest=True))
            .groupby(['binned', grouping])
            .size()
            .reset_index(name='counts')
            .assign(x_values=lambda d: d.binned.map(labels))
            .pivot(index=['x_values', 'binned'], columns=grouping, values='counts')
            .reset_index()
            )


def chart_histogram_continents() -> None:
    """Create a curved histogram for GII by continent"""

    (get_latest_for_countries('gii')
     .assign(continent=lambda d: coco.convert(d.iso3, to='continent'))
     .pipe(_histogram_chart, grouping='continent')
     .to_csv(f'{PATHS.output}/hdr_gii_histogram_continents.csv', index=False)
     )


def chart_histogram_income() -> None:
    """Create a curved histogram for GII by income level"""

    (get_latest_for_countries('gii')
     .pipe(add.add_income_level_column, id_column='iso3', id_type='iso3')
     .pipe(_histogram_chart, grouping='income_level')
     .reindex(columns=['x_values', 'binned', 'Low income',
                       'Lower middle income', 'Upper middle income', 'High income'])
     .to_csv(f'{PATHS.output}/hdr_gii_histogram_income.csv', index=False)
     )


def update_hdr_charts() -> None:
    """Update all HDR charts"""

    pass