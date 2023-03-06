""" """

import pandas as pd
import numpy as np
import country_converter as coco
from bblocks.dataframe_tools import add

from scripts.config import PATHS
from scripts import common

GII = pd.read_csv(f'{PATHS.raw_data}/hdr_gii.csv')


def _keep_only_indicator(df: pd.DataFrame, indicator: str, col: str = 'indicator'):
    """Helper function to filter dataframe keeping only selected indicator"""

    return df.loc[df[col] == indicator].reset_index(drop=True)


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


# GII charts

def chart_gii_explorer_latest() -> None:
    """Create explorer chart for GII - map and bubble chart"""

    return (get_latest_for_countries('gii')
            .loc[:, ['iso3', 'country', 'value', 'year']]
            .assign(female_pop=lambda d: d.iso3.map(common.female_population()),
                    continent=lambda d: coco.convert(d.iso3, to='continent'),
                    )
            .pipe(add.add_income_level_column, id_column='iso3', id_type='iso3')

            # highlight categories for scrolly
            .assign(africa_highlight=lambda d: d.continent.where(d.continent == 'Africa', np.nan),
                    europe_highlight=lambda d: d.continent.where(d.continent == 'Europe', np.nan),
                    low_income_highlight=lambda d: d.income_level.where(
                        d.income_level == 'Low income', np.nan),
                    high_income_highlight=lambda d: d.income_level.where(
                        d.income_level == 'High income', np.nan),
                    low_high_income_highlight=lambda d: d.income_level.where(
                        d.income_level.isin(['Low income', 'High income']), np.nan)
                    )
            .to_csv(f'{PATHS.output}/hdr_gii_bubble_latest.csv', index=False)
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


def chart_histogram_time_series():
    """Create a curved histogram for GII by year for world and Africa"""

    world = (GII
             .pipe(_keep_only_indicator, 'gii', 'variable')
             .pipe(_histogram_chart, grouping='year')
             .melt(id_vars=['x_values', 'binned'], var_name='year', value_name='World')
             .set_index(['x_values', 'binned', 'year'])
             )

    africa = (GII
              .pipe(_keep_only_indicator, 'gii', 'variable')
              .assign(continent=lambda d: coco.convert(d.iso3, to='continent'))
              .loc[lambda d: d.continent == 'Africa']
              .pipe(_histogram_chart, grouping='year')
              .melt(id_vars=['x_values', 'binned'], var_name='year', value_name='Africa')
              .set_index(['x_values', 'binned', 'year'])
              )

    lic = (GII
           .pipe(_keep_only_indicator, 'gii', 'variable')
           .pipe(add.add_income_level_column, id_column='iso3', id_type='iso3')
           .loc[lambda d: d.income_level == 'Low income']
           .pipe(_histogram_chart, grouping='year')
           .melt(id_vars=['x_values', 'binned'], var_name='year', value_name='Low income')
           .set_index(['x_values', 'binned', 'year'])
           )

    hic = (GII
           .pipe(_keep_only_indicator, 'gii', 'variable')
           .pipe(add.add_income_level_column, id_column='iso3', id_type='iso3')
           .loc[lambda d: d.income_level == 'High income']
           .pipe(_histogram_chart, grouping='year')
           .melt(id_vars=['x_values', 'binned'], var_name='year', value_name='High income')
           .set_index(['x_values', 'binned', 'year'])
           )

    # (pd.merge(africa, world, on=['x_values', 'binned', 'year'], how='left')
    # .to_csv(f'{PATHS.output}/hdr_gii_histogram_time_series.csv', index=False)
    # )

    (pd.concat([africa, lic, hic, world], axis=1)
     .reset_index()
     .loc[lambda d: d.year.isin([1990, 2000, 2010, d.year.max()])]
     .to_csv(f'{PATHS.output}/hdr_gii_histogram_time_series.csv', index=False)
     )


#  Education charts

edu_ind = {'se_m': 'male', 'se_f': 'female'}


def chart_education_connected_dot_ssa() -> None:
    """Create a connected dot chart for GII education in SSA"""

    ssa = {'code': 'ZZJ.SSA', 'name': 'Sub-Saharan Africa'}

    (GII
     .loc[
         lambda d: (d.variable.isin(edu_ind)) & ((d.region == 'SSA') | (d.country == ssa['name']))]
     .assign(sex=lambda d: d.variable.map(edu_ind))
     .dropna(subset=['value'])
     .loc[lambda d: d.groupby(['country', 'sex', 'iso3'])['year'].idxmax()]
     .assign(country=lambda d: coco.convert(d.iso3, to='name_short', not_found=None))
     .loc[:, ['country', 'year', 'sex', 'value']]
     .replace({'country': {ssa['code']: ssa['name']}})
     .reset_index(drop=True)
     .to_csv(f'{PATHS.output}/hdr_education_connected_dot_ssa.csv', index=False)
     )


def chart_education_regions_time_series() -> None:
    """Create a time series chart for GII education in regions"""

    region_names = ['Arab States', 'East Asia and the Pacific', 'Europe and Central Asia',
                    'Latin America and the Caribbean', 'South Asia',
                    'Sub-Saharan Africa']

    return (GII
            .loc[lambda d: (d.variable.isin(edu_ind)) & (d.country.isin(region_names))]
            .assign(sex=lambda d: d.variable.map(edu_ind))
            .loc[:, ['country', 'value', 'year', 'sex']]
            .pivot(index=['country', 'year'], columns='sex', values='value')
            .reset_index()
            .rename(columns={'country': 'region'})
            .to_csv(f'{PATHS.output}/hdr_education_regions_time_series.csv', index=False)
            )


def update_hdr_charts() -> None:
    """Update all HDR charts"""

    # gii explorer for map and bubble plot
    # chart_gii_explorer()

    # gii distribution
    # chart_histogram_continents()
    # chart_histogram_income()
    chart_histogram_time_series()  # use this one

    # education
    chart_education_connected_dot_ssa()
    chart_education_regions_time_series()
