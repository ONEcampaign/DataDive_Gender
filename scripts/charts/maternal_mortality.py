"""create maternal mortality charts"""

import pandas as pd

from scripts.config import PATHS
from scripts.logger import logger

COUNTRIES = pd.read_csv(f'{PATHS.raw_data}/mmr2020_country_estimates.csv')
REGIONS = pd.read_csv(f'{PATHS.raw_data}/mmr2020_region_estimates.csv')


def chart_pictogram_world() -> None:
    """Create picotgram of total maternal deaths for world in 2020"""

    (REGIONS.loc[lambda d: (d.parameter == 'maternal_deaths_summation_of_country_estimates')
                       & (d.region == 'world')
                       & (d.year == 2020),
    ['region', 'year', 'value']]
     .assign(value = lambda d: d.value.round(0),
             region = lambda d: d.region.map({'world': 'World'}))
     .to_csv(f'{PATHS.output}/mmr_pictogram_world.csv', index=False)
     )


def chart_pictogram_SSA_rest_of_world() -> None:
    """Create pictogram of total maternal deaths for SSA and rest of the world in 2020"""

    (REGIONS.loc[lambda d: (d.parameter == 'maternal_deaths_summation_of_country_estimates')
                              & (d.region.isin(['world', 'Sub-Saharan Africa']))
                              & (d.year == 2020)
            ]
            .pivot(index='year', columns='region', values='value')
            .assign(diff=lambda d: d['world'] - d['Sub-Saharan Africa'])
            .rename(columns={'diff': 'Rest of the world'})
            .reset_index()
            .loc[:, ['year', 'Sub-Saharan Africa', 'Rest of the world']]
            .melt(id_vars='year')
            .assign(value = lambda d: d.value.round(0))
            .to_csv(f'{PATHS.output}/mmr_pictogram_SSA_rest_of_world.csv', index=False)
            )


def chart_pictogram_low_middle_income() -> None:
    """Create pictogram of total maternal deaths for low and lower middle
    income countries and rest of the world in 2020"""

    (REGIONS.loc[lambda d: (d.parameter == 'maternal_deaths_summation_of_country_estimates')
                              & (d.region.isin(['world', 'Low income', 'Lower middle income']))
                              & (d.year == 2020)]
            .pivot(index='year', columns='region', values='value')
            .assign(low=lambda d: d['Low income'] + d['Lower middle income'],
                    diff=lambda d: d['world'] - d['low'])
            .rename(columns={'diff': 'Rest of the world',
                             'low': 'Low and Lower middle income'})
            .reset_index()
            .loc[:, ['year', 'Low and Lower middle income', 'Rest of the world']]
            .melt(id_vars='year')
    .assign(value = lambda d: d.value.round(0))
            .to_csv(f'{PATHS.output}/mmr_pictogram_low_middle_income.csv', index=False)
            )


def calculate_pct_change(
        df: pd.DataFrame, year: int, group_col: str, val_col: str
        ) -> pd.DataFrame:
    """Calculate the percentage decrease in maternal mortality compared to a given year
    for a given group

    Args:
        df: DataFrame with columns 'year', 'country', 'value'
        year: year to compare to
        group_col: column to group by
        val_col: column to calculate percentage decrease for

    Returns:
        DataFrame with a column 'decrease' with the percentage decrease
    """

    year_val_dict = df[lambda d: (d.year == year)].set_index(group_col)[val_col].to_dict()
    return (df.assign(val_year=lambda d: d[group_col].map(year_val_dict),
                      decrease=lambda d: ((d[val_col] - d.val_year) / d.val_year) * 100
                      )
            .drop(columns='val_year')
            )


def chart_line_change_in_mmr() -> None:
    """Create a line chart showing the percentage decrease in maternal mortality"""

    country_list = ['Greece',
                    #'Venezuela',
                    #'Belize',
                    'United States',
                    #'Brazil',
                    'Ukraine',
                    'Russia',
                    'United Kingdom',
                    'Portugal',
                    #'South Korea',
                    ]
    region_list = [
        # 'Eastern and South-Eastern Asia',
        # 'Europe and Northern America',
        'Latin America and the Caribbean',
        #'Sub-Saharan Africa',
    'world']

    countries_df = (COUNTRIES.loc[lambda d: (d.parameter == 'mmr')
                                            & (d.country.isin(country_list)),
    ['country', 'year', 'value', 'lower', 'upper']]
                    .pipe(calculate_pct_change, 2000, 'country', 'value')
                    .pivot(index='year', columns='country', values='decrease')
                    .reset_index()
                    )

    df_regions = (REGIONS.loc[lambda d: (d.parameter == 'mmr')
                                        & (d.region.isin(region_list)),

    ['region', 'year', 'value', 'lower', 'upper']]
                  .pipe(calculate_pct_change, 2000, 'region', 'value')
                  .pivot(index='year', columns='region', values='decrease')
                  .reset_index()
                  .rename(columns={'Latin America and the Caribbean': 'Latin America'})
                  )

    (pd.merge(countries_df, df_regions, on='year', how='outer')
     .to_csv(f'{PATHS.output}/mmr_line_change_in_mmr.csv', index=False)
     )
    logger.debug('Update mmr_line_change_in_mmr')

