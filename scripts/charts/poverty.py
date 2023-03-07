"""Poverty charts"""

import pandas as pd

from scripts.config import PATHS

POVERTY = pd.read_csv(f'{PATHS.raw_data}/unwomen_pardee_poverty.csv')


def chart_poverty_change_line() -> None:
    """Create poverty chart showing change in poverty compared to 2019"""

    df = (POVERTY.loc[(POVERTY.variable_code == 'POVCOUNT')
                      & (POVERTY.region_name.isin([' Sub-Saharan Africa', 'World']))
                      & (POVERTY.sex == 'Female')
                      & (POVERTY.year >= 2019),
    ['region_name', 'year', 'value']]
          .reset_index(drop=True)

          )

    df = (df.merge(
        df.loc[df.year == 2019, ['region_name', 'value']].rename(columns={'value': 'value_2019'}),
        on='region_name',
        how='left'
    )
          .assign(change=lambda d: ((d.value - d.value_2019) / d.value_2019) * 100, )
          .pivot(index=['year', 'value'], columns='region_name', values='change')
          .reset_index()
          .rename(columns={' Sub-Saharan Africa': 'SSA'})
          )

    df.to_csv(f'{PATHS.output}/poverty_change_line.csv', index=False)


def chart_poverty_pictograms() -> None:
    """Create poverty pictograms

    2 charts are created:

    1. chart with poverty values per year
    2. chart with poverty values for 2019 and increase since 2019 per year

    """

    df = (POVERTY.loc[(POVERTY.region_name == 'World')
                      & (POVERTY.variable_code == 'POVCOUNT')
                      & (POVERTY.sex == 'Female'),
                      ['region_name', 'year', 'value']
                      ]
          .assign(value_formatted=lambda d: round(d.value * 1000000, 0))
          )

    # poverty number women for all years
    (df.pivot(index='region_name', columns = 'year', values='value_formatted')
     .to_csv(f'{PATHS.output}/poverty_pictogram_all_years.csv', index=False)
     )

    # get only value for 2019 not region_name
    (df.assign(value_2019 = df.loc[df.year == 2019, 'value_formatted'].values[0],
               change_2019 = lambda d: d.value_formatted - df.loc[df.year == 2019, 'value_formatted'].values[0],
               )
     .loc[:,  ['year','change_2019', 'value_2019']]
     .melt(id_vars = 'year')
     .pivot(index='variable', columns = 'year', values='value')
     .reset_index()
    .sort_values(by='variable', ascending=False)
     .to_csv(f'{PATHS.output}/poverty_pictogram_increase_2019.csv', index=False)
     )






