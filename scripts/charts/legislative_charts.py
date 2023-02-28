"""Charts relating to laws and political participation"""

import pandas as pd
import numpy as np
import country_converter as coco

from scripts.config import PATHS
from scripts import common

LAWS = pd.read_csv(f'{PATHS.raw_data}/world_bank_law.csv')

employment_laws = ['SG.GET.JOBS.EQ', "SG.BUS.REGT.EQ",
                   "SG.OPN.BANK.EQ",
                   "SG.LEG.SXHR.EM",
                   "SG.PEN.SXHR.EM"]


def get_law_data() -> pd.DataFrame:
    """Get law data"""
    value_dict = {0: -1, 1: 1}
    name_dict = {-1: 'no', 1: 'yes'}

    return (LAWS
            .loc[lambda d: d.indicator_code.isin(employment_laws)]  # only keep employment laws
            .assign(year=lambda d: pd.to_datetime(d['date']).dt.year,
                    value=lambda d: d.value.map(value_dict),
                    name=lambda d: d.value.map(name_dict)
                    )
            .pipe(common.latest_value, 'value', ['iso_code', 'indicator_code'], 'year')
            .pivot(index=['indicator_name', 'entity_name'], columns='name', values='value')
            .reset_index()
            )


def chart_marimekko() -> pd.DataFrame:
    """ """

    value_dict = {0: -1, 1: 1}
    name_dict = {-1: 'no', 1: 'yes'}

    df = (LAWS
            .loc[lambda d: d.indicator_code.isin(employment_laws)]  # only keep employment laws
            .assign(year=lambda d: pd.to_datetime(d['date']).dt.year,
                    value=lambda d: d.value.map(value_dict),
                    name=lambda d: d.value.map(name_dict)
                    )
            .pipe(common.latest_value, 'value', ['iso_code', 'indicator_code'], 'year')
            .assign(female_pop = lambda d: d.iso_code.map(common.female_population()))
            )

    total_population = df.groupby('indicator_code')['female_pop'].sum().unique()[0]
    df = (df.assign(female_pop_pct = lambda d: (d.female_pop / total_population) * 100))

    df.loc[df.female_pop_pct < 1, 'female_pop_pct'] = 1

    return df

