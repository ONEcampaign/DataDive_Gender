"""Employment Charts"""

import pandas as pd
import numpy as np
import country_converter as coco

from scripts.config import PATHS

WB_GENDER = pd.read_csv(f'{PATHS.raw_data}/world_bank_gender.csv')


def unpaid_work_chart():
    """ """

    mapping = {'SG.TIM.UWRK.MA': 'male', 'SG.TIM.UWRK.FE': 'female'}

    df = (WB_GENDER
          .loc[lambda d: d.indicator_code.isin(mapping)]
          .dropna(subset=['value'])
          .assign(year=lambda d: pd.to_datetime(d["date"]).dt.year)
          .loc[lambda d: d.groupby(['indicator_code', 'iso_code'])['year'].idxmax()]
          .assign(country=lambda d: coco.convert(d.iso_code, to='name_short'),
                  sex=lambda d: d.indicator_code.map(mapping)
                  )
          )

    return df


