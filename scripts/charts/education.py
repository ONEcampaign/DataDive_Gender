"""Create education charts"""

import pandas as pd
import numpy as np
import country_converter as coco

from scripts.config import PATHS
from scripts import common

EDUCATION = pd.read_csv(PATHS.raw_data / 'uis.csv')

# Educational attainment rate, completed primary education or higher
attainment_indicators = {
    'EA.1T8.AG25T99.GPIA': 'gender_parity_index'}


def chart_scatter_attainment() -> None:
    """Create scatter plot of educational attainment Gender Parity Index"""

    (EDUCATION
     .loc[lambda d: d.INDICATOR_ID.isin(attainment_indicators)]
     .assign(type=lambda d: d.INDICATOR_ID.map(attainment_indicators))
     .pivot(index=['COUNTRY_ID', 'COUNTRY_NAME', 'YEAR'], columns='type', values='VALUE')
     .reset_index()
     .pipe(common.latest_value, 'gender_parity_index', 'COUNTRY_ID', 'YEAR')
     .loc[lambda d: d.YEAR >= 2015]
     .assign(continent=lambda d: coco.convert(d.COUNTRY_ID, to='continent', not_found=np.nan),
             gdp_per_capita=lambda d: d.COUNTRY_ID.map(common.gdp_per_capita()))
     .dropna(subset=['continent', 'gdp_per_capita'])
     .to_csv(PATHS.output / 'education_attainment_scatter.csv', index=False)
     )
