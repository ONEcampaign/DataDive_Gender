"""Common functions"""

import pandas as pd
import numpy as np
import country_converter as coco

ISO_COUNTRY_DICT = (coco.CountryConverter()
                    .data[['ISO3', 'name_short']]
                    .set_index("ISO3")['name_short']
                    .to_dict())


def only_countries(df: pd.DataFrame, iso_col: str = "iso_code"):
    """Filter a dataframe to keep only countries using a column of iso3 codes"""

    return (df
            .loc[lambda d: d[iso_col].isin(ISO_COUNTRY_DICT)]
            .reset_index(drop=True)
            )
