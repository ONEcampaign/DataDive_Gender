"""Extract data from mmr2020 https://mmr2020.srhr.org"""

import pandas as pd
from bblocks.import_tools.unzip import read_zipped_csv
import country_converter as coco

from scripts.config import PATHS


URL = 'https://mmr2020.srhr.org/download_mmr_estimates.zip'

indicators = ['mmr', 'maternal_deaths_summation_of_country_estimates']


def extract_country_estimates() -> pd.DataFrame:
    """ """

    path = 'download_mmr_estimates/country_level_pub/estimates_country_level.csv'
    df = read_zipped_csv(URL,
                         path)

    # clean
    df = (df
          .rename(columns={'iso_alpha_3_code': 'iso_code', 'X0.5': 'value', 'year_mid': 'year',
                           'X0.9': 'upper', 'X0.1': 'lower'})
          .drop(columns='estimate_version')
          .assign(country=lambda d: coco.convert(d.iso_code, to='name_short'),
                  continent=lambda d: coco.convert(d.iso_code, to='continent'),
                  )
          )

    return df


def extract_region_estimate(region_file, region_dict = None) -> pd.DataFrame:
    """Extract and cleandata for specific region file"""

    df = read_zipped_csv(URL,
                         region_file)

    # clean
    df = (df
          .rename(columns = {'group': 'region', 'year_mid': 'year', 'X0.1': 'lower',
                             'X0.9': 'upper', 'X0.5': 'value'})
          .drop(columns='estimate_version')
          )

    # convert region names
    if region_dict is not None:
        df = df.assign(region=lambda d: d.region.map(region_dict))

    return df


def update_mmr2020_data() -> None:
    """Extract country estimates and region estimates"""

    # extract country estimates
    extract_country_estimates().to_csv(PATHS.raw_data / 'mmr2020_country_estimates.csv', index=False)

    # extract region estimates
    (pd.concat([
        extract_region_estimate('download_mmr_estimates/aggregates_pub/estimates_World.csv'), # world
        extract_region_estimate('download_mmr_estimates/aggregates_pub/estimates_sdg_region.csv'), # regions
        extract_region_estimate('download_mmr_estimates/aggregates_pub/estimates_World_Bank_Income.csv'), # sub regions
    ])
     .to_csv(PATHS.raw_data / 'mmr2020_region_estimates.csv', index=False)
     )


