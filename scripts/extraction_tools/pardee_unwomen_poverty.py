"""Script to extract and clean data from the Pardee Center for International Futures
and UN Women's report on the impact of COVID-19 on women's poverty.
link: https://data.unwomen.org/features/poverty-deepens-women-and-girls-according-latest-projections
"""

import pandas as pd
import country_converter as coco

from scripts.config import PATHS
from scripts.logger import logger

URL = "https://data.unwomen.org/sites/default/files/inline-files/Poverty-Estimates_new-release_210122.xlsx"

SHEETS = {
    "variables": "Variable key and read me",
    "regional_data": "Regional Data",
    "country_data": "Country level data",
}


def get_mapper(df: pd.DataFrame) -> dict:
    """Get a mapper from the variables sheet to map variable names and units

    Args:
        df (pd.DataFrame): dataframe to get mapper from

    Returns:
        dictionary with keys as 'variable_mapper' and 'units_mapper',
        and mappers using variable codes as values
    """

    df = (
        df.dropna(subset="Description")
        .rename(
            columns={
                "Variable": "variable_code",
                "Description": "variable_name",
                "Unit": "units",
            }
        )
        .set_index("variable_code")
    )

    return {
        "variable_name": df.loc[:, "variable_name"].to_dict(),
        "units": df.loc[:, "units"].to_dict(),
    }


def clean_country_data(df: pd.DataFrame, mapper: dict) -> pd.DataFrame:
    """Clean country data dataframe

    Args:
        df (pd.DataFrame): dataframe to clean
        mapper (dict): mapper to map variable names and units

    Returns:
        clean dataframe with melted to create a year column
        and a new column for region_name using country_converter
    """

    cols = {"Sex": "sex", "Variable": "variable_code", "ISO Code": "iso_code"}

    return (
        df.drop(columns=["Age", "Scenario"])
        .rename(columns=cols)
        .assign(
            variable_name=lambda d: d.variable_code.map(mapper["variable_name"]),
            units=lambda d: d.variable_code.map(mapper["units"]),
        )
        .assign(region_name=lambda d: coco.convert(d.iso_code, to="name_short"))
        .melt(
            id_vars=list(cols.values()) + ["variable_name", "units", "region_name"],
            var_name="year",
        )
    )


def clean_region_data(df: pd.DataFrame, mapper: dict) -> pd.DataFrame
    """Clean regional data dataframe
    
    Args:
        df (pd.DataFrame): dataframe to clean
        mapper (dict): mapper to map variable names and units
    
    Returns:
        clean dataframe with melted to create a year column
    """

    cols = {"Sex": "sex", "Variable": "variable_code", "Region": "region_name"}

    return (
        df.drop(columns=["Age", "Scenario"])
        .rename(columns=cols)
        .assign(
            variable_name=lambda d: d.variable_code.map(mapper["variable_name"]),
            units=lambda d: d.variable_code.map(mapper["units"]),
        )
        .melt(id_vars=list(cols.values()) + ["variable_name", "units"], var_name="year")
    )


def update_unwomen_pardee_poverty() -> None:
    """Pipeline to update UNwomen, Pardee poverty data

    Stored as a csv in raw_data/unwomen_pardee_poverty.csv
    """

    mapper = pd.read_excel(URL, sheet_name=SHEETS["variables"]).pipe(get_mapper)
    df_region = pd.read_excel(URL, sheet_name=SHEETS["regional_data"])
    df_country = pd.read_excel(URL, sheet_name=SHEETS["country_data"])

    (
        pd.concat(
            [
                clean_region_data(df_region, mapper),
                clean_country_data(df_country, mapper),
            ],
            ignore_index=True,
        ).to_csv(PATHS.raw_data / "unwomen_pardee_poverty.csv", index=False)
    )
    logger.info("Updated unwomen_pardee_poverty.csv")
