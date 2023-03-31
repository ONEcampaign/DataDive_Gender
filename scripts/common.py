"""Common functions"""

import pandas as pd
import country_converter as coco
from bblocks.import_tools.world_bank import WorldBankData

from scripts.config import PATHS

ISO_COUNTRY_DICT = (
    coco.CountryConverter()
    .data[["ISO3", "name_short"]]
    .set_index("ISO3")["name_short"]
    .to_dict()
)


def only_countries(df: pd.DataFrame, iso_col: str = "iso_code") -> pd.DataFrame:
    """Filter a dataframe to keep only countries using a column of iso3 codes"""

    return df.loc[lambda d: d[iso_col].isin(ISO_COUNTRY_DICT)].reset_index(drop=True)


def latest_value(
    df: pd.DataFrame, value_col: str, grouping_col: str | list, date_col
) -> pd.DataFrame:
    """Return the latest non null value for each group in a dataframe"""

    return (
        df.dropna(subset=[value_col])
        .loc[lambda d: d.groupby(grouping_col)[date_col].idxmax()]
        .reset_index(drop=True)
    )


def female_population() -> dict:
    """Return latest values for female population for each country/region"""

    return (
        pd.read_csv(f"{PATHS.raw_data}/world_bank_wdi.csv")
        .loc[lambda d: d.indicator_code == "SP.POP.TOTL.FE.IN"]
        .assign(year=lambda d: pd.to_datetime(d["date"]).dt.year)
        .pipe(latest_value, "value", "iso_code", "year")
        .set_index("iso_code")["value"]
        .to_dict()
    )


def gdp_per_capita() -> dict:
    """Return the latest values for gdp per capita for each country/region"""

    # get gdp per capita data
    gdp = WorldBankData().load_data("NY.GDP.PCAP.CD").get_data()

    # get dictionary of latest values
    return (
        gdp.assign(year=lambda d: pd.to_datetime(d["date"]).dt.year)
        .dropna(subset="value")
        .pipe(latest_value, "value", "iso_code", "year")
        .set_index("iso_code")["value"]
        .to_dict()
    )
