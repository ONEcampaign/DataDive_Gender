"""Employment Charts"""

import pandas as pd
import country_converter as coco
import numpy as np

from scripts.config import PATHS
from scripts.logger import logger

WB_GENDER = pd.read_csv(f"{PATHS.raw_data}/world_bank_gender.csv")


def chart_unpaid_work():
    """ """

    mapping = {"SG.TIM.UWRK.MA": "male", "SG.TIM.UWRK.FE": "female"}

    df = (
        WB_GENDER.loc[lambda d: d.indicator_code.isin(mapping)]
        .dropna(subset=["value"])
        .assign(year=lambda d: pd.to_datetime(d["date"]).dt.year)
        .loc[lambda d: d.groupby(["indicator_code", "iso_code"])["year"].idxmax()]
        .assign(
            country=lambda d: coco.convert(
                d.iso_code, to="name_short", not_found=np.nan
            ),
            sex=lambda d: d.indicator_code.map(mapping),
        )
        .dropna(subset=["country"])
        .loc[:, ["year", "country", "value", "sex"]]
        .assign(
            hours=lambda d: d.value * 24 / 100,
            value=lambda d: d.value.round(0).astype(int),
        )
        .rename(columns={"value": "percent_of_day"})
        .sort_values(by="hours", ascending=False)
    )

    df.to_csv(f"{PATHS.output}/unpaid_work.csv", index=False)
    logger.debug("Updated chart unpaid_work")


def get_labor_force(regions: list) -> pd.DataFrame:
    """Extract and reshape labor force data for a list of regions

    Args:
        regions: list of regions to extract data for
    """

    mapping = {"SL.TLF.CACT.FE.ZS": "female", "SL.TLF.CACT.MA.ZS": "male"}

    return (
        WB_GENDER.loc[
            lambda d: (d.indicator_code.isin(mapping)) & (d.iso_code.isin(regions))
        ]
        .dropna(subset="value")
        .assign(
            year=lambda d: pd.to_datetime(d.date).dt.year,
            sex=lambda d: d.indicator_code.map(mapping),
        )
        .pivot(index=["year", "entity_name"], columns="sex", values="value")
        .reset_index()
    )


def chart_labor_force_world() -> None:
    """Create a line chart of the labor force participation rate for the world"""

    (
        get_labor_force(regions=["WLD"]).to_csv(
            f"{PATHS.output}/labor_force_world.csv", index=False
        )
    )
    logger.debug(f"Created labor force world chart")


def chart_labor_force_income() -> None:
    """Create a line chart of the labor force participation rate by income level"""

    (
        get_labor_force(regions=["LIC", "LMC", "UMC", "HIC"]).to_csv(
            f"{PATHS.output}/labor_force_income.csv", index=False
        )
    )
    logger.debug(f"Created labor force income chart")
