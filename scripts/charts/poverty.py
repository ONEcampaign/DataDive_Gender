"""Poverty charts"""

import pandas as pd

from scripts.config import PATHS
from scripts.logger import logger

POVERTY = pd.read_csv(f"{PATHS.raw_data}/unwomen_pardee_poverty.csv")


def chart_poverty_change_line() -> None:
    """Create poverty chart showing change in poverty compared to 2019"""

    df = POVERTY.loc[
        (POVERTY.variable_code == "POVCOUNT")
        & (POVERTY.region_name.isin([" Sub-Saharan Africa", "World"]))
        & (POVERTY.sex == "Female")
        & (POVERTY.year >= 2019),
        ["region_name", "year", "value"],
    ].reset_index(drop=True)

    df = (
        df.merge(
            df.loc[df.year == 2019, ["region_name", "value"]].rename(
                columns={"value": "value_2019"}
            ),
            on="region_name",
            how="left",
        )
        .assign(
            change=lambda d: ((d.value - d.value_2019) / d.value_2019) * 100,
        )
        .pivot(index=["year", "value"], columns="region_name", values="change")
        .reset_index()
        .rename(columns={" Sub-Saharan Africa": "SSA"})
        .assign(value=lambda d: d.value.round(0).astype(int))
    )

    df.to_csv(f"{PATHS.output}/poverty_change_line.csv", index=False)

    logger.debug("Update chart poverty_change_line")
