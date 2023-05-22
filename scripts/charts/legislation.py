"""Charts relating to laws and political participation"""

import pandas as pd
from scripts.logger import logger
import country_converter as coco
import numpy as np
from bblocks.dataframe_tools import add
import pyreadstat

from scripts.config import PATHS
from scripts import common

LAWS = pd.read_csv(f"{PATHS.raw_data}/world_bank_law.csv")
WB_GENDER = pd.read_csv(f"{PATHS.raw_data}/world_bank_gender.csv")

afrobarometer_data, afrobarometer_meta = pyreadstat.read_sav(
    f"{PATHS.raw_data}/afrobarometer.sav"
)

laws = [
    "SG.LAW.EQRM.WK",
    "SG.LAW.NODC.HR",
    "SG.GET.JOBS.EQ",
    "SG.CNT.SIGN.EQ",
    "SG.PEN.SXHR.EM",
]


def make_marimekko(indicators: list) -> pd.DataFrame:
    """Create a marimekko chart for the given indicators

    Args:
        indicators: list of indicator codes to include in the chart

    Returns:
        df: dataframe with the data for the chart
    """

    value_dict = {0: -1, 1: 1}
    name_dict = {-1: "no", 1: "yes"}

    df = (
        LAWS.loc[lambda d: d.indicator_code.isin(indicators)]
        .dropna(subset="value")
        .assign(
            year=lambda d: pd.to_datetime(d["date"]).dt.year,
            value=lambda d: d.value.map(value_dict),
            value_label=lambda d: d.value.map(name_dict),
        )
        .pipe(common.latest_value, "value", ["iso_code", "indicator_code"], "year")
        .assign(female_pop=lambda d: d.iso_code.map(common.female_population()))
        .loc[
            :,
            [
                "indicator_name",
                "entity_name",
                "year",
                "value_label",
                "value",
                "female_pop",
            ],
        ]
    )

    # create bar width
    total_population = df.groupby("indicator_name")["female_pop"].sum().unique()[0]
    df = df.assign(width=lambda d: (d.female_pop / total_population) * 100)
    df.loc[df.width < 1, "width"] = 1  # make sure all bars are visible

    # convert raw population values to millions, sort by population
    df = df.sort_values(by="female_pop", ascending=False).assign(
        female_pop_annot=lambda d: (d.female_pop / 1000000)
    )

    return df


def chart_laws_marimekko() -> None:
    """Create a marimekko chart of the laws used in the WBL index
    and save to csv
    """

    (make_marimekko(laws).to_csv(f"{PATHS.output}/laws_marimekko.csv", index=False))
    logger.debug(f"Update chart laws_marimekko")


def chart_parliament_participation_beeswarm() -> None:
    """Create a beeswarm chart showing women's participation in parliament"""

    (
        WB_GENDER[lambda d: d.indicator_code == "SG.GEN.PARL.ZS"]
        .assign(year=lambda d: pd.to_datetime(d.date).dt.year)
        .dropna(subset="value")
        .pipe(common.latest_value, "value", "iso_code", "year")
        .pipe(common.only_countries, "iso_code")
        .assign(
            continent=lambda d: coco.convert(
                d.iso_code, to="continent", not_found=np.nan
            ),
            # female_pop=lambda d: d.iso_code.map(common.female_population()),
            # gdppc = lambda d: d.iso_code.map(common.gdp_per_capita()),
        )
        .pipe(add.add_income_level_column, id_column="iso_code", id_type="iso3")
        .loc[:, ["continent", "value", "income_level", "entity_name", "year"]]
        .to_csv(f"{PATHS.output}/parliament_participation_beeswarm.csv", index=False)
    )
    logger.debug("update chart parliament_participation_beeswarm")


def chart_afrobarometer_sentiment() -> None:
    """Create a chart showing sentiment towards gender equality in politics
    from Afrobarometer data"""

    (
        afrobarometer_data[["COUNTRY", "Q101", "Q16"]]
        .assign(
            gender=lambda d: d.Q101.map(
                afrobarometer_meta.value_labels[
                    afrobarometer_meta.variable_to_label["Q101"]
                ]
            ),
            country=lambda d: d.COUNTRY.map(
                afrobarometer_meta.value_labels[
                    afrobarometer_meta.variable_to_label["COUNTRY"]
                ]
            ),
            response=lambda d: d.Q16.map(
                afrobarometer_meta.value_labels[
                    afrobarometer_meta.variable_to_label["Q16"]
                ]
            ),
        )
        .loc[
            lambda d: d.response.isin(
                [
                    "Agree very strongly with 2",
                    "Agree with 2",
                    "Agree very strongly with 1",
                    "Agree with 1",
                ]
            ),
            ["country", "gender", "response"],
        ]
        # aggregate and get totals
        .groupby(["country", "gender", "response"])
        .size()
        .reset_index(name="response_count")
        # pivot for gender
        .pivot(index=["country", "response"], columns="gender", values="response_count")
        .fillna(0)
        .assign(all=lambda d: d.sum(axis=1))
        .drop(columns="Missing")  # remove values from missing gender var
        # calculate proportions
        .assign(
            female=lambda d: (d.Female / d.groupby(level=0)["Female"].transform("sum"))
            * 100,
            male=lambda d: (d.Male / d.groupby(level=0)["Male"].transform("sum")) * 100,
            total=lambda d: (d["all"] / d.groupby(level=0)["all"].transform("sum"))
            * 100,
        )
        .loc[:, ["female", "male", "total"]]
        .rename(columns={"total": "all"})
        .reset_index()
        # melt and pivot so that responses are columns
        .melt(id_vars=["country", "response"], value_name="proportion")
        .pivot(index=["country", "gender"], columns="response", values="proportion")
        .reset_index()
        # make values negative for disagree responses
        .assign(
            **{
                "Agree very strongly with 1": lambda d: d["Agree very strongly with 1"]
                * -1,
                "Agree with 1": lambda d: d["Agree with 1"] * -1,
            }
        )
        # rename responses
        .rename(
            columns={
                "Agree very strongly with 1": "strongly disagree",
                "Agree very strongly with 2": "strongly agree",
                "Agree with 1": "disagree",
                "Agree with 2": "agree",
            }
        )
        .to_csv(f"{PATHS.output}/afrobarometer_sentiment.csv", index=False)
    )

    logger.debug("update chart afrobarometer_sentiment")
