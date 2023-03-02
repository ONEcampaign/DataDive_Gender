"""Charts relating to laws and political participation"""

import pandas as pd
from scripts.logger import logger

from scripts.config import PATHS
from scripts import common

LAWS = pd.read_csv(f"{PATHS.raw_data}/world_bank_law.csv")

employment_laws = [
    "SG.GET.JOBS.EQ",
    "SG.BUS.REGT.EQ",
    "SG.OPN.BANK.EQ",
    "SG.LEG.SXHR.EM",
    "SG.PEN.SXHR.EM",
]

laws_dict = {
    "entrepreneurship_laws": [
        "SG.LAW.CRDD.GR",
        "SG.CNT.SIGN.EQ",
        "SG.BUS.REGT.EQ",
        "SG.OPN.BANK.EQ",
    ],
    "workplace_laws": [
        "SG.GET.JOBS.EQ",
        "SG.LAW.NODC.HR",
        "SG.LEG.SXHR.EM",
        "SG.PEN.SXHR.EM",
    ],
    "pay_laws": [
        "SG.LAW.EQRM.WK",
        "SG.NGT.WORK.EQ",
        "SG.DNG.WORK.DN.EQ",
        "SG.IND.WORK.EQ",
    ],
}


def make_marimekko(indicators: list) -> pd.DataFrame:
    """Create a marimekko chart for the given indicators

    Args:
        indicators: list of indicator codes to include in the chart
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

    for law_type, indicators in laws_dict.items():
        (
            make_marimekko(indicators).to_csv(
                f"{PATHS.output}/laws_{law_type}.csv", index=False
            )
        )
        logger.info(f"Created {law_type} marimekko chart")
