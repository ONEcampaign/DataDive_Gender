"""Main extraction script."""

import pandas as pd
from bblocks.import_tools import hdr
from unesco_reader import uis
from bblocks.import_tools.unzip import read_zipped_csv
from bblocks.import_tools import ilo
from bblocks.import_tools import world_bank
from bblocks.cleaning_tools import clean
import country_converter as coco
import numpy as np

from scripts.config import PATHS
from scripts.logger import logger


# Extract data from UNDP HDR


def hdr_gii() -> None:
    """Extract data from HDR GII and save to csv called hdr_gii.csv"""

    (
        hdr.HDR()
        .load_data(hdr.available_indicators("gii"))
        .get_data(hdr.available_indicators("gii"))
        .to_csv(f"{PATHS.raw_data}/hdr_gii.csv", index=False)
    )
    logger.debug("Extracted GII data from HDR")


def hdr_gdi() -> None:
    """Extract data from HDR GDI and save to csv called hdr_gdi.csv"""

    (
        hdr.HDR()
        .load_data(hdr.available_indicators("gdi"))
        .get_data(hdr.available_indicators("gdi"))
        .to_csv(f"{PATHS.raw_data}/hdr_gdi.csv", index=False)
    )
    logger.debug("Extracted GDI data from HDR")


# Extract data from World Bank

WB_REGION_INCOME_CODES = {
    "WLD": "World",
    # income groups
    "LIC": "Low income",
    "LMC": "Lower middle income",
    "UMC": "Upper middle income",
    "HIC": "High income",
    # regions
    "SSF": "Sub Saharan Africa",
    "SAS": "South Asia",
    "NAC": "North America",
    "MEA": "Middle-East and North Africa",
    "LCN": "Latin America and the Caribbean",
    "ECS": "Europe and Central Asia",
    "EAS": "East Asia and Pacific",
}


def _extract_wb_data(indicators: dict, db: int) -> pd.DataFrame:
    """Retrieve world Bank Data"""

    wb = world_bank.WorldBankData()
    wb.load_data(list(indicators.keys()), db=db)
    return (
        wb.get_data()
        .assign(
            indicator_name=lambda d: d.indicator_code.map(indicators),
            entity_name=lambda d: clean.convert_id(
                d.iso_code,
                from_type="ISO3",
                to_type="name_short",
                not_found=np.nan,
                additional_mapping=WB_REGION_INCOME_CODES,
            ),
        )
        .dropna(subset="entity_name")
        .reset_index(drop=True)
    )


def wb_wdi() -> None:
    """Extract data from World Bank WDI and save to csv called world_bank_wdi.csv"""

    indicators = {
        "SP.POP.TOTL.FE.IN": "Female population",
        "SP.POP.TOTL": "Population total",
        "SH.STA.MMRT": "Maternal mortality",
        "SP.DYN.TFRT.IN": "Fertility rate",
    }

    (
        _extract_wb_data(indicators, db=2).to_csv(
            f"{PATHS.raw_data}/world_bank_wdi.csv", index=False
        )
    )
    logger.debug("Extracted WDI data from World Bank")


def wb_gender() -> None:
    """Extract gender data from World Bank Gender Data Portal
    and save to csv called world_bank_wdi.csv
    """

    indicators = {
        "SG.GEN.PARL.ZS": "Proportion of seats held by women in national parliaments (%)",
        "HD.HCI.EYRS.FE": "Expected Years of School, Female",
        "SP.ADO.TFRT": "Adolescent fertility rate (births per 1,000 women ages 15-19)",
        "SH.STA.FGMS.ZS": "Female genital mutilation prevalence (%)",
        "SG.VAW.1549.ZS": "Proportion of women subjected to physical and/or sexual violence in the last 12 months (% of ever-partnered women ages 15-49)",
        "SG.VAW.AFSX.ZS": "Proportion of women who have ever experienced any form of sexual violence (% of women ages 15-49)",
        "SG.TIM.UWRK.MA": "Proportion of time spent on unpaid domestic and care work, female (% of 24 hour day)- males",
        "SG.TIM.UWRK.FE": "Proportion of time spent on unpaid domestic and care work, female (% of 24 hour day) - females",
        "SL.TLF.CACT.FE.ZS": "Labor force participation rate, female (% of female population ages 15+) (modeled ILO estimate)",
        "SL.TLF.CACT.MA.ZS": "Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate)",
        "SE.ENR.PRSC.FM.ZS": "School enrollment, primary and secondary (gross), gender parity index (GPI)",
    }

    (
        _extract_wb_data(indicators, db=14).to_csv(
            f"{PATHS.raw_data}/world_bank_gender.csv", index=False
        )
    )
    logger.debug("Extracted gender data from World Bank")


def wb_law() -> None:
    """Extract law data from World Bank Gender Data Portal
    and save to csv called world_bank_wdi.csv
    """

    indicators = {
        "SG.LAW.EQRM.WK": "Does the law require equal pay for equal work?",
        "SG.LAW.NODC.HR": "Does the law prohibit gender discrimination in the workplace?",
        "SG.GET.JOBS.EQ": "Can a woman get a job in the same way as a man?",
        "SG.CNT.SIGN.EQ": "Can a woman sign a contract in the same way as a man?",
        "SG.PEN.SXHR.EM": "Are there criminal penalties for sexual harassment in in the workplace?",
    }

    (
        _extract_wb_data(indicators, db=14).to_csv(
            f"{PATHS.raw_data}/world_bank_law.csv", index=False
        )
    )
    logger.debug("Extracted law data from World Bank")


# Extract UIS data
def uis_sdg() -> None:
    """Extract data from UIS and save to csv called uis.csv"""

    indicators = {  # Out-of-school rate for children of primary school age
        "ROFST.1.CP": "Out-of-school rate for children of primary school age, both sexes (%)",
        "ROFST.1.F.CP": "Out-of-school rate for children of primary school age, female (%)",
        "ROFST.1.GPIA.CP": "Out-of-school rate for children of primary school age, adjusted gender parity index (GPIA)",
        "ROFST.1.M.CP": "Out-of-school rate for children of primary school age, male (%)",
        # Out-of-school rate for children of primary and lower secondary school age
        "ROFST.1T2.CP": "Out-of-school rate for children and adolescents of primary and lower secondary school age, both sexes (%)",
        "ROFST.1T2.F.CP": "Out-of-school rate for children and adolescents of primary and lower secondary school age, female (%)",
        "ROFST.1T2.GPIA.CP": "Out-of-school rate for children and adolescents of primary and lower secondary school age, adjusted gender parity index (GPIA)",
        "ROFST.1T2.M.CP": "Out-of-school rate for children and adolescents of primary and lower secondary school age, male (%)",
        # Educational attainment rate, completed primary education or higher
        " EA.1T8.AG25T99": "Educational attainment rate, completed primary education or higher, population 25+ years, both sexes (%)",
        "EA.1T8.AG25T99.F": "Educational attainment rate, completed primary education or higher, population 25+ years, female (%)",
        "EA.1T8.AG25T99.GPIA": "Educational attainment rate, completed primary education or higher, population 25+ years, adjusted gender parity index (GPIA)",
        "EA.1T8.AG25T99.M": "Educational attainment rate, completed primary education or higher, population 25+ years, male (%)",
    }

    (
        uis.UIS("SDG")
        .load_data()
        .get_data()
        .loc[lambda d: d["INDICATOR_ID"].isin(indicators)]
        .to_csv(PATHS.raw_data / "uis.csv", index=False)
    )
    logger.debug("Extracted data from UIS")


# Extract mmr2020

MMR_URL = "https://mmr2020.srhr.org/download_mmr_estimates.zip"


def _mmr2020_country_estimates() -> pd.DataFrame:
    """Exract and clean country level estimates from mmr2020

    Returns:
        pd.DataFrame -- country level estimates
    """

    path = "download_mmr_estimates/country_level_pub/estimates_country_level.csv"
    df = read_zipped_csv(MMR_URL, path)

    # clean
    df = (
        df.rename(
            columns={
                "iso_alpha_3_code": "iso_code",
                "X0.5": "value",
                "year_mid": "year",
                "X0.9": "upper",
                "X0.1": "lower",
            }
        )
        .drop(columns="estimate_version")
        .assign(
            country=lambda d: coco.convert(d.iso_code, to="name_short"),
            continent=lambda d: coco.convert(d.iso_code, to="continent"),
        )
    )

    return df


def _mmr2020_region_estimates(region_file: str) -> pd.DataFrame:
    """Extract and cleandata for specific region file

    Args:
        region_file (str): name of region file

    Returns:
        pd.DataFrame -- region level estimates

    """

    df = read_zipped_csv(MMR_URL, region_file)

    # clean
    df = df.rename(
        columns={
            "group": "region",
            "year_mid": "year",
            "X0.1": "lower",
            "X0.9": "upper",
            "X0.5": "value",
        }
    ).drop(columns="estimate_version")

    return df


def mmr2020() -> None:
    """Extract data from MMR2020 for country and region estimates
    save country estimates to mmr2020_country_estimates.csv
    save region estimates to mmr2020_region_estimates.csv
    """

    _mmr2020_country_estimates().to_csv(
        PATHS.raw_data / "mmr2020_country_estimates.csv", index=False
    )
    logger.debug("Extracted country estimates from MMR2020")

    (
        pd.concat(
            [
                _mmr2020_region_estimates(
                    "download_mmr_estimates/aggregates_pub/estimates_World.csv"
                ),
                # world
                _mmr2020_region_estimates(
                    "download_mmr_estimates/aggregates_pub/estimates_sdg_region.csv"
                ),
                # regions
                _mmr2020_region_estimates(
                    "download_mmr_estimates/aggregates_pub/estimates_World_Bank_Income.csv"
                ),  # sub regions
            ]
        ).to_csv(PATHS.raw_data / "mmr2020_region_estimates.csv", index=False)
    )
    logger.debug("Extracted region estimates from MMR2020")


# Extract ILO data
def ilo_employment() -> None:
    """Extract employment data from ILO and save to ilo.csv"""

    indicators = {
        "EAR_GGAP_OCU_RT_A": "Gender wage gap by occupation (%)",
        "LAP_2FTM_NOC_RT_A": "Gender income gap, ratio of women's to men's labour income",
        "SDG_T552_NOC_RT_A": "Proportion of women in managerial positions (%)",
        "SDG_0831_SEX_ECO_RT_A": "Proportion of informal employment in total employment by sex and sector (%)",
        "EMP_TEMP_SEX_AGE_OCU_NB_A": "Employment by sex, age and occupation (thousands)",
        "EAP_DWAP_SEX_AGE_RT_A": "Labour force participation rate by sex and age (%)",
    }

    (
        ilo.ILO()
        .load_data(list(indicators))
        .get_data()
        .to_csv(f"{PATHS.raw_data}/ilo.csv", index=False)
    )
    logger.debug("Extracted data from ILO")


if __name__ == "__main__":
    """Update raw data from all sources"""

    hdr_gii()
    hdr_gdi()
    wb_wdi()
    wb_law()
    wb_gender()
    mmr2020()
    uis_sdg()
    # ilo_employment()

    logger.debug("Successfully extracted data from all sources to raw_data folder")
