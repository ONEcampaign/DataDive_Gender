""" """

from bblocks.import_tools.world_bank import WorldBankData
from bblocks.cleaning_tools import clean
import pandas as pd
import numpy as np

from scripts.config import PATHS

WB_REGION_INCOME_CODES = {'WLD': 'World',

                          # income groups
                          'LIC': 'Low income',
                          'LMC': 'Lower middle income',
                          'UMC': 'Upper middle income',
                          'HIC': 'High income',

                          # regions
                          'SSF': 'Sub Saharan Africa',
                          'SAS': 'South Asia',
                          'NAC': 'North America',
                          'MEA': 'Middle-East and North Africa',
                          'LCN': 'Latin America and the Caribbean',
                          'ECS': 'Europe and Central Asia',
                          'EAS': 'East Asia and Pacific'
                          }

gender_indicators = {
    'SG.GEN.PARL.ZS': 'Proportion of seats held by women in national parliaments (%)',
    'HD.HCI.EYRS.FE': 'Expected Years of School, Female',
    'SP.ADO.TFRT': 'Adolescent fertility rate (births per 1,000 women ages 15-19)',
    'SH.STA.FGMS.ZS': 'Female genital mutilation prevalence (%)',
    'SG.VAW.1549.ZS': 'Proportion of women subjected to physical and/or sexual violence in the last 12 months (% of ever-partnered women ages 15-49)',
    'SG.VAW.AFSX.ZS': 'Proportion of women who have ever experienced any form of sexual violence (% of women ages 15-49)'
}

wdi_indicators = {
    'SP.POP.TOTL.FE.IN': 'Female population',
    'SP.POP.TOTL': 'Population total',
    'SH.STA.MMRT': 'Maternal mortality',
    'SP.DYN.TFRT.IN': 'Fertility rate'
}


law_indicators = {
    # entrepreneurship
    'SG.LAW.CRDD.GR': 'Does the law prohibit discrimination in access to credit based on gender?',
    'SG.CNT.SIGN.EQ': 'Can a woman sign a contract in the same way as a man?',
    'SG.BUS.REGT.EQ': 'Can a woman register a business in the same way as a man?',
    'SG.OPN.BANK.EQ': 'Can a woman open a bank account in the same way as a man?',

    # workplace
    'SG.GET.JOBS.EQ': 'Can a woman get a job in the same way as a man?',
    "SG.LAW.NODC.HR": 'Does the law prohibit discrimination in employment based on gender?',
    "SG.LEG.SXHR.EM": 'Is there legislation on sexual harassment in employment?',
    "SG.PEN.SXHR.EM": 'Are there criminal penalties or civil remedies for sexual harassment in employment?',

    # pay
    'SG.LAW.EQRM.WK': 'Does the law mandate equal remuneration for work of equal value?',
    'SG.NGT.WORK.EQ': 'Can a woman work at night in the same way as a man?',
    'SG.DNG.WORK.DN.EQ': 'Can a woman work in a job deemed dangerous in the same way as a man?',
    'SG.IND.WORK.EQ': 'Can a woman work in an industrial job in the same way as a man?',

    # WBL index
    "SG.LAW.INDX": 'Women Business and the Law Index Score',
    "SG.LAW.INDX.WP": "Women, Business and the Law: Workplace Indicator Score",
    "SG.LAW.INDX.PY":"Women, Business and the Law: Pay Indicator Score",
    "SG.LAW.INDX.EN": "Women, Business and the Law: Entrepreneurship Indicator Score"
}


def get_data(indicators: dict, db: int) -> pd.DataFrame:
    """Retrieve world Bank Data"""

    wb = WorldBankData()
    wb.load_data(list(indicators.keys()), db=db)
    return (wb.get_data()
            .assign(indicator_name=lambda d: d.indicator_code.map(indicators),
                    entity_name=lambda d: clean.convert_id(d.iso_code,
                                                           from_type='ISO3',
                                                           to_type='name_short',
                                                           not_found=np.nan,
                                                           additional_mapping=WB_REGION_INCOME_CODES
                                                           )
                    )
            .dropna(subset='entity_name')
            .reset_index(drop=True)
            )


def download_wb_data() -> None:
    """Download World Bank Data"""

    # gender data
    (get_data(gender_indicators, db=14)
     .to_csv(f'{PATHS.raw_data}/world_bank_gender.csv', index=False))

    # wdi data
    (get_data(wdi_indicators, db=2)
     .to_csv(f'{PATHS.raw_data}/world_bank_wdi.csv', index=False))

    # law data
    (get_data(law_indicators, db=14)
     .to_csv(f'{PATHS.raw_data}/world_bank_law.csv', index=False))
