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
