"""Main extraction script."""

import pandas as pd
from bblocks.import_tools import hdr
from unesco_reader import uis

from scripts.config import PATHS
from scripts.logger import logger
from scripts.extraction_tools import world_bank


class ExtractData:
    """A class to manage extraction of data from various sources."""

    # HDR data
    def gii(self) -> None:
        """Extract data from HDR GII and save to csv called hdr_gii.csv"""

        (hdr.HDR()
         .load_data(hdr.available_indicators('gii'))
         .get_data(hdr.available_indicators('gii'))
         .to_csv(f'{PATHS.raw_data}/hdr_gii.csv', index=False)
         )
        logger.debug("Extracted GII data from HDR")

    def gdi(self) -> None:
        """Extract data from HDR GDI and save to csv called hdr_gdi.csv"""

        (hdr.HDR()
         .load_data(hdr.available_indicators('gdi'))
         .get_data(hdr.available_indicators('gdi'))
         .to_csv(f'{PATHS.raw_data}/hdr_gdi.csv', index=False)
         )
        logger.debug("Extracted GDI data from HDR")

    # World Bank data
    def wb_wdi(self) -> None:
        """Extract data from World Bank WDI and save to csv called world_bank_wdi.csv"""

        indicators = {'SP.POP.TOTL.FE.IN': 'Female population',
                      'SP.POP.TOTL': 'Population total',
                      'SH.STA.MMRT': 'Maternal mortality',
                      'SP.DYN.TFRT.IN': 'Fertility rate'
                      }

        (world_bank.get_data(indicators, db=2)
         .to_csv(f'{PATHS.raw_data}/world_bank_wdi.csv', index=False)
         )
        logger.debug("Extracted WDI data from World Bank")

    def wb_gender(self) -> None:
        """Extract gender data from World Bank Gender Data Portal
         and save to csv called world_bank_wdi.csv
         """

        indicators = {
            'SG.GEN.PARL.ZS': 'Proportion of seats held by women in national parliaments (%)',
            'HD.HCI.EYRS.FE': 'Expected Years of School, Female',
            'SP.ADO.TFRT': 'Adolescent fertility rate (births per 1,000 women ages 15-19)',
            'SH.STA.FGMS.ZS': 'Female genital mutilation prevalence (%)',
            'SG.VAW.1549.ZS': 'Proportion of women subjected to physical and/or sexual violence in the last 12 months (% of ever-partnered women ages 15-49)',
            'SG.VAW.AFSX.ZS': 'Proportion of women who have ever experienced any form of sexual violence (% of women ages 15-49)',
            'SG.TIM.UWRK.MA': 'Proportion of time spent on unpaid domestic and care work, female (% of 24 hour day)- males',
            'SG.TIM.UWRK.FE': 'Proportion of time spent on unpaid domestic and care work, female (% of 24 hour day) - females',
            'SL.TLF.CACT.FE.ZS': 'Labor force participation rate, female (% of female population ages 15+) (modeled ILO estimate)',
            'SL.TLF.CACT.MA.ZS': 'Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate)',
            'SE.ENR.PRSC.FM.ZS': 'School enrollment, primary and secondary (gross), gender parity index (GPI)'
        }

        (world_bank.get_data(indicators, db=14)
         .to_csv(f'{PATHS.raw_data}/world_bank_gender.csv', index=False)
         )
        logger.debug("Extracted gender data from World Bank")

    def wb_law(self) -> None:
        """Extract law data from World Bank Gender Data Portal
         and save to csv called world_bank_wdi.csv
         """

        indicators = {'SG.LAW.EQRM.WK': 'Does the law require equal pay for equal work?',
                      "SG.LAW.NODC.HR": 'Does the law prohibit gender discrimination in the workplace?',
                      'SG.GET.JOBS.EQ': 'Can a woman get a job in the same way as a man?',
                      'SG.CNT.SIGN.EQ': 'Can a woman sign a contract in the same way as a man?',
                      "SG.PEN.SXHR.EM": 'Are there criminal penalties for sexual harassment in in the workplace?',
                      }

        (world_bank.get_data(indicators, db=14)
         .to_csv(f'{PATHS.raw_data}/world_bank_law.csv', index=False)
         )
        logger.debug("Extracted law data from World Bank")

    # UIS data
    def uis(self) -> None:
        """Extract data from UIS and save to csv called uis.csv"""

        indicators = {  # Out-of-school rate for children of primary school age
            "ROFST.1.CP": "Out-of-school rate for children of primary school age, both sexes (%)",
            "ROFST.1.F.CP": "Out-of-school rate for children of primary school age, female (%)",
            "ROFST.1.GPIA.CP": "Out-of-school rate for children of primary school age, adjusted gender parity index (GPIA)",
            "ROFST.1.M.CP": "Out-of-school rate for children of primary school age, male (%)",

            # Out-of-school rate for children of primary and lower secondary school age
            'ROFST.1T2.CP': 'Out-of-school rate for children and adolescents of primary and lower secondary school age, both sexes (%)',
            'ROFST.1T2.F.CP': 'Out-of-school rate for children and adolescents of primary and lower secondary school age, female (%)',
            'ROFST.1T2.GPIA.CP': 'Out-of-school rate for children and adolescents of primary and lower secondary school age, adjusted gender parity index (GPIA)',
            'ROFST.1T2.M.CP': 'Out-of-school rate for children and adolescents of primary and lower secondary school age, male (%)',

            # Educational attainment rate, completed primary education or higher
            ' EA.1T8.AG25T99': 'Educational attainment rate, completed primary education or higher, population 25+ years, both sexes (%)',
            'EA.1T8.AG25T99.F': 'Educational attainment rate, completed primary education or higher, population 25+ years, female (%)',
            'EA.1T8.AG25T99.GPIA': 'Educational attainment rate, completed primary education or higher, population 25+ years, adjusted gender parity index (GPIA)',
            'EA.1T8.AG25T99.M': 'Educational attainment rate, completed primary education or higher, population 25+ years, male (%)',
        }

        (uis.UIS()
         .get_data()
         .to_csv(f'{PATHS.raw_data}/uis.csv', index=False)
         )
        logger.debug("Extracted data from UIS")

    def update(self):

        self.gdi()
        self.gii()
        self.wb_law()
        self.wb_gender()
        self.wb_wdi()

