"""extract education data from UIS data"""

from unesco_reader import uis

from scripts.config import PATHS
from scripts.logger import logger



# SDG indicators
sdg_indicators = {

    # Out-of-school rate for children of primary school age
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


def get_sdg() -> None:
    """Extract data from UIS SDG dataset"""

    sdg = uis.UIS('SDG')
    sdg.load_data()
    df = sdg.get_data()

    # filter for indicators of interest
    (df[df['INDICATOR_ID'].isin(sdg_indicators)]
        .to_csv(PATHS.raw_data / 'uis.csv', index=False)
     )

    logger.info('UIS data extracted')


