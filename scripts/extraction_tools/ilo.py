"""Extract data from ILO"""


from bblocks.import_tools import ilo
from scripts.config import PATHS


indicators = {'EAR_GGAP_OCU_RT_A': 'Gender wage gap by occupation (%)',
              "LAP_2FTM_NOC_RT_A": "Gender income gap, ratio of women's to men's labour income",
              'SDG_T552_NOC_RT_A': 'Proportion of women in managerial positions (%)',
              'SDG_0831_SEX_ECO_RT_A': 'Proportion of informal employment in total employment by sex and sector (%)',
              'EMP_TEMP_SEX_AGE_OCU_NB_A': 'Employment by sex, age and occupation (thousands)',
              'EAP_DWAP_SEX_AGE_RT_A': 'Labour force participation rate by sex and age (%)'

              }


def update_ilo_data():
    """ """
    obj = ilo.ILO()
    obj.load_data(list(indicators))
    obj.update_data()
    df = obj.get_data()
    df.to_csv(f'{PATHS.raw_data}/ilo.csv', index=False)

