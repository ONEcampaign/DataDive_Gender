"""Extract and format HDI indices"""

from bblocks.import_tools import hdr
from scripts.config import PATHS


def download_raw_hdi():
    hdr_data = hdr.HDR().load_data(hdr.available_indicators('gii')
                                   + hdr.available_indicators('gdi')
                                   )

    # save gii
    (hdr_data
     .get_data(hdr.available_indicators('gii'))
     .to_csv(f'{PATHS.raw_data}/hdr_gii.csv', index=False))

    # save gdi
    (hdr_data
     .get_data(hdr.available_indicators('gdi'))
     .to_csv(f'{PATHS.raw_data}/hdr_gdi.csv', index=False))

