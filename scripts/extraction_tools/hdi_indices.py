"""Extract and format HDI indices"""

from bblocks.import_tools import hdr


def download_raw_hdi():
    hdr_data = hdr.HDR().load_data(hdr.available_indicators('gii')
                                   + hdr.available_indicators('gdi')
                                   )

    # save gii
    (hdr_data
     .get_data(hdr.available_indicators('gii'))
     .to_csv('data/raw/hdi/hdr_gii.csv', index=False))

    # save gdi
    (hdr_data
     .get_data(hdr.available_indicators('gdi'))
     .to_csv('data/raw/hdi/hdr_gdi.csv', index=False))

