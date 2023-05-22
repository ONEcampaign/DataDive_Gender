"""Extract data from Afrobarometer data files."""

import requests
from scripts.config import PATHS
from scripts.logger import logger

URL = "https://www.afrobarometer.org/wp-content/uploads/2022/02/r7_merged_data_34ctry.release.sav"


def get_afrobarometer_data():
    """Download Afrobarometer data from the website."""

    response = requests.get(URL)

    with open(PATHS.raw_data / "afrobarometer.sav", "wb") as f:
        f.write(response.content)

    logger.debug(f"Successfully downloaded Afrobarometer data")
