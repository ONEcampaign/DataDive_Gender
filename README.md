# Gender DataDive

This repository contains scripts and data powering the 
[Gender DataDive](https://data.one.org/data-dives/women-arent-given-power-they-make-it/).

Raw data files for the sources used in this analysis 
can be accessed in the `raw_data` folder, along with documentation on sources used

Chart datasets can be accessed in the `output` folder.

Scripts powering the raw data extraction and the analysis can
be found in the `scripts` folder. The data extraction and analysis runs 
automatically using GitHub Actions and the results are pushed to the `raw_data`
and `output` folder.

## Requirements

To reproduce the analysis, `Python >=3.10` is required.
The project is managed using [Poetry](https://python-poetry.org/) and dependencies 
can be installed by running `poetry install` in the root directory. Required
libraries and packages are listed in the `requirements.txt` file.`

## Data Sources

The following sources were used in this analysis:
- World Bank - [World Development Indicators](https://databank.worldbank.org/source/world-development-indicators)
- World Bank - [Gender Data Portal](https://genderdata.worldbank.org/)
- UNDP [Human Development Report](https://hdr.undp.org/data-center)
- UNESCO [Institute for Statistics](https://uis.unesco.org/en/)
- [UN Women, Pardee Center, UNDP](https://data.unwomen.org/features/poverty-deepens-women-and-girls-according-latest-projections)
- WHO, UNICEF, UNFPA, World Bank Group and UNDESA/Population Division - [mmr2020](https://mmr2020.srhr.org/)



