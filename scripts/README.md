## Scripts

This directory contains scripts that are used to extract data and run
the analysis.

The extraction_tools directory contains scripts that are used to extract
data from several sources and save them to the `raw_data` directory.
`extract_data.py` extarcts data from all sources that regularly
update. Only `pardee_unwomen_poverty.py` contains tools to extract poverty
data that will not be updated. Data extraction makes extensive use
of the packages `bblocks` and `unesco_reader` to facilitate the extraction
process.

The charts directory contains scripts that are used to generate charts
and save them to the `charts` directory. `update_charts.py` will update
all charts used to power the analysis.

`common.py` contains helper functions that are used by multiple scripts.
`config.py` is the configuration scripts, namely to manage project paths.
`logger.py` is a simple logger that is used to log the progress of the
scripts.
