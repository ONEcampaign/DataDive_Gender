"""Pipeline to update charts."""

from scripts.charts import (
    education,
    hdr,
    employment,
    legislation,
    maternal_mortality,
    poverty,
)
from scripts.logger import logger


def update_charts() -> None:
    """Update charts."""

    education.chart_scatter_attainment()

    hdr.chart_gii_explorer_latest()
    hdr.chart_gii_ridgeline()

    employment.chart_unpaid_work()
    employment.chart_labor_force_world()
    employment.chart_labor_force_income()

    legislation.chart_laws_marimekko()
    legislation.chart_parliament_participation_beeswarm()

    maternal_mortality.chart_line_change_in_mmr()

    poverty.chart_poverty_change_line()

    logger.debug("All charts successfully updated")
