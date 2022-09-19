from typing import Dict, Optional

from hdx.location.country import Country
from hdx.scraper.outputs.base import BaseOutput
from hdx.scraper.outputs.update_tabs import update_tab
from hdx.scraper.runner import Runner
from hdx.utilities.text import get_fraction_str
from hdx.utilities.typehint import ListTuple


def calculate_ratios(ratios, items_per_country, affected_items_per_country):
    for countryiso in items_per_country:
        if countryiso in affected_items_per_country:
            ratios[countryiso] = get_fraction_str(
                affected_items_per_country[countryiso], items_per_country[countryiso]
            )
        else:
            ratios[countryiso] = "0.0"
    return ratios
