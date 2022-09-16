from typing import Dict, Optional

from hdx.location.adminone import AdminOne
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


admintwo_headers = (
    ("iso3", "countryname", "adm2_pcode", "adm2_name"),
    ("#country+code", "#country+name", "#adm2+code", "#adm2+name"),
)

def update_admintwo(
    runner: Runner,
    admintwo: AdminOne,
    outputs: Dict[str, BaseOutput],
    names: Optional[ListTuple[str]] = None,
    level: str = "admintwo",
    tab: str = "admintwo",
) -> None:
    """Update the subnational tab (or key in JSON) in the outputs for scrapers limiting
    to those in names.

    Args:
        runner (Runner): Runner object
        adminone (AdminOne): AdminOne object
        outputs (Dict[str, BaseOutput]): Mapping from names to output objects
        names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
        level (str): Name of subnational level. Defaults to "subnational".
        tab (str): Name of tab (key in JSON) to update. Defaults to "subnational".

    Returns:
        None
    """

    def get_country_name(adm):
        countryiso3 = admintwo.pcode_to_iso3[adm]
        return Country.get_country_name_from_iso3(countryiso3)

    fns = (
        lambda adm: admintwo.pcode_to_iso3[adm],
        get_country_name,
        lambda adm: adm,
        lambda adm: admintwo.pcode_to_name[adm],
    )
    rows = runner.get_rows(
        level, admintwo.pcodes, admintwo_headers, fns, names=names
    )
    update_tab(outputs, tab, rows)
