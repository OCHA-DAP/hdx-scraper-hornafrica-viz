import logging
from os.path import join

from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.scraper.outputs.update_tabs import (
    get_toplevel_rows,
    update_national,
    update_sources,
    update_subnational,
    update_toplevel,
)
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.fallbacks import Fallbacks

from .ipc import IPC

logger = logging.getLogger(__name__)


def get_indicators(
    configuration,
    today,
    outputs,
    tabs,
    scrapers_to_run=None,
    countries_override=None,
    errors_on_exit=None,
    use_live=True,
    fallbacks_root="",
):
    Country.countriesdata(
        use_live=use_live,
        country_name_overrides=configuration["country_name_overrides"],
        country_name_mappings=configuration["country_name_mappings"],
    )

    if countries_override:
        countries = countries_override
    else:
        countries = configuration["countries"]
    configuration["countries_fuzzy_try"] = countries
    adminone = AdminOne(configuration)
    if fallbacks_root is not None:
        fallbacks_path = join(fallbacks_root, configuration["json"]["output"])
        levels_mapping = {
            "regional": "regional_data",
            "national": "national_data",
            "subnational": "subnational_data",
        }
        Fallbacks.add(
            fallbacks_path,
            levels_mapping=levels_mapping,
            sources_key="sources_data",
        )
    runner = Runner(
        countries,
        adminone,
        today,
        errors_on_exit=errors_on_exit,
        scrapers_to_run=scrapers_to_run,
    )
    configurable_scrapers = dict()
    for level in ("national", "subnational"):
        suffix = f"_{level}"
        configurable_scrapers[level] = runner.add_configurables(
            configuration[f"scraper{suffix}"], level, suffix=suffix
        )
    ipc = IPC(configuration["ipc"], today, countries, adminone)

    runner.add_customs((ipc,))

    runner.run(
        prioritise_scrapers=(
            "population_national",
            "population_subnational",
            "population_regional",
        )
    )

    if "regional" in tabs:
        rows = get_toplevel_rows(runner, toplevel="regional")
        update_toplevel(outputs, rows, tab="regional")
    if "national" in tabs:
        update_national(
            runner,
            countries,
            outputs,
        )
    if "subnational" in tabs:
        update_subnational(runner, adminone, outputs)

    adminone.output_matches()
    adminone.output_ignored()
    adminone.output_errors()

    if "sources" in tabs:
        update_sources(
            runner,
            outputs,
            additional_sources=configuration["additional_sources"],
        )
    return countries
