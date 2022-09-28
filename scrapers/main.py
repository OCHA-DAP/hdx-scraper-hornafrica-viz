import logging
from os.path import join

from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.outputs.update_tabs import (
    get_toplevel_rows,
    sources_headers,
    update_national,
    update_sources,
    update_subnational,
    update_tab,
    update_toplevel,
)
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.fallbacks import Fallbacks

from .affected_targeted_reached import AffectedTargetedReached
from .fts import FTS
from .iom_dtm import IOMDTM
from .ipc import IPC
from .ipc_somalia import ipc_post_run
from .unhcr_somalia_idps import idps_post_run
from .utilities.sources import custom_sources

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
    adminone = AdminLevel(configuration["admin1"])
    admintwo = AdminLevel(
        configuration["admin2"], admin_level=2, admin_level_overrides={"KEN": 1}
    )
    if fallbacks_root is not None:
        fallbacks_path = join(fallbacks_root, configuration["json"]["output"])
        levels_mapping = {
            "regional": "regional_data",
            "national": "national_data",
            #            "adminone": "adminone_data",
            #            "admintwo": "admintwo_data",
        }
        Fallbacks.add(
            fallbacks_path,
            levels_mapping=levels_mapping,
            sources_key="sources_data",
        )
    runner = Runner(
        countries,
        today,
        errors_on_exit=errors_on_exit,
        scrapers_to_run=scrapers_to_run,
    )
    configurable_scrapers = dict()
    for level in (
        "national",
        "regional",
        "adminone",
        "admintwo",
    ):
        suffix = f"_{level}"
        if level == "admintwo":
            configurable_scrapers[level] = runner.add_configurables(
                configuration[f"scraper{suffix}"],
                level,
                adminlevel=admintwo,
                suffix=suffix,
            )
            continue
        configurable_scrapers[level] = runner.add_configurables(
            configuration[f"scraper{suffix}"], level, adminlevel=adminone, suffix=suffix
        )
    ipc = IPC(configuration["ipc"], today, countries, adminone, admintwo)
    fts = FTS(configuration["fts"], today, outputs, countries)
    iom_dtm = IOMDTM(configuration["iom_dtm"], today, admintwo)
    affectedtargetedreached = AffectedTargetedReached(
        configuration["affected_targeted_reached"], today, adminone, admintwo
    )

    runner.add_customs((ipc, fts, iom_dtm, affectedtargetedreached))
    runner.add_instance_variables(
        "iom_dtm", overrideinfo=configuration["unhcr_somalia_idps"]
    )
    runner.add_post_run("iom_dtm", idps_post_run)
    runner.add_instance_variables("ipc", overrideinfo=configuration["ipc_somalia"])
    runner.add_post_run("ipc", ipc_post_run)
    runner.add_aggregators(
        True,
        configuration["aggregate_regional"],
        "national",
        "regional",
        countries,
        force_add_to_run=True,
    )

    runner.run(
        prioritise_scrapers=(
            "population_national",
            "population_adminone",
            "population_admintwo",
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
    if "adminone" in tabs:
        update_subnational(runner, adminone, outputs, level="adminone", tab="adminone")

    if "admintwo" in tabs:
        update_subnational(runner, admintwo, outputs, level="admintwo", tab="admintwo")

    adminone.output_matches()
    adminone.output_ignored()
    adminone.output_errors()

    admintwo.output_matches()
    admintwo.output_ignored()
    admintwo.output_errors()

    if "sources" in tabs:
        sources = (
            list(sources_headers)
            + custom_sources(configuration["custom_sources_keyfigures"])
            + custom_sources(configuration["custom_sources_other"])
        )
        update_tab(outputs, "sources", sources)
    return countries
