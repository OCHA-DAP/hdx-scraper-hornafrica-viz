import logging
from os.path import join

from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.fallbacks import Fallbacks
from hdx.scraper.utilities.sources import Sources
from hdx.scraper.utilities.writer import Writer

from .acled import ACLED
from .affected_targeted_reached import AffectedTargetedReached
from .fts import FTS
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
    admintwo = AdminLevel(
        configuration["admin2"],
        admin_level=2,
        admin_level_overrides={"ETH": 3, "KEN": 1},
    )
    admintwo.setup_from_admin_info(configuration["admin2"]["admin_info"])
    if fallbacks_root is not None:
        fallbacks_path = join(fallbacks_root, configuration["json"]["output"])
        levels_mapping = {
            "regional": "regional_data",
            "national": "national_data",
            "admintwo": "admintwo_data",
        }
        admin_name_mapping = {
            "regional": "value",
            "national": "#country+code",
            "admintwo": "#adm2+code",
        }
        Fallbacks.add(
            fallbacks_path,
            levels_mapping=levels_mapping,
            sources_key="sources_data",
            admin_name_mapping=admin_name_mapping,
        )
    runner = Runner(
        countries,
        today,
        errors_on_exit=errors_on_exit,
        scrapers_to_run=scrapers_to_run,
    )
    configurable_scrapers = dict()

    def create_configurable_scrapers(level, suffix_attribute=None, adminlevel=None):
        suffix = f"_{level}"
        source_configuration = Sources.create_source_configuration(
            suffix_attribute=suffix_attribute, admin_sources=True, adminlevel=adminlevel
        )
        configurable_scrapers[level] = runner.add_configurables(
            configuration[f"scraper{suffix}"],
            level,
            adminlevel=adminlevel,
            source_configuration=source_configuration,
            suffix=suffix,
        )

    create_configurable_scrapers("regional", suffix_attribute="regional")
    create_configurable_scrapers("national")
    create_configurable_scrapers("admintwo", adminlevel=admintwo)

    fts = FTS(configuration["fts"], today, outputs, countries)
    affectedtargetedreached = AffectedTargetedReached(
        configuration["affected_targeted_reached"], today, admintwo
    )
    acled = ACLED(configuration["acled"], today, countries, outputs, admintwo)

    runner.add_customs((fts, affectedtargetedreached, acled))
    source_configuration = Sources.create_source_configuration(
        suffix_attribute="regional"
    )
    runner.add_aggregators(
        True,
        configuration["aggregate_regional"],
        "national",
        "regional",
        countries,
        source_configuration=source_configuration,
        force_add_to_run=True,
    )
    runner.run(
        prioritise_scrapers=(
            "population_eth_national",
            "population_ken_national",
            "population_som_national",
            "population_admintwo",
            "population_regional",
        )
    )

    writer = Writer(runner, outputs)
    if "regional" in tabs:
        rows = writer.get_toplevel_rows(toplevel="regional")
        writer.update_toplevel(rows, tab="regional")
    if "national" in tabs:
        writer.update_national(
            countries,
        )
    if "admintwo" in tabs:
        writer.update_subnational(admintwo, level="admintwo", tab="admintwo")

    admintwo.output_matches()
    admintwo.output_ignored()
    admintwo.output_errors()

    if "sources" in tabs:
        # sources = (
        #     list(writer.sources_headers)
        #     + custom_sources(configuration["custom_sources_keyfigures"], today)
        #     + custom_sources(configuration["custom_sources_other"], today)
        # )
        # writer.update("sources", sources)
        writer.update_sources(additional_sources=configuration["additional_sources"])
    return countries
