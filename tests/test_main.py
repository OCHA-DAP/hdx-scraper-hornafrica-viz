import filecmp
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.scraper.outputs.base import BaseOutput
from hdx.scraper.outputs.json import JsonFile
from hdx.scraper.utilities.reader import Read
from hdx.utilities.dateparse import parse_date
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.useragent import UserAgent
from scrapers.main import get_indicators


class TestHornAfrica:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def folder(self):
        return join("tests", "fixtures")

    def test_get_indicators(self, configuration, folder):
        with ErrorsOnExit() as errors_on_exit:
            with temp_dir(
                "TestHornAfricaViz", delete_on_success=True, delete_on_failure=False
            ) as temp_folder:
                today = parse_date("2022-09-05")
                Read.create_readers(
                    temp_folder,
                    join(folder, "input"),
                    temp_folder,
                    save=False,
                    use_saved=True,
                    today=today,
                )
                tabs = configuration["tabs"]
                noout = BaseOutput(tabs)
                jsonout = JsonFile(configuration["json"], tabs)
                outputs = {"gsheets": noout, "excel": noout, "json": jsonout}
                countries_to_save = get_indicators(
                    configuration,
                    today,
                    outputs,
                    tabs,
                    scrapers_to_run=None,
                    countries_override=None,
                    errors_on_exit=errors_on_exit,
                    use_live=False,
                )
                filepaths = jsonout.save(
                    folder=temp_folder, countries_to_save=countries_to_save
                )
                filename = configuration["json"]["output"]
                assert filecmp.cmp(filepaths[0], join(folder, filename))
