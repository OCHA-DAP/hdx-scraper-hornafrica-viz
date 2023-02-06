import logging

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities.sources import Sources
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import number_format

logger = logging.getLogger(__name__)


class AffectedTargetedReached(BaseScraper):
    def __init__(self, datasetinfo, today, admintwo):
        headers = (
            ("Total Affected", "Total Targeted", "Total Reached", "Priority"),
            (
                "#affected+total",
                "#targeted+total",
                "#reached+total",
                "#priority",
            ),
        )
        self.hxltags = headers[1]
        super().__init__(
            "affected_targeted_reached",
            datasetinfo,
            {
                "admintwo": headers,
            },
            source_configuration=Sources.create_source_configuration(
                adminlevel=admintwo,
                should_overwrite_sources=True,
            ),
        )
        self.today = today
        self.admintwo = admintwo
        self.datasetinfos = dict()

    def run(self) -> None:
        datasets = self.datasetinfo["datasets"]
        reader = self.get_reader()
        affecteddict = dict()
        targeteddict = dict()
        reacheddict = dict()
        prioritydict = dict()

        for countryiso3, dataset in datasets.items():
            datasetinfo = {"dataset": dataset, "format": "csv"}
            resource = reader.read_hdx_metadata(datasetinfo)
            self.datasetinfos[countryiso3] = datasetinfo
            data = reader.read_hxl_resource(
                f"{self.name}-{countryiso3}", resource, self.name
            )
            admin_level = self.admintwo.get_admin_level(countryiso3)
            for row in data:
                pcode = row.get(f"#adm{admin_level}+code")

                def add_to_dict(inddict, hxltag, pcode):
                    value = row.get(hxltag)
                    if value is not None:
                        dict_of_lists_add(inddict, f"{countryiso3}:{pcode}", int(value))

                add_to_dict(affecteddict, "#affected+total", pcode)
                add_to_dict(targeteddict, "#targeted+total", pcode)
                add_to_dict(reacheddict, "#reached+total", pcode)
                add_to_dict(prioritydict, "#priority", pcode)

        def fill_values(input, output, adminlevel, average=False):
            for countrypcode in input:
                countryiso3, pcode = countrypcode.split(":")
                if pcode not in adminlevel.pcodes:
                    logger.error(f"PCode {pcode} in {countryiso3} does not exist!")
                else:
                    aggregate_value = sum(input[countrypcode])
                    if average:
                        aggregate_value /= len(input[countrypcode])
                    output[pcode] = number_format(aggregate_value, format="%.0f")

        affected = self.get_values("admintwo")[0]
        fill_values(affecteddict, affected, self.admintwo)
        targeted = self.get_values("admintwo")[1]
        fill_values(targeteddict, targeted, self.admintwo)
        reached = self.get_values("admintwo")[2]
        fill_values(reacheddict, reached, self.admintwo)
        priority = self.get_values("admintwo")[3]
        fill_values(prioritydict, priority, self.admintwo, average=True)

    def add_sources(self) -> None:
        for countryiso3, datasetinfo in self.datasetinfos.items():
            self.add_hxltag_sources(
                self.hxltags, datasetinfo=datasetinfo, suffix_attributes=(countryiso3,)
            )
