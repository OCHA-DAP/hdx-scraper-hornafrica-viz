import logging

import hxl
from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import number_format
from hxl import InputOptions

logger = logging.getLogger(__name__)


class AffectedTargetedReached(BaseScraper):
    def __init__(self, datasetinfo, today, admintwo):
        super().__init__(
            "affected_targeted_reached",
            datasetinfo,
            {
                "admintwo": (
                    ("Total Affected", "Total Targeted", "Total Reached", "Priority"),
                    ("#affected+total", "#targeted+total", "#reached+total", "#priority"),
                )
            },
        )
        self.today = today
        self.admintwo = admintwo

    def run(self) -> None:
        urls = self.datasetinfo["urls"]
        affecteddict = dict()
        targeteddict = dict()
        reacheddict = dict()
        prioritydict = dict()

        for countryiso3, url in urls.items():
            data = hxl.data(url, InputOptions(allow_local=True)).cache()
            admin_level = self.admintwo.get_admin_level(countryiso3)
            for row in data:
                pcode = row.get(f"#adm{admin_level}+code")

                def add_to_dict(inddict, hxltag):
                    value = row.get(hxltag)
                    if value is not None:
                        dict_of_lists_add(inddict, f"{countryiso3}:{pcode}", int(value))

                add_to_dict(affecteddict, "#affected+total")
                add_to_dict(targeteddict, "#targeted+total")
                add_to_dict(reacheddict, "#reached+total")
                add_to_dict(prioritydict, "#priority")

        def fill_values(input, output, average=False):
            for countrypcode in input:
                countryiso3, pcode = countrypcode.split(":")
                if pcode not in self.admintwo.pcodes:
                    logger.error(f"PCode {pcode} in {countryiso3} does not exist!")
                else:
                    aggregate_value = sum(input[countrypcode])
                    if average:
                        aggregate_value /= len(input[countrypcode])
                    output[pcode] = number_format(aggregate_value, format="%.0f")

        affected = self.get_values("admintwo")[0]
        fill_values(affecteddict, affected)
        targeted = self.get_values("admintwo")[1]
        fill_values(targeteddict, targeted)
        reached = self.get_values("admintwo")[2]
        fill_values(reacheddict, reached)
        priority = self.get_values("admintwo")[3]
        fill_values(prioritydict, priority, average=True)
        self.datasetinfo["source_date"] = self.today
        self.datasetinfo["source_url"] = ""
